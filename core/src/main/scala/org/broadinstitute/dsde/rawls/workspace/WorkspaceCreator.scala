package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import com.google.api.services.storage.model.StorageObject
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.traceWithParent
import io.opencensus.trace.{Span, AttributeValue => OpenCensusAttributeValue}
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReport, GoogleProjectId, GoogleProjectNumber, ManagedGroupRef, ProjectPoolType, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, RawlsGroupName, SamBillingProjectActions, SamBillingProjectPolicyNames, SamCreateResourceResponse, SamPolicy, SamProjectRoles, SamResourcePolicyName, SamResourceTypeNames, SamWorkflowCollectionPolicyNames, SamWorkflowCollectionRoles, SamWorkspaceActions, SamWorkspacePolicyNames, SamWorkspaceRoles, ServicePerimeterName, UserInfo, Workspace, WorkspaceAccessLevels, WorkspaceAttributeSpecs, WorkspaceName, WorkspaceRequest, WorkspaceVersions}
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, LibraryPermissionsSupport, Retry, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class WorkspaceCreator(val userInfo: UserInfo,
                       val dataSource: SlickDataSource,
                       val samDAO: SamDAO,
                       val gcsDAO: GoogleServicesDAO,
                       val config: WorkspaceServiceConfig,
                       val resourceBufferService: ResourceBufferService)
                      (implicit val system: ActorSystem, val materializer: Materializer, protected val executionContext: ExecutionContext)
  extends WorkspaceSupport with AttributeSupport with LibraryPermissionsSupport with Retry with LazyLogging {

  import dataSource.dataAccess.driver.api._

  /**
    * Public method to be used for creating a new Workspace.  The bulk of the creation process is performed by
    * `createWorkspaceInternal`
    *
    * @param workspaceRequest
    * @param span
    * @return
    */
  def createWorkspace(workspaceRequest: WorkspaceRequest, span: Span = null): Future[Workspace] = {
    validateAttributeNamespace(workspaceRequest.attributes.keys)
    createWorkspaceInternal(workspaceRequest, span)
  }

  /**
    * Public method for cloning an existing Workspace.  The creation of the new cloned Workspace will be performed by
    * `createWorkspaceInternal`, but this method will need to do some work before that to make sure the user has the
    * necessary permissions on the source Workspace and that any Auth Domain restrictions on the source Workspace are
    * still in place on the cloned Workspace.  After the cloned Workspace is created, this method will also do things
    * like copying Entities, Method Configs, and bucket data from the source Workspace into the cloned Workspace.
    *
    * If you have concerns with how Attributes are cloned from the existing Workspace, you may also want to review
    * https://github.com/broadinstitute/firecloud-orchestration/blob/develop/src/main/scala/org/broadinstitute/dsde/firecloud/webservice/WorkspaceApiService.scala
    * in POST /api/workspaces/{workspaceNamespace}/{workspaceName}/clone
    *
    * @param sourceWorkspaceName
    * @param destWorkspaceRequest
    * @return
    */
  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest): Future[Workspace] = {
    destWorkspaceRequest.copyFilesWithPrefix.foreach(prefix => validateFileCopyPrefix(prefix))
    val (libraryAttributeNames, workspaceAttributeNames) = destWorkspaceRequest.attributes.keys.partition(name => name.namespace == AttributeName.libraryNamespace)

    validateAttributeNamespace(workspaceAttributeNames)
    validateLibraryAttributeNamespaces(libraryAttributeNames)

    for {
      sourceWorkspace <- getWorkspaceIfUserHasAction(sourceWorkspaceName, SamWorkspaceActions.read)
      destAuthDomains <- validateAuthDomainsForDestWorkspace(sourceWorkspace.workspaceId, destWorkspaceRequest.authorizationDomain.getOrElse(Set.empty))
      newAttrs = sourceWorkspace.attributes ++ destWorkspaceRequest.attributes
      cloneWorkspaceRequest = destWorkspaceRequest.copy(authorizationDomain = Option(destAuthDomains), attributes = newAttrs)
      destWorkspace <- createWorkspaceInternal(cloneWorkspaceRequest)
      _ <- copyEntitiesAndMethodConfigs(sourceWorkspace, destWorkspace)
    } yield {
      //we will fire and forget this. a more involved, but robust, solution involves using the Google Storage Transfer APIs
      //in most of our use cases, these files should copy quickly enough for there to be no noticeable delay to the user
      //we also don't want to block returning a response on this call because it's already a slow endpoint
      copyBucketContents(sourceWorkspace, destWorkspace, cloneWorkspaceRequest.copyFilesWithPrefix)

      destWorkspace
    }
  }

  // Temporary method for easily switching between the parallel/sequential versions of createWorkspaceInternal while
  // this is still in development/testing
  private def createWorkspaceInternal(workspaceRequest: WorkspaceRequest, span: Span = null): Future[Workspace] = {
    createWorkspaceInternal_parallel(workspaceRequest, span)
  }

  /**
    * This method will take a WorkspaceRequest and attempt to create a fully functional Terra Workspace.  This method
    * will check that the User making the request has the required permissions on the Billing Project to create
    * Workspaces within it.  This method will claim a Google Project from the Resource Buffering Service and perform
    * all of the actions on the Google Project to initialize it for use by Terra, such as setting up Billing, IAM,
    * Google Storage Bucket(s), etc.  This method will ensure that all Workspace Policies are created and populated in
    * Sam and that they are also Sync'd with Google as necessary.  Finally, this method will attempt to add the newly
    * created Workspace's Google Project into the Billing Project's Service Perimeter if one is defined.
    *
    * This method will either succeed, or a RawlsExceptionWithErrorReport will be thrown describing the reason why it
    * failed.
    *
    * This method is shared by createWorkspace and cloneWorkspace.  They each do some of their own stuff, but ultimately
    * both should call this method to create the desired Workspace
    *
    * Note:  There is no rollback logic here, so it is possible that the creation process could fail anywhere and the
    * Workspace will be in some partially created state
    *
    * @param workspaceRequest
    * @param span
    * @return
    */
  private def createWorkspaceInternal_sequential(workspaceRequest: WorkspaceRequest, span: Span = null): Future[Workspace] = {
    val authDomains = workspaceRequest.authorizationDomain.getOrElse(Set.empty)
    val noWorkspaceOwner = workspaceRequest.noWorkspaceOwner.getOrElse(false)

    for {
      _ <- checkCreateWorkspacePermissions(workspaceRequest, span)
      billingProjectOwnerPolicyEmail <- getBillingProjectOwnerPolicyEmail(workspaceRequest, span)
      _ <- checkIfWorkspaceWithNameAlreadyExists(workspaceRequest.toWorkspaceName, span)
      billingProject <- getBillingProjectForNewWorkspace(workspaceRequest.namespace, span)
      workspaceId = UUID.randomUUID.toString
      (googleProjectId, googleProjectNumber) <- claimGoogleProject(billingProject, workspaceId, span)
      workspace <- persistNewWorkspace(workspaceRequest, googleProjectId, googleProjectNumber, workspaceId, span)
      workspaceResourceResponse <- createWorkspaceResourceInSam(workspace.workspaceId, billingProjectOwnerPolicyEmail, noWorkspaceOwner, authDomains, span)
      policyNameToEmailMap: Map[SamResourcePolicyName, WorkbenchEmail] = workspaceResourceResponse.accessPolicies.map(x => SamResourcePolicyName(x.id.accessPolicyName) -> WorkbenchEmail(x.email)).toMap
      _ <- createWorkflowCollectionForWorkspace(workspace.workspaceId, policyNameToEmailMap, span)
      _ <- syncWorkspacePoliciesWithGoogle(workspace, policyNameToEmailMap, authDomains, span)
      _ <- setupGoogleProjectForWorkspace(workspace, policyNameToEmailMap, billingProjectOwnerPolicyEmail, authDomains, span)
      _ <- maybeUpdateGoogleProjectsInPerimeter(billingProject)
    } yield workspace
  }

  /**
    * Creates a new Workspace doing everything the same as createWorkspaceInternal_sequential, but this version tries
    * to run certain steps/futures in parallel instead of running them all sequentially.  It's not super pretty.  It
    * breaks things down into 3 different steps, each of which is its own `for` comprehension.  Why 3?  It was chosen
    * because there was a set of steps identified that could be run first and all in parallel.  After these finish,
    * there is a set of steps that must be executed sequentially.  After those complete, there is another set of steps
    * that can all be kicked off in parallel.  If you need to add a new step, depending on its placement and whether it
    * needs to be run in parallel or sequentially with existing steps, you may be able to just add it to an existing
    * `for` comp, or you may need to add a whole new `for` comp.
    * @param workspaceRequest
    * @param span
    * @return
    */
  private def createWorkspaceInternal_parallel(workspaceRequest: WorkspaceRequest, span: Span = null): Future[Workspace] = {
    val authDomains = workspaceRequest.authorizationDomain.getOrElse(Set.empty)
    val noWorkspaceOwner = workspaceRequest.noWorkspaceOwner.getOrElse(false)

    for {
      (billingProjectOwnerPolicyEmail, billingProject) <- firstSteps(workspaceRequest, span)
      (workspace, workspaceResourceResponse) <- secondSteps(workspaceRequest, billingProject, billingProjectOwnerPolicyEmail, noWorkspaceOwner, authDomains, span)
      _ <- thirdSteps(workspace, billingProject, workspaceResourceResponse, billingProjectOwnerPolicyEmail, authDomains, span)
    } yield workspace
  }

  private def firstSteps(workspaceRequest: WorkspaceRequest, span: Span = null): Future[(WorkbenchEmail, RawlsBillingProject)] = {
    val checkUserCanCreateWorkspace = checkCreateWorkspacePermissions(workspaceRequest, span)
    val checkWorkspaceNameIsUnique = checkIfWorkspaceWithNameAlreadyExists(workspaceRequest.toWorkspaceName, span)
    val billingProjectOwnerPolicyEmailFuture = getBillingProjectOwnerPolicyEmail(workspaceRequest, span)
    val getBillingProjectFuture = getBillingProjectForNewWorkspace(workspaceRequest.namespace, span)

    for { // These things happen in parallel, but they all need to finish before moving on
      _ <- checkUserCanCreateWorkspace
      _ <- checkWorkspaceNameIsUnique
      billingProjectOwnerPolicyEmail <- billingProjectOwnerPolicyEmailFuture
      billingProject <- getBillingProjectFuture
    } yield (billingProjectOwnerPolicyEmail, billingProject)
  }

  private def secondSteps(workspaceRequest: WorkspaceRequest,
                          billingProject: RawlsBillingProject,
                          billingProjectOwnerPolicyEmail: WorkbenchEmail,
                          noWorkspaceOwner: Boolean,
                          authDomains: Set[ManagedGroupRef],
                          span: Span = null): Future[(Workspace, SamCreateResourceResponse)] = {
    val workspaceId = UUID.randomUUID.toString
    for { // The following futures need to happen sequentially
      (googleProjectId, googleProjectNumber) <- claimGoogleProject(billingProject, workspaceId, span)
      workspace <- persistNewWorkspace(workspaceRequest, googleProjectId, googleProjectNumber, workspaceId, span)
      workspaceResourceResponse <- createWorkspaceResourceInSam(workspace.workspaceId, billingProjectOwnerPolicyEmail, noWorkspaceOwner, authDomains, span)
    } yield (workspace, workspaceResourceResponse)
  }

  private def thirdSteps(workspace: Workspace,
                         billingProject: RawlsBillingProject,
                         samCreateResourceResponse: SamCreateResourceResponse,
                         billingProjectOwnerPolicyEmail: WorkbenchEmail,
                         authDomains: Set[ManagedGroupRef],
                         span: Span = null): Future[Unit] = {
    val policyNameToEmailMap = samCreateResourceResponse.accessPolicies.map(x => SamResourcePolicyName(x.id.accessPolicyName) -> WorkbenchEmail(x.email)).toMap
    for { // Run all of these in parallel
      _ <- Future.sequence(Seq(
        createWorkflowCollectionForWorkspace(workspace.workspaceId, policyNameToEmailMap, span),
        syncWorkspacePoliciesWithGoogle(workspace, policyNameToEmailMap, authDomains, span),
        setupGoogleProjectForWorkspace(workspace, policyNameToEmailMap, billingProjectOwnerPolicyEmail, authDomains, span),
        maybeUpdateGoogleProjectsInPerimeter(billingProject)))
    } yield ()
  }

  private def validateFileCopyPrefix(copyFilesWithPrefix: String): Unit = {
    if (copyFilesWithPrefix.isEmpty) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, """You may not specify an empty string for `copyFilesWithPrefix`. Did you mean to specify "/" or leave the field out entirely?"""))
  }

  /**
    * This method will query Sam to get the list of Auth Domains defined on the Source Workspace, i.e. the one that is
    * being cloned.  It will then Union these with the Auth Domains being defined on the new cloned Workspace. An
    * exception will be thrown if there are Auth Domains defined on the Source Workspace that are missing from the
    * Destination Workspace.  If the Auth Domains on the Destination Workspace are valid, this method will finish
    * processing normally and return the Union
    *
    * @param sourceWorkspaceId
    * @param destWorkspaceAuthDomains
    * @return
    */
  private def validateAuthDomainsForDestWorkspace(sourceWorkspaceId: String, destWorkspaceAuthDomains: Set[ManagedGroupRef]): Future[Set[ManagedGroupRef]] = {
    samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceId, userInfo).map { sourceAuthDomainStrings =>
      val sourceAuthDomains = sourceAuthDomainStrings.map(n => ManagedGroupRef(RawlsGroupName(n))).toSet
      unionAuthDomains(sourceAuthDomains, destWorkspaceAuthDomains)
    }
  }

  /**
    * Ever Auth Domain listed in sourceWorkspaceADs must be present in destWorkspaceADs or else an exception will be
    * thrown.  If valid, then return the Union of sourceWorkspaceADs and destWorkspaceADs.
    *
    * TODO: If sourceWorkspaceADs is a subset of destWorkspaceADs, isn't the Union equivalent to destWorkspaceADs?
    *
    * @param sourceWorkspaceADs
    * @param destWorkspaceADs
    * @return
    */
  private def unionAuthDomains(sourceWorkspaceADs: Set[ManagedGroupRef], destWorkspaceADs: Set[ManagedGroupRef]): Set[ManagedGroupRef] = {
    // if the source has an auth domain, the dest must also have that auth domain as a subset
    // otherwise, the caller may choose to add to the auth domain
    if (sourceWorkspaceADs.subsetOf(destWorkspaceADs)) sourceWorkspaceADs ++ destWorkspaceADs
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg = s"Source workspace has an Authorization Domain containing the groups ${missingGroups.map(_.membersGroupName.value).mkString(", ")}, which are missing on the destination workspace"
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg))
    }
  }

  /**
    * Copy all of the Entities AND all of the Method Configs from sourceWorkspace into destWorkspace.
    *
    * This was "All or nothing" before, so keeping it that way.  If either Entities OR Method Configs fail to copy over,
    * then neither will copy over.
    *
    * @param sourceWorkspace
    * @param destWorkspace
    * @return
    */
  private def copyEntitiesAndMethodConfigs(sourceWorkspace: Workspace, destWorkspace: Workspace): Future[Unit] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.copyAllEntities(sourceWorkspace, destWorkspace) andThen
        dataAccess.methodConfigurationQuery.listActive(sourceWorkspace).flatMap { methodConfigShorts =>
          val inserts = methodConfigShorts.map { methodConfigShort =>
            dataAccess.methodConfigurationQuery.get(sourceWorkspace, methodConfigShort.namespace, methodConfigShort.name).flatMap { methodConfig =>
              dataAccess.methodConfigurationQuery.create(destWorkspace, methodConfig.get)
            }
          }
          DBIO.seq(inserts: _*)
        } andThen {
        DBIO.successful()
      }
    }
  }

  /**
    * Find all the objects in sourceWorkspace's bucket that start with filePrefix and copy them to destWorkspace's
    * bucket
    *
    * @param filePrefix
    * @param sourceWorkspace
    * @param destWorkspace
    */
  private def copyBucketContents(sourceWorkspace: Workspace, destWorkspace: Workspace, filePrefix: Option[String]): Future[List[Option[StorageObject]]] = {
    filePrefix match {
      case Some(prefix) => gcsDAO.listObjectsWithPrefix(sourceWorkspace.bucketName, prefix).flatMap { objectsToCopy =>
        Future.traverse(objectsToCopy) { objectToCopy => gcsDAO.copyFile(sourceWorkspace.bucketName, objectToCopy.getName, destWorkspace.bucketName, objectToCopy.getName) }
      }
      case None => Future.successful(List[Option[StorageObject]]())
    }
  }

  /**
    * Use this method to make all the checks with Sam about whether the user has the required permissions to create a
    * Workspace in this Namespace/Billing Project.  These checks should throw RawlsExceptionsWithErrorReport any time
    * they encounter a scenario where the user does not have the necessary permissions.
    *
    * @param workspaceRequest
    * @param parentSpan
    * @return
    */
  private def checkCreateWorkspacePermissions(workspaceRequest: WorkspaceRequest, parentSpan: Span): Future[Unit] = {
    // Running futures in parallel
    val requireCreateWorkspaceAccessFuture = requireCreateWorkspaceAccess(workspaceRequest, parentSpan)
    val maybeRequireBillingProjectOwnerAccessFuture = maybeRequireBillingProjectOwnerAccess(workspaceRequest, parentSpan)

    for {
      _ <- requireCreateWorkspaceAccessFuture
      _ <- maybeRequireBillingProjectOwnerAccessFuture
    } yield ()
  }

  /**
    * Make an API call to Sam to check if the user has permissions to create a Workspace in the specified Billing
    * Project.  Throw an RawlsExceptionWithErrorReport if user does not have required permissions.
    *
    * @param workspaceRequest
    * @param parentSpan
    * @return
    */
  private def requireCreateWorkspaceAccess(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Unit] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    traceWithParent("checkUserCanCreateWorkspace", parentSpan)(_ => samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectActions.createWorkspace, userInfo)).map {
      case true => ()
      case false => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}"))
    }
  }

  // Creating a Workspace without an Owner policy is allowed only if the requesting User has the `owner` role
  // granted on the Workspace's Billing Project
  private def maybeRequireBillingProjectOwnerAccess(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Unit] = {
    workspaceRequest.noWorkspaceOwner match {
      case Some(true) => requireBillingProjectOwnerAccess(workspaceRequest, parentSpan)
      case _ => Future.successful()
    }
  }

  private def requireBillingProjectOwnerAccess(workspaceRequest: WorkspaceRequest, parentSpan: Span): Future[Unit] = {
    traceWithParent("listUsersRolesOnBillingProject", parentSpan)(_ => samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, workspaceRequest.namespace, userInfo)).map { billingProjectRoles =>
      if (!billingProjectRoles.contains(SamProjectRoles.owner))
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, s"Missing ${SamProjectRoles.owner} role on billing project '${workspaceRequest.namespace}'."))
    }
  }

  // Legacy code calls samDAO.getPolicySyncStatus just to get the Owner Policy Email.  Not exactly sure why it is done
  // this way instead of just searching up the Policy email.  This way checks that there's sync information for the
  // policy, and perhaps that's useful?
  private def getBillingProjectOwnerPolicyEmail(workspaceRequest: WorkspaceRequest, span: Span = null): Future[WorkbenchEmail] = {
    traceWithParent("getBillingProjectOwnerPolicySyncStatus", span)(_ => samDAO.getPolicySyncStatus(SamResourceTypeNames.billingProject, workspaceRequest.namespace, SamBillingProjectPolicyNames.owner, userInfo)).map(_.email)
  }

  /**
    * Checks that Rawls has the right permissions on the BillingProject's Billing Account, and then returns the
    * BillingProject with the latest value of invalidBillingAccount set
    *
    * @param billingProjectName
    * @param parentSpan
    * @return
    */
  private def getBillingProjectForNewWorkspace(billingProjectName: String, parentSpan: Span = null): Future[RawlsBillingProject] = {
    for {
      maybeBillingProject <- loadBillingProject(billingProjectName, parentSpan)
      billingProject = maybeBillingProject.getOrElse(throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Billing Project ${billingProjectName} does not exist")))
      _ <- updateAndGetBillingAccountAccess(billingProject, parentSpan).map { hasAccess =>
        if (!hasAccess) {
          throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"Terra does not have required permissions on Billing Account: ${billingProject.billingAccount}.  Please ensure that 'terra-billing@terra.bio' is a member of your Billing Account with the 'Billing Account User' role"))
        }
      }
    } yield billingProject
  }

  private def loadBillingProject(billingProjectName: String, parentSpan: Span): Future[Option[RawlsBillingProject]] = {
    dataSource.inTransaction { dataAccess =>
      traceDBIOWithParent("loadBillingProject", parentSpan)(_ => dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(billingProjectName)))
    }
  }

  /**
    * takes a RawlsBillingProject and checks that Rawls has the appropriate permissions on the underlying Billing
    * Account on Google.  Does NOT check if Terra _User_ has necessary permissions on the Billing Account.  Updates
    * BillingProject to persist latest 'invalidBillingAccount' info.  Returns TRUE if user has right IAM access, else
    * FALSE
    */
  private def updateAndGetBillingAccountAccess(billingProject: RawlsBillingProject, parentSpan: Span = null): Future[Boolean] = {
    val billingAccountName: RawlsBillingAccountName = billingProject.billingAccount.getOrElse(throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"Billing Project ${billingProject.projectName.value} has no Billing Account associated with it")))
    for {
      hasAccess <- traceWithParent("checkBillingAccountIAM", parentSpan)(_ => gcsDAO.testDMBillingAccountAccess(billingAccountName))
      _ <- maybeUpdateInvalidBillingAccountField(billingProject, !hasAccess, parentSpan)
    } yield hasAccess
  }

  private def maybeUpdateInvalidBillingAccountField(billingProject: RawlsBillingProject, invalidBillingAccount: Boolean, span: Span = null): Future[Seq[Int]] = {
    // Only update the Billing Project record if the invalidBillingAccount field has changed
    if (billingProject.invalidBillingAccount != invalidBillingAccount) {
      val updatedBillingProject = billingProject.copy(invalidBillingAccount = invalidBillingAccount)
      dataSource.inTransaction { dataAccess =>
        traceDBIOWithParent("updateInvalidBillingAccountField", span)(_ => dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(updatedBillingProject)))
      }
    } else {
      Future.successful(Seq[Int]())
    }
  }

  private def checkIfWorkspaceWithNameAlreadyExists(workspaceName: WorkspaceName, span: Span = null): Future[Unit] = {
    dataSource.inTransaction { dataAccess =>
      traceDBIOWithParent("findWorkspaceByName", span)(_ => dataAccess.workspaceQuery.findByName(workspaceName, Option(WorkspaceAttributeSpecs(all = false, List.empty[AttributeName])))).map {
        case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceName} already exists"))
        case None => Future.successful()
      }
    }.flatten
  }

  /**
    * Gets a Google Project from the Resource Buffering Service (RBS) and sets it up to be usable by Rawls as the backing
    * Google Project for a Workspace.  The specific entities in the Google Project (like Buckets or compute nodes or
    * whatever) that are used by the Workspace will all get set up later after the Workspace is created in Rawls.  The
    * project should NOT be added to any Service Perimeters yet, that needs to happen AFTER we persist the Workspace
    * record.  We will immediately try to set the Billing Account on the Google Project though because if we cannot
    * set that for any reason, we should not consider the "claim" action to be successful.
    * 1. Claim Project from RBS
    * 2. Update Billing Account information on Google Project
    *
    * @param billingProject
    * @param span
    * @return Future[(GoogleProjectId, GoogleProjectNumber)] of the project that we claimed from RBS
    */
  private def claimGoogleProject(billingProject: RawlsBillingProject, workspaceId: String, span: Span = null): Future[(GoogleProjectId, GoogleProjectNumber)] = {
    // We should never get here with a missing or invalid Billing Account, but we still need to get the value out of the
    // Option, so we are being thorough
    val billingAccount = billingProject.billingAccount match {
      case Some(ba) if !billingProject.invalidBillingAccount => ba
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Billing Account is missing or invalid for Billing Project: ${billingProject}"))
    }

    val projectPoolType = billingProject.servicePerimeter match {
      case Some(_) => ProjectPoolType.ExfiltrationControlled
      case _ => ProjectPoolType.Regular
    }

    for {
      googleProjectId <- traceWithParent("getGoogleProjectFromBuffer", span)(_ => resourceBufferService.getGoogleProjectFromBuffer(projectPoolType, workspaceId))
      _ <- traceWithParent("updateBillingAccountForProject", span)(_ => gcsDAO.setBillingAccountForProject(googleProjectId, billingAccount))
      googleProjectNumber <- traceWithParent("getProjectNumberFromGoogle", span)(_ => getGoogleProjectNumber(googleProjectId))
    } yield (googleProjectId, googleProjectNumber)
  }

  private def getGoogleProjectNumber(googleProjectId: GoogleProjectId): Future[GoogleProjectNumber] = {
    gcsDAO.getGoogleProject(googleProjectId).map { p =>
      Option(p.getProjectNumber) match {
        case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, s"Failed to retrieve Google Project Number for Google Project ${googleProjectId}"))
        case Some(longProjectNumber) => GoogleProjectNumber(longProjectNumber.toString)
      }
    }
  }

  /**
    * This is the method that finally saves the new Workspace Record in the database.  All of the validations and
    * permissions checks should have already been done.
    *
    * @param workspaceRequest
    * @param googleProjectId
    * @param googleProjectNumber
    * @param span
    * @return
    */
  private def persistNewWorkspace(workspaceRequest: WorkspaceRequest,
                                  googleProjectId: GoogleProjectId,
                                  googleProjectNumber: GoogleProjectNumber,
                                  workspaceId: String,
                                  span: Span): Future[Workspace] = {
    val currentDate = DateTime.now
    val bucketName = constructBucketName(workspaceId, workspaceRequest.authorizationDomain.exists(_.nonEmpty))

    val workspace = Workspace(
      namespace = workspaceRequest.namespace,
      name = workspaceRequest.name,
      workspaceId = workspaceId,
      bucketName = bucketName,
      workflowCollectionName = Option(workspaceId),
      createdDate = currentDate,
      lastModified = currentDate,
      createdBy = userInfo.userEmail.value,
      attributes = workspaceRequest.attributes,
      isLocked = false,
      workspaceVersion = WorkspaceVersions.V2,
      googleProjectId = googleProjectId,
      googleProjectNumber = Option(googleProjectNumber)
    )

    dataSource.inTransaction { dataAccess =>
      traceDBIOWithParent("createOrUpdateWorkspace", span)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace))
    }.map { workspace =>
      // add the workspace id to the span so we can find and correlate it later with other services
      if (span != null) span.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspace.workspaceId))
      workspace
    }
  }

  private def constructBucketName(workspaceId: String, secure: Boolean) = s"${config.workspaceBucketNamePrefix}-${if (secure) "secure-" else ""}${workspaceId}"

  private def createWorkspaceResourceInSam(workspaceId: String,
                                           billingProjectOwnerPolicyEmail: WorkbenchEmail,
                                           noWorkspaceOwner: Boolean,
                                           authDomains: Set[ManagedGroupRef],
                                           span: Span = null): Future[SamCreateResourceResponse] = {
    val projectOwnerPolicy = SamWorkspacePolicyNames.projectOwner -> SamPolicy(Set(billingProjectOwnerPolicyEmail), Set.empty, Set(SamWorkspaceRoles.owner, SamWorkspaceRoles.projectOwner))
    val ownerPolicyMembership: Set[WorkbenchEmail] = if (noWorkspaceOwner) {
      Set.empty
    } else {
      Set(WorkbenchEmail(userInfo.userEmail.value))
    }
    val ownerPolicy = SamWorkspacePolicyNames.owner -> SamPolicy(ownerPolicyMembership, Set.empty, Set(SamWorkspaceRoles.owner))
    val writerPolicy = SamWorkspacePolicyNames.writer -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.writer))
    val readerPolicy = SamWorkspacePolicyNames.reader -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.reader))
    val shareReaderPolicy = SamWorkspacePolicyNames.shareReader -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.shareReader))
    val shareWriterPolicy = SamWorkspacePolicyNames.shareWriter -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.shareWriter))
    val canComputePolicy = SamWorkspacePolicyNames.canCompute -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.canCompute))
    val canCatalogPolicy = SamWorkspacePolicyNames.canCatalog -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.canCatalog))

    val defaultPolicies = Map(projectOwnerPolicy, ownerPolicy, writerPolicy, readerPolicy, shareReaderPolicy, shareWriterPolicy, canComputePolicy, canCatalogPolicy)

    traceWithParent("createWorkspaceResourceInSam", span)(_ => samDAO.createResourceFull(SamResourceTypeNames.workspace, workspaceId, defaultPolicies, authDomains.map(_.membersGroupName.value), userInfo, None))
  }

  private def createWorkflowCollectionForWorkspace(workspaceId: String, policyMap: Map[SamResourcePolicyName, WorkbenchEmail], span: Span = null): Future[Unit] = {
    for {
      _ <- traceWithParent("createWorkflowCollectionResourceInSam", span)(_ => samDAO.createResourceFull(
        SamResourceTypeNames.workflowCollection,
        workspaceId,
        Map(
          SamWorkflowCollectionPolicyNames.workflowCollectionOwnerPolicyName ->
            SamPolicy(Set(policyMap(SamWorkspacePolicyNames.projectOwner), policyMap(SamWorkspacePolicyNames.owner)), Set.empty, Set(SamWorkflowCollectionRoles.owner)),
          SamWorkflowCollectionPolicyNames.workflowCollectionWriterPolicyName ->
            SamPolicy(Set(policyMap(SamWorkspacePolicyNames.canCompute)), Set.empty, Set(SamWorkflowCollectionRoles.writer)),
          SamWorkflowCollectionPolicyNames.workflowCollectionReaderPolicyName ->
            SamPolicy(Set(policyMap(SamWorkspacePolicyNames.reader), policyMap(SamWorkspacePolicyNames.writer)), Set.empty, Set(SamWorkflowCollectionRoles.reader))
        ),
        Set.empty,
        userInfo,
        None
      ))
    } yield {}
  }

  /**
    * There is a list of policy names defined in WorkspaceAccessLevels, and each of those policies need to be sync'd
    * with Google so that Sam and Google agree on who can do what in Terra and on GCP in the context of this Workspace.
    *
    * @param workspace
    * @param policyNameToEmailMap
    * @param span
    * @return
    */
  private def syncWorkspacePoliciesWithGoogle(workspace: Workspace,
                                              policyNameToEmailMap: Map[SamResourcePolicyName, WorkbenchEmail],
                                              authDomains: Set[ManagedGroupRef],
                                              span: Span = null): Future[Iterable[Any]] = {
    traceWithParent("traversePolicies", span)(s1 =>
      Future.traverse(policyNameToEmailMap) { x =>
        val policyName = x._1
        if (policyName == SamWorkspacePolicyNames.projectOwner && authDomains.isEmpty) {
          // when there isn't an auth domain, we will use the billing project admin policy email directly on workspace
          // resources instead of synching an extra group. This helps to keep the number of google groups a user is in below
          // the limit of 2000
          Future.successful(())
        } else if (WorkspaceAccessLevels.withPolicyName(policyName.value).isDefined) {
          // only sync policies that have corresponding WorkspaceAccessLevels to google because only those are
          // granted bucket access (and thus need a google group)
          traceWithParent(s"syncPolicy-${policyName}", s1)(_ => samDAO.syncPolicyToGoogle(SamResourceTypeNames.workspace, workspace.workspaceId, policyName))
        } else {
          Future.successful(())
        }
      }
    )
  }

  private def setupGoogleProjectForWorkspace(workspace: Workspace,
                                             policyNameToEmailMap: Map[SamResourcePolicyName, WorkbenchEmail],
                                             billingProjectOwnerPolicyEmail: WorkbenchEmail,
                                             authDomains: Set[ManagedGroupRef],
                                             span: Span = null): Future[GoogleWorkspaceInfo] = {
    //there's potential for another perf improvement here for workspaces with auth domains. if a workspace is in an auth domain, we'll already have
    //the projectOwnerEmail, so we don't need to get it from sam. in a pinch, we could also store the project owner email in the rawls DB since it
    //will never change, which would eliminate the call to sam entirely
    val policyEmails = policyNameToEmailMap.map { case (policyName, policyEmail) =>
      if (policyName == SamWorkspacePolicyNames.projectOwner && authDomains.isEmpty) {
        // when there isn't an auth domain, we will use the billing project admin policy email directly on workspace
        // resources instead of synching an extra group. This helps to keep the number of google groups a user is in below
        // the limit of 2000
        Option(WorkspaceAccessLevels.ProjectOwner -> billingProjectOwnerPolicyEmail)
      } else {
        WorkspaceAccessLevels.withPolicyName(policyName.value).map(_ -> policyEmail)
      }
    }.flatten.toMap

    traceWithParent("gcsDAO.setupWorkspace", span)(_ => gcsDAO.setupWorkspace(userInfo, workspace.googleProjectId, policyEmails, workspace.bucketName, getLabels(authDomains.toList), span))
  }

  private def getLabels(authDomain: List[ManagedGroupRef]): Map[String, String] = authDomain match {
    case Nil => Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.LOW_SECURITY_LABEL)
    case ads => Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.HIGH_SECURITY_LABEL) ++ ads.map(ad => gcsDAO.labelSafeString(ad.membersGroupName.value, "ad-") -> "")
  }

  /**
    * If a ServicePerimeter is specified on the BillingProject, then we should update the list of Google Projects in the
    * Service Perimeter.  All newly created Workspaces (and their newly claimed Google Projects) should already be
    * persisted in the Rawls database prior to calling this method.  If no ServicePerimeter is specified on the Billing
    * Project, do nothing
    *
    * @param billingProject
    * @param span
    * @return Future[Unit]
    */
  private def maybeUpdateGoogleProjectsInPerimeter(billingProject: RawlsBillingProject, span: Span = null): Future[Unit] = {
    billingProject.servicePerimeter match {
      case Some(servicePerimeterName) => overwriteGoogleProjectsInPerimeter(servicePerimeterName)
      case None => Future.successful()
    }
  }

  /**
    * Takes the the name of a Service Perimeter as the only parameter.  Since multiple Billing Projects can specify the
    * same Service Perimeter, we will:
    * 1. Load all the Billing Projects that also use this servicePerimeterName
    * 2. Load all the Workspaces in all of those Billing Projects
    * 3. Collect all of the GoogleProjectNumbers from those Workspaces
    * 4. Post that list to Google to overwrite the Service Perimeter's list of included Google Projects
    * 5. Poll until Google Operation to update the Service Perimeter gets to some terminal state
    * Throw exceptions if any of this goes awry
    *
    * @param servicePerimeterName
    * @return Future[Unit] indicating whether we succeeded to update the Service Perimeter
    */
  private def overwriteGoogleProjectsInPerimeter(servicePerimeterName: ServicePerimeterName): Future[Unit] = {
    collectWorkspacesInPerimeter(servicePerimeterName).map { workspacesInPerimeter =>
      val projectNumbers = workspacesInPerimeter.flatMap(_.googleProjectNumber) ++ loadStaticProjectsForPerimeter(servicePerimeterName)
      val projectNumberStrings: Set[String] = projectNumbers.map(_.value).toSet

      // Make the call to Google to overwrite the project.  Poll and wait for the Google Operation to complete
      gcsDAO.accessContextManagerDAO.overwriteProjectsInServicePerimeter(servicePerimeterName, projectNumberStrings).map { operation =>
        // Keep retrying the pollOperation until the OperationStatus that gets returned is some terminal status
        retryUntilSuccessOrTimeout(failureLogMessage = s"Google Operation to update Service Perimeter: ${servicePerimeterName} was not successful")(5 seconds, 50 seconds) { () =>
          gcsDAO.pollOperation(OperationId(GoogleApiTypes.AccessContextManagerApi, operation.getName)).map {
            case OperationStatus(false, _) => Future.failed(new RawlsException(s"Google Operation to update Service Perimeter ${servicePerimeterName} is still in progress..."))
            // TODO: If the operation to update the Service Perimeter failed, we need to consider the possibility that
            // the list of Projects in the Perimeter may have been wiped or somehow modified in an undesirable way.  If
            // this happened, it would be possible for Projects intended to be in the Perimeter are NOT in that
            // Perimeter anymore, which is a problem.
            case OperationStatus(true, errorMessage) if errorMessage.nonEmpty => Future.successful(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Google Operation to update Service Perimeter ${servicePerimeterName} failed with message: ${errorMessage}")))
            case _ => Future.successful()
          }
        }
      }
    }
  }

  /**
    * In its own transaction, look up all of the Workspaces contained in Billing Projects that use the specified
    * ServicePerimeterName
    *
    * @param servicePerimeterName
    * @return
    */
  private def collectWorkspacesInPerimeter(servicePerimeterName: ServicePerimeterName): Future[Seq[Workspace]] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.getWorkspacesInPerimeter(servicePerimeterName)
    }
  }

  /**
    * Some Service Perimeters may required that they have some additional non-Terra Google Projects that need to be in
    * the perimeter for some other reason.  These are provided to us by the Service Perimeter stakeholders and we add
    * them to the Rawls Config so that whenever we update the list of projects for a perimeter, these projects are
    * always included.
    *
    * @param servicePerimeterName
    * @return
    */
  private def loadStaticProjectsForPerimeter(servicePerimeterName: ServicePerimeterName): Seq[GoogleProjectNumber] = {
    config.staticProjectsInPerimeters.getOrElse(servicePerimeterName, Seq.empty)
  }
}
