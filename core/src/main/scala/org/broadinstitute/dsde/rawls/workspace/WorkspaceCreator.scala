package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.traceWithParent
import io.opencensus.trace.{Span, AttributeValue => OpenCensusAttributeValue}
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReport, GoogleProjectId, GoogleProjectNumber, ManagedGroupRef, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, SamBillingProjectActions, SamBillingProjectPolicyNames, SamCreateResourceResponse, SamPolicy, SamProjectRoles, SamResourcePolicyName, SamResourceTypeNames, SamWorkflowCollectionPolicyNames, SamWorkflowCollectionRoles, SamWorkspacePolicyNames, SamWorkspaceRoles, ServicePerimeterName, UserInfo, Workspace, WorkspaceAccessLevels, WorkspaceAttributeSpecs, WorkspaceName, WorkspaceRequest, WorkspaceVersions}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, Retry}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class WorkspaceCreator(val userInfo: UserInfo,
                       val dataSource: SlickDataSource,
                       val samDAO: SamDAO,
                       val gcsDAO: GoogleServicesDAO,
                       val config: WorkspaceServiceConfig)
                      (implicit val system: ActorSystem, val materializer: Materializer, protected val executionContext: ExecutionContext)
  extends AttributeSupport with Retry with LazyLogging {

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
    * Note:  There is no rollback logic here, so it is possible that the creation process could fail anywhere and the
    * Workspace will be in some partially created state
    *
    * @param workspaceRequest
    * @param span
    * @return
    */
  def createWorkspace(workspaceRequest: WorkspaceRequest, span: Span = null): Future[Workspace] = {
    validateAttributeNamespace(workspaceRequest)
    val authDomains = workspaceRequest.authorizationDomain.getOrElse(Set.empty)
    val noWorkspaceOwner = workspaceRequest.noWorkspaceOwner.getOrElse(false)

    // TODO:
    // 1. Verify the ordering of things.  Generally kept the same from what was here before, but could also see
    //    reordering a few steps possibly
    // 2. Futures are all run in order - there were a few spots in the old code where Futures were kicked off to allow
    //    them to run in parallel.  Can/should probably still do that below, it just makes the `for` comp a little
    //    uglier.
    for {
      _ <- checkCreateWorkspacePermissions(workspaceRequest, span)
      billingProjectOwnerPolicyEmail <- getBillingProjectOwnerPolicyEmail(workspaceRequest, span)
      _ <- checkIfWorkspaceWithNameAlreadyExists(workspaceRequest.toWorkspaceName, span)
      billingProject <- getBillingProjectForNewWorkspace(workspaceRequest.namespace, span)
      (googleProjectId, googleProjectNumber) <- claimGoogleProject(billingProject, span)
      workspace <- persistNewWorkspace(workspaceRequest, googleProjectId, googleProjectNumber, span)
      workspaceResourceResponse <- createWorkspaceResourceInSam(workspace.workspaceId, billingProjectOwnerPolicyEmail, noWorkspaceOwner, authDomains, span)
      policyNameToEmailMap: Map[SamResourcePolicyName, WorkbenchEmail] = workspaceResourceResponse.accessPolicies.map(x => SamResourcePolicyName(x.id.accessPolicyName) -> WorkbenchEmail(x.email)).toMap
      _ <- createWorkflowCollectionForWorkspace(workspace.workspaceId, policyNameToEmailMap, span)
      _ <- syncWorkspacePoliciesWithGoogle(workspace, policyNameToEmailMap, authDomains, span)
      _ <- setupGoogleProjectForWorkspace(workspace, policyNameToEmailMap, billingProjectOwnerPolicyEmail, authDomains, span)
      _ <- maybeUpdateGoogleProjectsInPerimeter(billingProject)
    } yield workspace
  }

  /**
    * Validates the Attribute Namespace and will throw an exception if it is invalid
    *
    * @param workspaceRequest
    */
  private def validateAttributeNamespace(workspaceRequest: WorkspaceRequest): Unit = {
    val errors = attributeNamespaceCheck(workspaceRequest.attributes.keys)
    if (errors.nonEmpty) failAttributeNamespaceCheck(errors)
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
    // Could we run these Future's in parallel instead of sequentially?
    for {
      _ <- requireCreateWorkspaceAccess(workspaceRequest, parentSpan)
      _ <- maybeRequireBillingProjectOwnerAccess(workspaceRequest, parentSpan)
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
  private def requireCreateWorkspaceAccess(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Boolean] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    traceWithParent("checkUserCanCreateWorkspace", parentSpan)(_ => samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectActions.createWorkspace, userInfo)).map {
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
        case Some(_) => Future.successful()
        case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceName} already exists"))
      }
    }
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
  private def claimGoogleProject(billingProject: RawlsBillingProject, span: Span = null): Future[(GoogleProjectId, GoogleProjectNumber)] = {
    // We should never get here with a missing or invalid Billing Account, but we still need to get the value out of the
    // Option, so we are being thorough
    val billingAccount = billingProject.billingAccount match {
      case Some(ba) if !billingProject.invalidBillingAccount => ba
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Billing Account is missing or invalid for Billing Project: ${billingProject}"))
    }

    for {
      googleProjectId <- traceWithParent("getGoogleProjectFromRBS", span)(_ => getGoogleProjectFromRBS)
      _ <- traceWithParent("updateBillingAccountForProject", span)(_ => gcsDAO.setBillingAccountForProject(googleProjectId, billingAccount))
      googleProjectNumber <- traceWithParent("getProjectNumberFromGoogle", span)(_ => getGoogleProjectNumber(googleProjectId))
    } yield (googleProjectId, googleProjectNumber)
  }

  // TODO: https://broadworkbench.atlassian.net/browse/CA-946
  // This method should talk to the Resource Buffering Service, get a Google Project, and then set the appropriate
  // Billing Account on the project and any other things we need to set on the Project.
  private def getGoogleProjectFromRBS: Future[GoogleProjectId] = {
    // TODO: Implement for real
    Future.successful(GoogleProjectId(UUID.randomUUID.toString.substring(0, 8) + "_fake_proj_name"))
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
    * @param parentSpan
    * @return
    */
  private def persistNewWorkspace(workspaceRequest: WorkspaceRequest,
                                  googleProjectId: GoogleProjectId,
                                  googleProjectNumber: GoogleProjectNumber,
                                  parentSpan: Span): Future[Workspace] = {
    val currentDate = DateTime.now
    val workspaceId = UUID.randomUUID.toString
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
      traceDBIOWithParent("createOrUpdateWorkspace", parentSpan)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace))
    }.map { workspace =>
      // add the workspace id to the span so we can find and correlate it later with other services
      parentSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspace.workspaceId))
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
          traceWithParent(s"syncPolicy-${policyName}", s1)( _ => samDAO.syncPolicyToGoogle(SamResourceTypeNames.workspace, workspace.workspaceId, policyName))
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
      val projectNumberStrings = projectNumbers.map(_.value)

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
