package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import io.opencensus.scala.Tracing.traceWithParent
import io.opencensus.trace.{Span, AttributeValue => OpenCensusAttributeValue}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReport, GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, SamBillingProjectActions, SamBillingProjectPolicyNames, SamCreateResourceResponse, SamPolicy, SamPolicySyncStatus, SamProjectRoles, SamResourcePolicyName, SamResourceTypeNames, SamWorkflowCollectionPolicyNames, SamWorkflowCollectionRoles, SamWorkspacePolicyNames, SamWorkspaceRoles, UserInfo, Workspace, WorkspaceAccessLevels, WorkspaceAttributeSpecs, WorkspaceName, WorkspaceRequest, WorkspaceVersions}
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

class WorkspaceCreator(val userInfo: UserInfo,
                       val dataSource: SlickDataSource,
                       val samDAO: SamDAO,
                       val gcsDAO: GoogleServicesDAO,
                       val config: WorkspaceServiceConfig)
                      (implicit val system: ActorSystem, val materializer: Materializer, protected val executionContext: ExecutionContext)
  extends AttributeSupport {

  def createWorkspace(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Workspace] = {
    validateAttributeNamespace(workspaceRequest)
    for {
      _ <- checkCreateWorkspacePermissions(workspaceRequest, parentSpan)
      billingProjectOwnerPolicyEmail <- getBillingProjectOwnerPolicyEmail(workspaceRequest, parentSpan)
      _ <- checkIfWorkspaceWithNameAlreadyExists(workspaceRequest.toWorkspaceName, parentSpan)
      billingProject <- getBillingProjectForNewWorkspace(workspaceRequest.namespace, parentSpan)
      (googleProjectId, googleProjectNumber) <- claimAndSetupGoogleProject(billingProject, parentSpan)
      workspace <- persistNewWorkspace(workspaceRequest, googleProjectId, googleProjectNumber, parentSpan)
      workspaceResourceResponse <- createWorkspaceResourceInSam(workspaceRequest, workspace.workspaceId, billingProjectOwnerPolicyEmail, parentSpan)
      // TODO: rename to policyNameToEmailMap policyMap has policyName -> policyEmail
      policyMap: Map[SamResourcePolicyName, WorkbenchEmail] = workspaceResourceResponse.accessPolicies.map(x => SamResourcePolicyName(x.id.accessPolicyName) -> WorkbenchEmail(x.email)).toMap
      _ <- createWorkflowCollectionForWorkspace(workspace.workspaceId, policyMap, parentSpan)

      //      _ <- maybeUpdateGoogleProjectsInPerimeter(billingProject)
    } yield workspace
  }

  /**
    * Validates the Attribute Namespace and will throw an exception if it is invalid
    *
    * @param workspaceRequest
    */
  def validateAttributeNamespace(workspaceRequest: WorkspaceRequest): Unit = {
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
  def checkCreateWorkspacePermissions(workspaceRequest: WorkspaceRequest, parentSpan: Span): Future[Unit] = {
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
  def requireCreateWorkspaceAccess(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Boolean] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    traceWithParent("checkUserCanCreateWorkspace", parentSpan)(_ => samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectActions.createWorkspace, userInfo)).map {
      case false => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}"))
    }
  }

  // Creating a Workspace without an Owner policy is allowed only if the requesting User has the `owner` role
  // granted on the Workspace's Billing Project
  def maybeRequireBillingProjectOwnerAccess(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Unit] = {
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

  def getBillingProjectForNewWorkspace(billingProjectName: String, parentSpan: Span = null): Future[RawlsBillingProject] = {
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
  def updateAndGetBillingAccountAccess(billingProject: RawlsBillingProject, parentSpan: Span = null): Future[Boolean] = {
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
    * record.
    * 1. Claim Project from RBS
    * 2. Update Billing Account information on Google Project
    *
    * @param billingProject
    * @param span
    * @return Future[(GoogleProjectId, GoogleProjectNumber)] of the project that we claimed from RBS
    */
  private def claimAndSetupGoogleProject(billingProject: RawlsBillingProject, span: Span = null): Future[(GoogleProjectId, GoogleProjectNumber)] = {
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

  private def saveNewWorkspace(): Future[Workspace] = ???

  def saveNewWorkspace(workspaceRequest: WorkspaceRequest,
                       billingProjectOwnerPolicyEmail: WorkbenchEmail,
                       googleProjectId: GoogleProjectId,
                       googleProjectNumber: Option[GoogleProjectNumber],
                       parentSpan: Span = null): Future[Workspace] = {
    for {
      workspace <- persistNewWorkspace(workspaceRequest, googleProjectId, googleProjectNumber, parentSpan)
      // add the WorkspaceId to the span so we can find and correlate it later with other services
      unitResult = parentSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspace.workspaceId))
    } yield workspace


  }

  private def persistNewWorkspace(workspaceRequest: WorkspaceRequest, googleProjectId: GoogleProjectId, googleProjectNumber: GoogleProjectNumber, parentSpan: Span): Future[Workspace] = {
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
    }
  }

  private def constructBucketName(workspaceId: String, secure: Boolean) = s"${config.workspaceBucketNamePrefix}-${if (secure) "secure-" else ""}${workspaceId}"

  def createWorkspaceResourceInSam(workspaceRequest: WorkspaceRequest, workspaceId: String, billingProjectOwnerPolicyEmail: WorkbenchEmail, span: Span = null): Future[SamCreateResourceResponse] = {
    val projectOwnerPolicy = SamWorkspacePolicyNames.projectOwner -> SamPolicy(Set(billingProjectOwnerPolicyEmail), Set.empty, Set(SamWorkspaceRoles.owner, SamWorkspaceRoles.projectOwner))
    val ownerPolicyMembership: Set[WorkbenchEmail] = if (workspaceRequest.noWorkspaceOwner.getOrElse(false)) {
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

    traceWithParent("createWorkspaceResourceInSam", span)(_ => samDAO.createResourceFull(SamResourceTypeNames.workspace, workspaceId, defaultPolicies, workspaceRequest.authorizationDomain.getOrElse(Set.empty).map(_.membersGroupName.value), userInfo, None))
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
    * @param workspaceRequest
    * @param workspace
    * @param policyNameToEmailMap
    * @param span
    * @return
    */
  private def maybeSyncWorkspacePolicies(workspaceRequest: WorkspaceRequest,
                                         workspace: Workspace,
                                         policyNameToEmailMap: Map[SamResourcePolicyName, WorkbenchEmail],
                                         span: Span = null): Future[Iterable[Any]] = {
    traceWithParent("traversePolicies", span)(s1 =>
      Future.traverse(policyNameToEmailMap) { x =>
        val policyName = x._1
        val hasAuthDomains = workspaceRequest.authorizationDomain.getOrElse(Set.empty).nonEmpty
        if (policyName == SamWorkspacePolicyNames.projectOwner && !hasAuthDomains) {
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
}
