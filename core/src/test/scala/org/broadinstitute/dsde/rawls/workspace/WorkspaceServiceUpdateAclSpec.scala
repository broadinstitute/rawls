package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.IamRole
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{
  ExecutionServiceCluster,
  GoogleServicesDAO,
  RequesterPaysSetupService,
  SamDAO,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  GoogleProjectId,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectPolicyNames,
  SamPolicy,
  SamPolicyWithNameAndEmail,
  SamResourcePolicyName,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  SamWorkspacePolicyNames,
  SamWorkspaceRoles,
  UserIdInfo,
  UserInfo,
  Workspace,
  WorkspaceACLUpdate,
  WorkspaceACLUpdateResponseList,
  WorkspaceAccessLevels,
  WorkspaceName,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.submissions.SubmissionsRepository
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when, RETURNS_SMART_NULLS}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

// Workspace ACL change tests, separated from the main WorkspaceServiceSpec and WorkspaceServiceUnitTest to keep the file size manageable
class WorkspaceServiceUpdateAclSpec extends AnyFlatSpecLike with MockitoSugar with ScalaFutures with Matchers {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val workspace: Workspace = Workspace(
    "test-namespace",
    "test-name",
    UUID.randomUUID().toString,
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  val ctx: RawlsRequestContext = RawlsRequestContext(
    UserInfo(RawlsUserEmail("user@example.com"),
             OAuth2BearerToken("Bearer 123"),
             123,
             RawlsUserSubjectId("fake_user_id")
    )
  )

  def workspacePolicies(projectOwners: Set[WorkbenchEmail] = Set.empty,
                        owners: Set[WorkbenchEmail] = Set.empty,
                        writers: Set[WorkbenchEmail] = Set.empty,
                        readers: Set[WorkbenchEmail] = Set.empty,
                        shareWriters: Set[WorkbenchEmail] = Set.empty,
                        shareReaders: Set[WorkbenchEmail] = Set.empty,
                        canComputers: Set[WorkbenchEmail] = Set.empty,
                        canCatalogers: Set[WorkbenchEmail] = Set.empty
  ): Set[SamPolicyWithNameAndEmail] = Set(
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner,
                              SamPolicy(projectOwners, Set.empty, Set.empty),
                              WorkbenchEmail("projectOwner@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.owner,
                              SamPolicy(owners, Set.empty, Set.empty),
                              WorkbenchEmail("owner@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.writer,
                              SamPolicy(writers, Set.empty, Set.empty),
                              WorkbenchEmail("writer@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.reader,
                              SamPolicy(readers, Set.empty, Set.empty),
                              WorkbenchEmail("reader@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareWriter,
                              SamPolicy(shareWriters, Set.empty, Set.empty),
                              WorkbenchEmail("shareWriter@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareReader,
                              SamPolicy(shareReaders, Set.empty, Set.empty),
                              WorkbenchEmail("shareReader@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCompute,
                              SamPolicy(canComputers, Set.empty, Set.empty),
                              WorkbenchEmail("canCompute@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCatalog,
                              SamPolicy(canCatalogers, Set.empty, Set.empty),
                              WorkbenchEmail("canCatalog@example.com")
    )
  )

  // This is just for convenience, so we only need to specify mocks we care about
  def workspaceServiceConstructor(
    executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster](RETURNS_SMART_NULLS),
    workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
    leonardoService: LeonardoService = mock[LeonardoService](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO],
    notificationDAO: NotificationDAO = mock[NotificationDAO](RETURNS_SMART_NULLS),
    userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService](RETURNS_SMART_NULLS),
    workbenchMetricBaseName: String = "",
    config: WorkspaceServiceConfig = mock[WorkspaceServiceConfig](RETURNS_SMART_NULLS),
    requesterPaysSetupService: RequesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS),
    resourceBufferService: ResourceBufferService = mock[ResourceBufferService](RETURNS_SMART_NULLS),
    servicePerimeterService: ServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS),
    googleIamDao: GoogleIamDAO = mock[GoogleIamDAO](RETURNS_SMART_NULLS),
    terraBillingProjectOwnerRole: String = "",
    terraWorkspaceCanComputeRole: String = "",
    terraWorkspaceNextflowRole: String = "",
    terraBucketReaderRole: String = "",
    terraBucketWriterRole: String = "",
    rawlsWorkspaceAclManager: RawlsWorkspaceAclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS),
    multiCloudWorkspaceAclManager: MultiCloudWorkspaceAclManager =
      mock[MultiCloudWorkspaceAclManager](RETURNS_SMART_NULLS),
    fastPassServiceConstructor: RawlsRequestContext => FastPassService = _ =>
      mock[FastPassService](RETURNS_SMART_NULLS),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    billingRepository: BillingRepository = mock[BillingRepository](RETURNS_SMART_NULLS),
    submissionsRepository: SubmissionsRepository = mock[SubmissionsRepository](RETURNS_SMART_NULLS)
  ): RawlsRequestContext => WorkspaceService = info =>
    new WorkspaceService(
      info,
      mock[SlickDataSource](RETURNS_SMART_NULLS),
      executionServiceCluster,
      workspaceManagerDAO,
      leonardoService,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      workbenchMetricBaseName,
      config,
      requesterPaysSetupService,
      resourceBufferService,
      servicePerimeterService,
      googleIamDao,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager,
      fastPassServiceConstructor,
      workspaceRepository,
      billingRepository,
      submissionsRepository
    )(scala.concurrent.ExecutionContext.global)

  // Returns mock of SamDAO that handles basic usage.
  def getSamDaoWithBasicMocking: SamDAO = {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    // Mock check for missing users so emails in AclUpdate show as registered in Terra
    when(samDAO.getUserIdInfo(any(), any()))
      .thenReturn(Future.successful(SamDAO.User(UserIdInfo("subjectId", "email", None))))
    // Mock enabled check for calling user
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("subjectId", "email", true))))

    // Mock successful Unit Sam calls
    when(samDAO.inviteUser(any(), any())).thenReturn(Future.successful(()))
    samDAO
  }

  def allWorkspaceAclUpdatePermutations(emailString: String): Seq[WorkspaceACLUpdate] = for {
    accessLevel <- WorkspaceAccessLevels.all
    canShare <- Set(Some(true), Some(false), None)
    canCompute <- Set(Some(true), Some(false), None)
  } yield WorkspaceACLUpdate(emailString, accessLevel, canShare, canCompute)

  def expectedPolicies(
    aclUpdate: WorkspaceACLUpdate
  ): Either[StatusCode, Set[(SamResourceTypeName, SamResourcePolicyName)]] =
    aclUpdate match {
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.ProjectOwner, _, _) => Left(StatusCodes.BadRequest)
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Owner, _, _) =>
        Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.owner))

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, canShare, canCompute) =>
        val canSharePolicy = canShare match {
          case None | Some(false) => Set.empty
          case Some(true)         => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareWriter)
        }
        val canComputePolicy = canCompute match {
          case None | Some(true) =>
            Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.canCompute)
          case Some(false) => Set.empty
        }
        Right(
          Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.writer) ++ canSharePolicy ++ canComputePolicy
        )

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, canShare, canCompute) =>
        if (canCompute.contains(true)) {
          Left(StatusCodes.BadRequest)
        } else {
          val canSharePolicy = canShare match {
            case None | Some(false) => Set.empty
            case Some(true)         => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareReader)
          }
          Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.reader) ++ canSharePolicy)
        }

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.NoAccess, _, _) => Right(Set.empty)
    }

  behavior of "updateACL"

  it should "add ACLs" in {
    val user1Email = WorkbenchEmail("obama@whitehouse.gov")
    val user2Email = WorkbenchEmail("obama2@whitehouse.gov")
    val aclAdd = Set(
      WorkspaceACLUpdate(user1Email.value, WorkspaceAccessLevels.Owner, None),
      WorkspaceACLUpdate(user2Email.value, WorkspaceAccessLevels.Read, Option(true))
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listPoliciesForResource(any(), any(), any())).thenReturn(Future.successful(workspacePolicies()))
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("owner"),
            SamWorkspaceActions.sharePolicy("reader"),
            SamWorkspaceActions.sharePolicy("share-reader")
        )
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                       workspaceRepository = workspaceRepository,
                                                       fastPassServiceConstructor = _ => fastPassService,
                                                       rawlsWorkspaceAclManager = aclManager
    )(ctx)
    val result = Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclAdd, false), Duration.Inf)

    val responseFromAdd = WorkspaceACLUpdateResponseList(
      Set(
        WorkspaceACLUpdate(user1Email.value, WorkspaceAccessLevels.Owner, Some(true), Some(true)),
        WorkspaceACLUpdate(user2Email.value, WorkspaceAccessLevels.Read, Some(true), Some(false))
      ),
      Set.empty,
      Set.empty
    )

    assertResult(responseFromAdd, "add ACL shouldn't error") {
      result
    }

    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.owner, user1Email, ctx)
    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.shareReader, user2Email, ctx)
    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.reader, user2Email, ctx)
  }

  it should "update ACLs" in {
    val readerEmail = WorkbenchEmail("obama@whitehouse.gov")
    val aclUpdate = Set(
      WorkspaceACLUpdate(readerEmail.value, WorkspaceAccessLevels.Write, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"),
            SamWorkspaceActions.sharePolicy("reader"),
            SamWorkspaceActions.sharePolicy("can-compute")
        )
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any()))
      .thenReturn(Future.successful(Set(readerEmail -> SamWorkspacePolicyNames.reader)))
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                       workspaceRepository = workspaceRepository,
                                                       fastPassServiceConstructor = _ => fastPassService,
                                                       rawlsWorkspaceAclManager = aclManager
    )(ctx)
    val result = Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    val responseFromUpdate = WorkspaceACLUpdateResponseList(
      Set(
        WorkspaceACLUpdate(readerEmail.value, WorkspaceAccessLevels.Write, Some(false), Some(true))
      ),
      Set.empty,
      Set.empty
    )

    assertResult(responseFromUpdate, "Update ACL shouldn't error") {
      result
    }

    verify(aclManager).removeUserFromPolicy(workspace, SamWorkspacePolicyNames.reader, readerEmail, ctx)
    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.writer, readerEmail, ctx)
    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.canCompute, readerEmail, ctx)
  }

  it should "remove ACLs" in {
    val writerEmail = WorkbenchEmail("obama@whitehouse.gov")
    val aclUpdate = Set(
      WorkspaceACLUpdate(writerEmail.value, WorkspaceAccessLevels.NoAccess, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"), SamWorkspaceActions.sharePolicy("can-compute"))
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
      Future.successful(
        Set(writerEmail -> SamWorkspacePolicyNames.writer, writerEmail -> SamWorkspacePolicyNames.canCompute)
      )
    )
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      requesterPaysSetupService = requesterPaysSetupService,
      rawlsWorkspaceAclManager = aclManager
    )(ctx)
    val result = Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    val responseFromRemove = WorkspaceACLUpdateResponseList(
      Set(
        WorkspaceACLUpdate(writerEmail.value, WorkspaceAccessLevels.NoAccess, Some(false), Some(false))
      ),
      Set.empty,
      Set.empty
    )

    assertResult(responseFromRemove, "Remove ACL shouldn't error") {
      result
    }

    verify(aclManager).removeUserFromPolicy(workspace, SamWorkspacePolicyNames.writer, writerEmail, ctx)
    verify(aclManager).removeUserFromPolicy(workspace, SamWorkspacePolicyNames.canCompute, writerEmail, ctx)
  }

  it should "remove requester pays for writers and owners when removing ACLs" in {
    val writerEmail = WorkbenchEmail("writer@example.com")
    val ownerEmail = WorkbenchEmail("owner@example.com")
    val readerEmail = WorkbenchEmail("reader@example.com")
    val aclUpdate = Set(WorkspaceACLUpdate(writerEmail.value, WorkspaceAccessLevels.NoAccess, None),
                        WorkspaceACLUpdate(ownerEmail.value, WorkspaceAccessLevels.NoAccess, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"),
            SamWorkspaceActions.sharePolicy("can-compute"),
            SamWorkspaceActions.sharePolicy("owner")
        )
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
      Future.successful(
        Set(
          ownerEmail -> SamWorkspacePolicyNames.owner,
          writerEmail -> SamWorkspacePolicyNames.writer,
          writerEmail -> SamWorkspacePolicyNames.canCompute,
          readerEmail -> SamWorkspacePolicyNames.reader
        )
      )
    )
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      requesterPaysSetupService = requesterPaysSetupService,
      rawlsWorkspaceAclManager = aclManager
    )(ctx)
    Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    verify(requesterPaysSetupService).revokeUserFromWorkspace(RawlsUserEmail(writerEmail.value), workspace)
    verify(requesterPaysSetupService).revokeUserFromWorkspace(RawlsUserEmail(ownerEmail.value), workspace)
    verify(requesterPaysSetupService, never).revokeUserFromWorkspace(RawlsUserEmail(readerEmail.value), workspace)
  }

  it should "keep requester pays when changing a writer to an owner or vice versa" in {
    val writerEmail = WorkbenchEmail("writer@example.com")
    val ownerEmail = WorkbenchEmail("owner@example.com")
    val readerEmail = WorkbenchEmail("reader@example.com")
    val aclUpdate = Set(WorkspaceACLUpdate(writerEmail.value, WorkspaceAccessLevels.Owner, None),
                        WorkspaceACLUpdate(ownerEmail.value, WorkspaceAccessLevels.Write, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"),
            SamWorkspaceActions.sharePolicy("can-compute"),
            SamWorkspaceActions.sharePolicy("owner")
        )
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
      Future.successful(
        Set(
          ownerEmail -> SamWorkspacePolicyNames.owner,
          writerEmail -> SamWorkspacePolicyNames.writer,
          writerEmail -> SamWorkspacePolicyNames.canCompute,
          readerEmail -> SamWorkspacePolicyNames.reader
        )
      )
    )
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      requesterPaysSetupService = requesterPaysSetupService,
      rawlsWorkspaceAclManager = aclManager
    )(ctx)
    Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    verify(requesterPaysSetupService, never).revokeUserFromWorkspace(RawlsUserEmail(writerEmail.value), workspace)
    verify(requesterPaysSetupService, never).revokeUserFromWorkspace(RawlsUserEmail(ownerEmail.value), workspace)
    verify(requesterPaysSetupService, never).revokeUserFromWorkspace(RawlsUserEmail(readerEmail.value), workspace)
  }

  it should "remove requester pays for writers and owners when they are demoted to readers but retain access to the workspace" in {
    val writerEmail = WorkbenchEmail("writer@example.com")
    val ownerEmail = WorkbenchEmail("owner@example.com")
    val readerEmail = WorkbenchEmail("reader@example.com")
    val aclUpdate = Set(WorkspaceACLUpdate(writerEmail.value, WorkspaceAccessLevels.Read, None),
                        WorkspaceACLUpdate(ownerEmail.value, WorkspaceAccessLevels.Read, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(
          SamWorkspaceActions.sharePolicy("writer"),
          SamWorkspaceActions.sharePolicy("can-compute"),
          SamWorkspaceActions.sharePolicy("owner"),
          SamWorkspaceActions.sharePolicy("reader")
        )
      )
    )
    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
      Future.successful(
        Set(
          ownerEmail -> SamWorkspacePolicyNames.owner,
          writerEmail -> SamWorkspacePolicyNames.writer,
          writerEmail -> SamWorkspacePolicyNames.canCompute,
          readerEmail -> SamWorkspacePolicyNames.reader
        )
      )
    )
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      requesterPaysSetupService = requesterPaysSetupService,
      rawlsWorkspaceAclManager = aclManager
    )(ctx)
    Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    verify(requesterPaysSetupService).revokeUserFromWorkspace(RawlsUserEmail(writerEmail.value), workspace)
    verify(requesterPaysSetupService).revokeUserFromWorkspace(RawlsUserEmail(ownerEmail.value), workspace)
    verify(requesterPaysSetupService, never).revokeUserFromWorkspace(RawlsUserEmail(readerEmail.value), workspace)
  }

  it should "return non-existent users when updating ACLs" in {
    val nonExistentUserEmail = "missing@example.com"
    val aclUpdate = Set(
      WorkspaceACLUpdate(nonExistentUserEmail, WorkspaceAccessLevels.Owner, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.getUserIdInfo(ArgumentMatchers.eq(nonExistentUserEmail), any()))
      .thenReturn(Future.successful(SamDAO.NotFound))
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("owner"),
            SamWorkspaceActions.sharePolicy("reader"),
            SamWorkspaceActions.sharePolicy("share-reader")
        )
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                       workspaceRepository = workspaceRepository,
                                                       fastPassServiceConstructor = _ => fastPassService,
                                                       rawlsWorkspaceAclManager = aclManager
    )(ctx)
    val result = Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    val responseFromUpdate =
      WorkspaceACLUpdateResponseList(Set.empty,
                                     Set.empty,
                                     Set(WorkspaceACLUpdate(nonExistentUserEmail, WorkspaceAccessLevels.Owner, None))
      )

    assertResult(responseFromUpdate, "Update ACL should return non-existent users") {
      result
    }
  }

  it should "invite a user to a workspace" in {
    val nonExistentUserEmail = "missing@example.com"
    val aclUpdate = Set(
      WorkspaceACLUpdate(nonExistentUserEmail, WorkspaceAccessLevels.Owner, None)
    )

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.getUserIdInfo(ArgumentMatchers.eq(nonExistentUserEmail), any()))
      .thenReturn(Future.successful(SamDAO.NotFound))
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("owner"),
            SamWorkspaceActions.sharePolicy("reader"),
            SamWorkspaceActions.sharePolicy("share-reader")
        )
      )
    )

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                       workspaceRepository = workspaceRepository,
                                                       fastPassServiceConstructor = _ => fastPassService,
                                                       rawlsWorkspaceAclManager = aclManager
    )(ctx)
    val result = Await.result(workspaceService.updateACL(workspace.toWorkspaceName, aclUpdate, true), Duration.Inf)

    val responseFromUpdate =
      WorkspaceACLUpdateResponseList(
        Set.empty,
        Set(WorkspaceACLUpdate(nonExistentUserEmail, WorkspaceAccessLevels.Owner, Some(true), Some(true))),
        Set.empty
      )

    assertResult(responseFromUpdate, "Update ACL should return non-existent users") {
      result
    }

    verify(samDAO).inviteUser(ArgumentMatchers.eq(nonExistentUserEmail), any())
  }

  it should "add correct policies for a group" in {
    val groupEmail = "groupTest@example.com"
    val aclUpdate = WorkspaceACLUpdate(groupEmail, WorkspaceAccessLevels.Write)

    val samDAO = getSamDaoWithBasicMocking
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        SamWorkspacePolicyNames.all.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    when(samDAO.getUserIdInfo(ArgumentMatchers.eq(groupEmail), any()))
      .thenReturn(Future.successful(SamDAO.NotUser))

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                       workspaceRepository = workspaceRepository,
                                                       fastPassServiceConstructor = _ => fastPassService,
                                                       rawlsWorkspaceAclManager = aclManager
    )(ctx)
    Await.result(workspaceService.updateACL(workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                 Duration.Inf
    )

    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.writer, WorkbenchEmail(groupEmail), ctx)
    verify(aclManager).addUserToPolicy(workspace, SamWorkspacePolicyNames.canCompute, WorkbenchEmail(groupEmail), ctx)
    verify(aclManager, never).removeUserFromPolicy(any(), any(), any(), any())
  }

  val aclTestUserEmail = "permutations@example.com"
  for (aclUpdate <- allWorkspaceAclUpdatePermutations(aclTestUserEmail))
    it should s"add correctPolicies for $aclUpdate" in {
      val samDAO = getSamDaoWithBasicMocking
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          SamWorkspacePolicyNames.all.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )

      val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
      when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
      when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
      when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

      val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
      when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

      val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
      when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

      val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                         workspaceRepository = workspaceRepository,
                                                         fastPassServiceConstructor = _ => fastPassService,
                                                         rawlsWorkspaceAclManager = aclManager
      )(ctx)
      val result = Try {
        Await.result(workspaceService.updateACL(workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                     Duration.Inf
        )
      }

      (expectedPolicies(aclUpdate), result) match {
        case (Left(statusCode), util.Failure(exception: RawlsExceptionWithErrorReport)) =>
          assertResult(Some(statusCode), result.toString) {
            exception.errorReport.statusCode
          }

        case (Right(policies), util.Success(_)) =>
          policies.map {
            case (SamResourceTypeNames.workspace, policyName) =>
              verify(aclManager).addUserToPolicy(workspace, policyName, WorkbenchEmail(aclTestUserEmail), ctx)
            case _ => fail(policies.toString)
          }
          verify(samDAO, never).removeUserFromPolicy(any(), any(), any(), any(), any())
        case (_, r) => fail(r.toString)
      }
    }

  for (
    testPolicyName1 <- SamWorkspacePolicyNames.all - SamWorkspacePolicyNames.canCatalog;
    testPolicyName2 <- SamWorkspacePolicyNames.all - SamWorkspacePolicyNames.canCatalog
    if testPolicyName1 != testPolicyName2 && !(testPolicyName1 == SamWorkspacePolicyNames.shareReader && testPolicyName2 == SamWorkspacePolicyNames.shareWriter) && !(testPolicyName1 == SamWorkspacePolicyNames.shareWriter && testPolicyName2 == SamWorkspacePolicyNames.shareReader)
  )
    it should s"remove $testPolicyName1 and $testPolicyName2" in {
      val samDAO = getSamDaoWithBasicMocking
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          SamWorkspacePolicyNames.all.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )

      val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
      when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
        Future.successful(
          Set(WorkbenchEmail(aclTestUserEmail) -> testPolicyName1, WorkbenchEmail(aclTestUserEmail) -> testPolicyName2)
        )
      )
      when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
      when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
      when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

      val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
      when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

      val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
      when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

      val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
      when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

      val workspaceService = workspaceServiceConstructor(
        samDAO = samDAO,
        workspaceRepository = workspaceRepository,
        fastPassServiceConstructor = _ => fastPassService,
        requesterPaysSetupService = requesterPaysSetupService,
        rawlsWorkspaceAclManager = aclManager
      )(ctx)
      val result = Try {
        Await.result(
          workspaceService.updateACL(
            workspace.toWorkspaceName,
            Set(WorkspaceACLUpdate(aclTestUserEmail, WorkspaceAccessLevels.NoAccess)),
            inviteUsersNotFound = false
          ),
          Duration.Inf
        )
      }

      if (
        testPolicyName1 == SamWorkspacePolicyNames.projectOwner || testPolicyName2 == SamWorkspacePolicyNames.projectOwner
      ) {
        val error = intercept[RawlsExceptionWithErrorReport] {
          result.get
        }
        assertResult(Some(StatusCodes.BadRequest), result.toString) {
          error.errorReport.statusCode
        }
      } else {
        assert(result.isSuccess, result.toString)
        verify(aclManager).removeUserFromPolicy(workspace, testPolicyName1, WorkbenchEmail(aclTestUserEmail), ctx)
        verify(aclManager).removeUserFromPolicy(workspace, testPolicyName2, WorkbenchEmail(aclTestUserEmail), ctx)
        verify(aclManager, never).addUserToPolicy(any(), any(), any(), any())
      }
    }

  for (
    testPolicyName <- Set(SamWorkspacePolicyNames.writer,
                          SamWorkspacePolicyNames.reader,
                          SamWorkspacePolicyNames.owner
    );
    aclUpdate <- Set(WorkspaceAccessLevels.Read, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Owner)
      .map(l => WorkspaceACLUpdate(aclTestUserEmail, l, canCompute = Some(false)))
  )
    it should s"change $testPolicyName to $aclUpdate" in {
      val samDAO = getSamDaoWithBasicMocking
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          SamWorkspacePolicyNames.all.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )
      val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
      when(aclManager.getWorkspacePolicies(any(), any()))
        .thenReturn(Future.successful(Set(WorkbenchEmail(aclTestUserEmail) -> testPolicyName)))
      when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
      when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
      when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

      val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
      when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))
      val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
      when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))
      val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
      when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))
      val workspaceService = workspaceServiceConstructor(
        samDAO = samDAO,
        workspaceRepository = workspaceRepository,
        fastPassServiceConstructor = _ => fastPassService,
        requesterPaysSetupService = requesterPaysSetupService,
        rawlsWorkspaceAclManager = aclManager
      )(ctx)

      val result = Try {
        Await.result(workspaceService.updateACL(workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                     Duration.Inf
        )
      }

      assert(result.isSuccess, result.toString)

      if (aclUpdate.accessLevel.toPolicyName.contains(testPolicyName.value)) {
        verify(aclManager, never).addUserToPolicy(any(), any(), any(), any())
        verify(aclManager, never).removeUserFromPolicy(any(), any(), any(), any())
      } else {
        verify(aclManager).removeUserFromPolicy(workspace, testPolicyName, WorkbenchEmail(aclTestUserEmail), ctx)
        verify(aclManager).addUserToPolicy(workspace,
                                           SamResourcePolicyName(aclUpdate.accessLevel.toPolicyName.get),
                                           WorkbenchEmail(aclTestUserEmail),
                                           ctx
        )
      }
    }
}
