package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourcePolicyName,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  SamWorkspacePolicyNames,
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
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
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

  // Return mocks with reasonable defaults for basic usage. Override mocked behavior as needed.
  def getBasicMocks
    : (SamDAO, RawlsWorkspaceAclManager, WorkspaceRepository, FastPassService, RequesterPaysSetupService) = {

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    // Mock check for missing users so emails in AclUpdate show as registered in Terra
    when(samDAO.getUserIdInfo(any(), any()))
      .thenReturn(Future.successful(SamDAO.User(UserIdInfo("subjectId", "email", None))))
    // Mock enabled check for calling user
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("subjectId", "email", true))))
    // Mock successful Unit Sam call(s)
    when(samDAO.inviteUser(any(), any())).thenReturn(Future.successful(()))

    val aclManager = mock[RawlsWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(aclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.removeUserFromPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(aclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any())).thenReturn(Future.successful(()))

    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(workspace)))

    val fastPassService = mock[FastPassService](RETURNS_SMART_NULLS)
    when(fastPassService.syncFastPassesForUserInWorkspace(any(), any())).thenReturn(Future.successful(()))

    val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService)
  }

  val allWorkspacePolicies: Set[SamResourcePolicyName] = Set(
    SamWorkspacePolicyNames.owner,
    SamWorkspacePolicyNames.writer,
    SamWorkspacePolicyNames.reader,
    SamWorkspacePolicyNames.canCompute,
    SamWorkspacePolicyNames.shareReader,
    SamWorkspacePolicyNames.shareWriter
  )

  def allWorkspaceAclUpdatePermutations(emailString: String): Seq[WorkspaceACLUpdate] = for {
    accessLevel <- WorkspaceAccessLevels.all
    canShare <- Set(Some(true), Some(false), None)
    canCompute <- Set(Some(true), Some(false), None)
  } yield WorkspaceACLUpdate(emailString, accessLevel, canShare, canCompute)

  def expectedPoliciesForAclUpdatePermutationsTests(
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("owner"),
            SamWorkspaceActions.sharePolicy("reader"),
            SamWorkspaceActions.sharePolicy("share-reader")
        )
      )
    )

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

    val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"),
            SamWorkspaceActions.sharePolicy("reader"),
            SamWorkspaceActions.sharePolicy("can-compute")
        )
      )
    )
    when(aclManager.getWorkspacePolicies(any(), any()))
      .thenReturn(Future.successful(Set(readerEmail -> SamWorkspacePolicyNames.reader)))

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

    val (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"), SamWorkspaceActions.sharePolicy("can-compute"))
      )
    )
    when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
      Future.successful(
        Set(writerEmail -> SamWorkspacePolicyNames.writer, writerEmail -> SamWorkspacePolicyNames.canCompute)
      )
    )
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"),
            SamWorkspaceActions.sharePolicy("can-compute"),
            SamWorkspaceActions.sharePolicy("owner")
        )
      )
    )
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        Set(SamWorkspaceActions.sharePolicy("writer"),
            SamWorkspaceActions.sharePolicy("can-compute"),
            SamWorkspaceActions.sharePolicy("owner")
        )
      )
    )
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService) = getBasicMocks
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
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

    val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    when(samDAO.getUserIdInfo(ArgumentMatchers.eq(groupEmail), any()))
      .thenReturn(Future.successful(SamDAO.NotUser))

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

  it should "use the RawlsWorkspaceAclManager for Rawls workspaces" in {
    val (samDAO, rawlsWorkspaceAclManager, workspaceRepository, fastPassService, requesterPaysSetupService) =
      getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    val multiCloudWorkspaceAclManager = mock[MultiCloudWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(multiCloudWorkspaceAclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      requesterPaysSetupService = requesterPaysSetupService,
      rawlsWorkspaceAclManager = rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager = multiCloudWorkspaceAclManager
    )(ctx)
    val aclUpdate = WorkspaceACLUpdate("email@example.com", WorkspaceAccessLevels.Owner)
    Await.result(workspaceService.updateACL(workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                 Duration.Inf
    )

    verify(rawlsWorkspaceAclManager).addUserToPolicy(workspace,
                                                     SamWorkspacePolicyNames.owner,
                                                     WorkbenchEmail("email@example.com"),
                                                     ctx
    )
    verifyNoInteractions(multiCloudWorkspaceAclManager)
  }

  it should "use the MultiCloudWorkspaceAclManager for multi cloud workspaces" in {
    val mcWorkspace = workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val (samDAO, rawlsWorkspaceAclManager, workspaceRepository, fastPassService, requesterPaysSetupService) =
      getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    val multiCloudWorkspaceAclManager = mock[MultiCloudWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(multiCloudWorkspaceAclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(multiCloudWorkspaceAclManager.addUserToPolicy(any(), any(), any(), any())).thenReturn(Future.successful(()))
    when(multiCloudWorkspaceAclManager.maybeShareWorkspaceNamespaceCompute(any(), any(), any()))
      .thenReturn(Future.successful(()))
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(mcWorkspace)))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      requesterPaysSetupService = requesterPaysSetupService,
      rawlsWorkspaceAclManager = rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager = multiCloudWorkspaceAclManager
    )(ctx)
    val aclUpdate = WorkspaceACLUpdate("email@example.com", WorkspaceAccessLevels.Owner)
    Await.result(workspaceService.updateACL(mcWorkspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                 Duration.Inf
    )

    verify(multiCloudWorkspaceAclManager).addUserToPolicy(mcWorkspace,
                                                          SamWorkspacePolicyNames.owner,
                                                          WorkbenchEmail("email@example.com"),
                                                          ctx
    )
    verifyNoInteractions(rawlsWorkspaceAclManager)
  }

  it should "not allow share writers for McWorkspaces" in {
    val writerEmail = "writer@example.com"
    val mcWorkspace = workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val aclUpdate = WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, canShare = Some(true))

    val (samDAO, _, workspaceRepository, _, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    val multiCloudWorkspaceAclManager = mock[MultiCloudWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(multiCloudWorkspaceAclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(mcWorkspace)))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      multiCloudWorkspaceAclManager = multiCloudWorkspaceAclManager
    )(ctx)

    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(workspaceService.updateACL(mcWorkspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                   Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "not allow share readers for McWorkspaces" in {
    val readerEmail = "reader@example.com"
    val mcWorkspace = workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val aclUpdate = WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, canShare = Some(true))

    val (samDAO, _, workspaceRepository, _, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    val multiCloudWorkspaceAclManager = mock[MultiCloudWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(multiCloudWorkspaceAclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(mcWorkspace)))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      multiCloudWorkspaceAclManager = multiCloudWorkspaceAclManager
    )(ctx)

    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(workspaceService.updateACL(mcWorkspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                   Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "not allow compute writers for McWorkspaces" in {
    val writerEmail = "writer@example.com"
    val mcWorkspace = workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val aclUpdate =
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, canShare = Some(false), canCompute = Some(true))

    val (samDAO, _, workspaceRepository, _, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    val multiCloudWorkspaceAclManager = mock[MultiCloudWorkspaceAclManager](RETURNS_SMART_NULLS)
    when(multiCloudWorkspaceAclManager.getWorkspacePolicies(any(), any())).thenReturn(Future.successful(Set.empty))
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future.successful(Some(mcWorkspace)))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      multiCloudWorkspaceAclManager = multiCloudWorkspaceAclManager
    )(ctx)

    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(workspaceService.updateACL(mcWorkspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                   Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "not allow users to change their own ACL" in {
    val requesterEmail = ctx.userInfo.userEmail.value
    val aclUpdate = WorkspaceACLUpdate(requesterEmail, WorkspaceAccessLevels.Read)

    val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
    when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
      Future.successful(
        allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
      )
    )
    when(aclManager.getWorkspacePolicies(any(), any()))
      .thenReturn(Future.successful(Set(WorkbenchEmail(requesterEmail) -> SamWorkspacePolicyNames.owner)))

    val workspaceService = workspaceServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      fastPassServiceConstructor = _ => fastPassService,
      rawlsWorkspaceAclManager = aclManager
    )(ctx)

    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(workspaceService.updateACL(workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                   Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  val aclTestUserEmail = "permutations@example.com"

  // These are valid share and compute updates for a user with no permissions that should be rejected
  for (
    (aclUpdate, policies) <- Set(
      WorkspaceACLUpdate(aclTestUserEmail, WorkspaceAccessLevels.Owner) -> Set(SamWorkspacePolicyNames.owner),
      WorkspaceACLUpdate(aclTestUserEmail, WorkspaceAccessLevels.Write) -> Set(SamWorkspacePolicyNames.writer),
      WorkspaceACLUpdate(aclTestUserEmail,
                         WorkspaceAccessLevels.Write,
                         canShare = Some(true),
                         canCompute = Some(false)
      ) -> Set(SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.canCompute),
      WorkspaceACLUpdate(aclTestUserEmail,
                         WorkspaceAccessLevels.Write,
                         canShare = Some(false),
                         canCompute = Some(true)
      ) -> Set(SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.canCompute),
      WorkspaceACLUpdate(aclTestUserEmail,
                         WorkspaceAccessLevels.Write,
                         canShare = Some(true),
                         canCompute = Some(true)
      ) -> Set(SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.canCompute),
      WorkspaceACLUpdate(aclTestUserEmail, WorkspaceAccessLevels.Read) -> Set(SamWorkspacePolicyNames.reader),
      WorkspaceACLUpdate(aclTestUserEmail, WorkspaceAccessLevels.Read, canShare = Some(true)) -> Set(
        SamWorkspacePolicyNames.reader
      )
    )
  )
    it should s"require ${policies.map(p => SamWorkspaceActions.sharePolicy(p.value).value).mkString(",")} permissions to perform $aclUpdate" in {
      val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          (allWorkspacePolicies -- policies).map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )

      val workspaceService = workspaceServiceConstructor(samDAO = samDAO,
                                                         workspaceRepository = workspaceRepository,
                                                         fastPassServiceConstructor = _ => fastPassService,
                                                         rawlsWorkspaceAclManager = aclManager
      )(ctx)

      val exception = intercept[InvalidWorkspaceAclUpdateException] {
        Await.result(workspaceService.updateACL(workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false),
                     Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    }

  for (aclUpdate <- allWorkspaceAclUpdatePermutations(aclTestUserEmail))
    it should s"add correctPolicies for $aclUpdate" in {
      val (samDAO, aclManager, workspaceRepository, fastPassService, _) = getBasicMocks
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )

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

      (expectedPoliciesForAclUpdatePermutationsTests(aclUpdate), result) match {
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
    testPolicyName1 <- allWorkspacePolicies;
    testPolicyName2 <- allWorkspacePolicies
    if testPolicyName1 != testPolicyName2 && !(testPolicyName1 == SamWorkspacePolicyNames.shareReader && testPolicyName2 == SamWorkspacePolicyNames.shareWriter) && !(testPolicyName1 == SamWorkspacePolicyNames.shareWriter && testPolicyName2 == SamWorkspacePolicyNames.shareReader)
  )
    it should s"remove $testPolicyName1 and $testPolicyName2" in {
      val (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService) = getBasicMocks
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )
      when(aclManager.getWorkspacePolicies(any(), any())).thenReturn(
        Future.successful(
          Set(WorkbenchEmail(aclTestUserEmail) -> testPolicyName1, WorkbenchEmail(aclTestUserEmail) -> testPolicyName2)
        )
      )

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
      val (samDAO, aclManager, workspaceRepository, fastPassService, requesterPaysSetupService) = getBasicMocks
      when(samDAO.listUserActionsForResource(any(), any(), any())).thenReturn(
        Future.successful(
          allWorkspacePolicies.map(policyName => SamWorkspaceActions.sharePolicy(policyName.value))
        )
      )
      when(aclManager.getWorkspacePolicies(any(), any()))
        .thenReturn(Future.successful(Set(WorkbenchEmail(aclTestUserEmail) -> testPolicyName)))

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
