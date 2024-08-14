package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.cloud.storage.BucketInfo.LifecycleRule
import com.google.cloud.storage.BucketInfo.LifecycleRule.{LifecycleAction, LifecycleCondition}
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{
  GcpBucketLifecycleAction,
  GcpBucketLifecycleCondition,
  GcpBucketLifecycleConfig,
  GcpBucketLifecycleRule
}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  UserInfo,
  Workspace,
  WorkspaceSetting,
  WorkspaceSettingTypes
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{contain, include}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class WorkspaceSettingServiceUnitTests extends AnyFlatSpec with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  def workspaceSettingServiceConstructor(
    ctx: RawlsRequestContext = defaultRequestContext,
    workspaceSettingRepository: WorkspaceSettingRepository = mock[WorkspaceSettingRepository](
      RETURNS_SMART_NULLS
    ),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
  ): WorkspaceSettingService =
    new WorkspaceSettingService(ctx, workspaceSettingRepository, workspaceRepository, gcsDAO, samDAO)

  val workspace: Workspace = Workspace(
    "settingsTestWorkspace",
    "settingsTestNamespace",
    UUID.randomUUID.toString,
    "bucketName",
    Some("workflowCollection"),
    new DateTime(),
    new DateTime(),
    "creator",
    Map.empty
  )

  "getWorkspaceSettings" should "return the workspace settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val workspaceSettings = List(
      WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                       WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(workspaceSettings))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO,
                                         workspaceRepository = workspaceRepository,
                                         workspaceSettingRepository = workspaceSettingRepository
      )

    val returnedSettings = Await.result(service.getWorkspaceSettings(workspaceName), Duration.Inf)
    returnedSettings shouldEqual workspaceSettings
    verify(samDAO).userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 ArgumentMatchers.eq(workspaceId.toString),
                                 ArgumentMatchers.eq(SamWorkspaceActions.read),
                                 any()
    )
  }

  it should "handle a workspace with no settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List.empty))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO,
                                         workspaceRepository = workspaceRepository,
                                         workspaceSettingRepository = workspaceSettingRepository
      )

    val returnedSettings = Await.result(service.getWorkspaceSettings(workspaceName), Duration.Inf)
    returnedSettings shouldEqual List.empty
    verify(samDAO).userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 ArgumentMatchers.eq(workspaceId.toString),
                                 ArgumentMatchers.eq(SamWorkspaceActions.read),
                                 any()
    )
  }

  it should "return an error if the user does not have read access to the workspace" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(false))
    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)

    assertThrows[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceSettings(workspaceName), Duration.Inf)
    }
  }

  "setWorkspaceSettings" should "set the workspace settings if there aren't any set" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val workspaceSetting = WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                                            WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List.empty))
    when(
      workspaceSettingRepository.createWorkspaceSettingsRecords(workspaceId,
                                                                List(workspaceSetting),
                                                                defaultRequestContext.userInfo.userSubjectId
      )
    )
      .thenReturn(Future.successful(List(workspaceSetting)))
    when(workspaceSettingRepository.markWorkspaceSettingApplied(workspaceId, workspaceSetting.settingType))
      .thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List())).thenReturn(Future.successful())

    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO,
                                         workspaceRepository = workspaceRepository,
                                         gcsDAO = gcsDAO,
                                         workspaceSettingRepository = workspaceSettingRepository
      )

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List(workspaceSetting)), Duration.Inf)
    res.successes should contain theSameElementsAs List(workspaceSetting)
    res.failures shouldEqual Map.empty
  }

  it should "overwrite existing settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("prefixToMatch")), Some(30))
          )
        )
      )
    )
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("muchBetterPrefix")), Some(31))
          )
        )
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId))
      .thenReturn(Future.successful(List(existingSetting)))
    when(
      workspaceSettingRepository.createWorkspaceSettingsRecords(workspaceId,
                                                                List(newSetting),
                                                                defaultRequestContext.userInfo.userSubjectId
      )
    )
      .thenReturn(Future.successful(List(newSetting)))
    when(workspaceSettingRepository.markWorkspaceSettingApplied(workspaceId, newSetting.settingType))
      .thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    val newSettingGoogleRule = new LifecycleRule(
      LifecycleAction.newDeleteAction(),
      LifecycleCondition.newBuilder().setMatchesPrefix(List("muchBetterPrefix").asJava).setAge(31).build()
    )
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List(newSettingGoogleRule))).thenReturn(Future.successful())

    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO,
                                         workspaceRepository = workspaceRepository,
                                         gcsDAO = gcsDAO,
                                         workspaceSettingRepository = workspaceSettingRepository
      )

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    res.successes should contain theSameElementsAs List(newSetting)
    res.failures shouldEqual Map.empty
  }

  it should "not remove existing settings if no settings are specified" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("prefixToMatch")), Some(30))
          )
        )
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId))
      .thenReturn(Future.successful(List(existingSetting)))
    when(
      workspaceSettingRepository.createWorkspaceSettingsRecords(workspaceId,
                                                                List.empty,
                                                                defaultRequestContext.userInfo.userSubjectId
      )
    )
      .thenReturn(Future.successful(List.empty))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service = workspaceSettingServiceConstructor(samDAO = samDAO,
                                                     workspaceRepository = workspaceRepository,
                                                     workspaceSettingRepository = workspaceSettingRepository
    )

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List.empty), Duration.Inf)
    res.successes shouldEqual List.empty
    res.failures shouldEqual Map.empty
  }

  it should "report errors while applying settings and remove pending settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("prefixToMatch")), Some(30))
          )
        )
      )
    )
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("muchBetterPrefix")), Some(31))
          )
        )
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId))
      .thenReturn(Future.successful(List(existingSetting)))
    when(
      workspaceSettingRepository.createWorkspaceSettingsRecords(workspaceId,
                                                                List(newSetting),
                                                                defaultRequestContext.userInfo.userSubjectId
      )
    )
      .thenReturn(Future.successful(List(newSetting)))
    when(workspaceSettingRepository.removePendingSetting(workspaceId, newSetting.settingType))
      .thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    val newSettingGoogleRule = new LifecycleRule(
      LifecycleAction.newDeleteAction(),
      LifecycleCondition.newBuilder().setMatchesPrefix(List("muchBetterPrefix").asJava).setAge(31).build()
    )
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List(newSettingGoogleRule)))
      .thenReturn(Future.failed(new Exception("failed to apply settings")))

    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO,
                                         workspaceRepository = workspaceRepository,
                                         gcsDAO = gcsDAO,
                                         workspaceSettingRepository = workspaceSettingRepository
      )

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    res.successes shouldEqual List.empty
    res.failures(WorkspaceSettingTypes.GcpBucketLifecycle) shouldEqual ErrorReport(StatusCodes.InternalServerError,
                                                                                   "failed to apply settings"
    )
    verify(workspaceSettingRepository).removePendingSetting(workspaceId, newSetting.settingType)
  }

  it should "be limited to owners" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val newSetting = WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                                      WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(false))
    // Rawls confirms a user has at least read access to the workspace after a failed authz check
    // to determine if it should throw a 403 or 404
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service =
      workspaceSettingServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  "validateSettings" should "require a non-negative age for GcpBucketLifecycle settings" in {
    val negativeAgeSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("prefixToMatch")), Some(-1))
          )
        )
      )
    )

    val service = workspaceSettingServiceConstructor()

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspace.toWorkspaceName, List(negativeAgeSetting)), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    exception.errorReport.message should include("Invalid settings requested.")
    exception.errorReport.causes should contain theSameElementsAs List(
      ErrorReport("Invalid GcpBucketLifecycle configuration: age must be a non-negative integer.")
    )
  }

  it should "not allow unsupported lifecycle actions for GcpBucketLifecycle settings" in {
    val unsupportedActionSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("SetStorageClass"),
                                 GcpBucketLifecycleCondition(Some(Set("prefixToMatch")), Some(10))
          )
        )
      )
    )

    val service = workspaceSettingServiceConstructor()

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspace.toWorkspaceName, List(unsupportedActionSetting)),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    exception.errorReport.message should include("Invalid settings requested.")
    exception.errorReport.causes should contain theSameElementsAs List(
      ErrorReport("Invalid GcpBucketLifecycle configuration: unsupported lifecycle action SetStorageClass.")
    )
  }

  it should "require at least one condition for GcpBucketLifecycle settings" in {
    val noConditionsSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(None, None))
        )
      )
    )

    val service = workspaceSettingServiceConstructor()

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspace.toWorkspaceName, List(noConditionsSetting)), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    exception.errorReport.message should include("Invalid settings requested.")
    exception.errorReport.causes should contain theSameElementsAs List(
      ErrorReport("Invalid GcpBucketLifecycle configuration: at least one condition must be specified.")
    )
  }

  it should "require at least one prefix if matchesPrefix is the only condition for GcpBucketLifecycle settings" in {
    val noPrefixSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Some(Set.empty), None))
        )
      )
    )

    val service = workspaceSettingServiceConstructor()

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspace.toWorkspaceName, List(noPrefixSetting)), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    exception.errorReport.message should include("Invalid settings requested.")
    exception.errorReport.causes should contain theSameElementsAs List(
      ErrorReport(
        "Invalid GcpBucketLifecycle configuration: at least one prefix must be specified if matchesPrefix is the only condition."
      )
    )
  }
}
