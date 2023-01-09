package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceType.McWorkspace
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ErrorReport,
  MultiCloudWorkspaceRequest,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamResourceTypeNames,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceName,
  WorkspaceRequest,
  WorkspaceType
}
import org.mockito.ArgumentMatchers.{any, eq => equalTo}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec extends AnyFlatSpec with Matchers with OptionValues with TestDriverComponent {

  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")
  implicit val workbenchMetricBaseName: ShardId = "test"

  def activeMcWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    multiCloudWorkspacesEnabled = true,
    Some(MultiCloudWorkspaceManagerConfig("fake_app_id", 60 seconds)),
    Some(
      AzureConfig(
        "fake_group",
        "fake-landing-zone-definition",
        "fake-landing-zone-version"
      )
    )
  )

  behavior of "createMultiCloudOrRawlsWorkspace"

  it should "delegate legacy creation requests to WorkspaceService" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        mock[BillingProfileManagerDAO],
        samDAO,
        config,
        workbenchMetricBaseName
      )(testContext)
    )
    val workspaceRequest = WorkspaceRequest(
      "fake_billing_project",
      UUID.randomUUID().toString,
      Map.empty
    )
    val billingProject = mock[RawlsBillingProject]
    when(billingProject.projectName).thenReturn(RawlsBillingProjectName("fake_billing_project"))
    when(billingProject.billingProfileId).thenReturn(None)

    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    doReturn(Future.successful(billingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    when(workspaceService.createWorkspace(any(), any())).thenReturn(
      Future.successful(
        Workspace("fake", "fake", "fake", "fake", None, currentTime(), currentTime(), "fake", Map.empty)
      )
    )

    val result = Await.result(
      mcWorkspaceService.createMultiCloudOrRawlsWorkspace(workspaceRequest, workspaceService),
      Duration.Inf
    )

    result.workspaceType shouldBe WorkspaceType.RawlsWorkspace
    verify(workspaceService, Mockito.times(1))
      .createWorkspace(equalTo(workspaceRequest), any())
  }

  it should "not delegate when called with an azure billing project" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val bpDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS);
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpDAO,
        samDAO,
        config,
        workbenchMetricBaseName
      )(testContext)
    )
    val workspaceRequest = WorkspaceRequest(
      "test-bp-name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpDAO)
      .getBillingProfile(any[UUID], any[RawlsRequestContext])

    val result = Await.result(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(
                                workspaceRequest,
                                workspaceService
                              ),
                              Duration.Inf
    )

    result.workspaceType shouldBe WorkspaceType.McWorkspace
  }

  it should "return forbidden if creating a workspace against a billing project that the user does not have the createWorkspace action for" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = Mockito.spy(new MockSamDAO(slickDataSource))
    val bpDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS);

    when(
      samDAO.userHasAction(SamResourceTypeNames.billingProject,
                           "fake_mc_billing_project_name",
                           SamBillingProjectActions.createWorkspace,
                           testContext
      )
    ).thenReturn(Future.successful(false))
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpDAO,
        samDAO,
        config,
        workbenchMetricBaseName
      )(testContext)
    )
    val workspaceRequest = WorkspaceRequest(
      "fake_mc_billing_project_name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpDAO)
      .getBillingProfile(any[UUID], any[RawlsRequestContext])

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(
                     workspaceRequest,
                     workspaceService
                   ),
                   Duration.Inf
      )
    }

    actual.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if the billing profile is not found" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    when(bpmDAO.getBillingProfile(any[UUID], any[RawlsRequestContext])).thenReturn(None)
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpmDAO,
        samDAO,
        config,
        workbenchMetricBaseName
      )(testContext)
    )
    val request = WorkspaceRequest(
      "fake_mc_billing_project_name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        mcWorkspaceService.createMultiCloudOrRawlsWorkspace(request, workspaceService),
        Duration.Inf
      )
    }

    assert(actual.errorReport.message.contains("Unable to find billing profile"))
  }

  behavior of "createMultiCloudWorkspace"

  it should "throw an exception if creating a multi-cloud workspace is not enabled" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(multiCloudWorkspacesEnabled = false, None, None)
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      config,
      workbenchMetricBaseName
    )(testContext)
    val request = MultiCloudWorkspaceRequest(
      "fake",
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      mock[AzureManagedAppCoordinates],
      "fake_billingProjectId"
    )

    val actual = intercept[RawlsExceptionWithErrorReport] {
      mcWorkspaceService.createMultiCloudWorkspace(request)
    }

    actual.errorReport.statusCode shouldBe Some(StatusCodes.NotImplemented)
  }

  it should "throw an exception if a workspace with the same name already exists" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      workbenchMetricBaseName
    )(testContext)
    val request = MultiCloudWorkspaceRequest(
      "fake",
      s"fake-name-${UUID.randomUUID().toString}",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake"),
      "fake_billingProjectId"
    )

    Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  it should "create a workspace" in {
    val subscriptionId = UUID.randomUUID()
    val tenantId = UUID.randomUUID()

    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = MultiCloudWorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      AzureManagedAppCoordinates(tenantId, subscriptionId, "fake_mrg_id"),
      "fake_billingProjectId"
    )
    val result: Workspace = Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .enableApplication(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_app_id"),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureWorkspaceCloudContext(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(tenantId.toString),
        ArgumentMatchers.eq("fake_mrg_id"),
        ArgumentMatchers.eq(subscriptionId.toString),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId))),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "fail on cloud context creation failure" in {
    val workspaceManagerDAO =
      MockWorkspaceManagerDAO.buildWithAsyncCloudContextResult(StatusEnum.FAILED)
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = MultiCloudWorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "managed_resource_group_id"),
      "fake_billingProjectId"
    )

    intercept[WorkspaceManagerCreationFailureException] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    }
  }

  behavior of "cloneMultiCloudWorkspace"

  def withMockedMultiCloudWorkspaceService(runTest: MultiCloudWorkspaceService => Assertion): Assertion = {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpmDAO,
        new MockSamDAO(slickDataSource),
        config,
        workbenchMetricBaseName
      )(testContext)
    )

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.azureBillingProject.projectName), any())

    doReturn(Future.successful(testData.billingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.billingProject.projectName), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpmDAO)
      .getBillingProfile(equalTo(testData.azureBillingProfile.getId), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    runTest(mcWorkspaceService)
  }

  it should "fail if the destination workspace already exists" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- slickDataSource.inTransaction { access =>
                access.rawlsBillingProjectQuery.create(testData.billingProject) >>
                  access.workspaceQuery.createOrUpdate(testData.azureWorkspace)
              }
              _ <- mcWorkspaceService.cloneMultiCloudWorkspace(
                mock[WorkspaceService](RETURNS_SMART_NULLS),
                testData.azureWorkspace.toWorkspaceName,
                WorkspaceRequest(
                  testData.azureWorkspace.namespace,
                  testData.azureWorkspace.name,
                  Map.empty
                )
              )
            } yield fail(
              "cloneMultiCloudWorkspace does not fail when a workspace " +
                "with the same name already exists."
            ),
            Duration.Inf
          )
        }

        result.errorReport.statusCode.value shouldBe StatusCodes.Conflict
      }
    }

  it should "not create a workspace record if the request to Workspace Manager fails" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.cloneWorkspace(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.ImATeapot, "short and stout"))
        )

        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        // preserve the error workspace manager returned
        result.errorReport.statusCode.value shouldBe StatusCodes.ImATeapot

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if the Workspace Manager clone operation fails" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.getCloneWorkspaceResult(
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) => MockWorkspaceManagerDAO.getCloneWorkspaceResult(StatusEnum.FAILED))

        intercept[WorkspaceManagerCreationFailureException] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if the workspace has no storage containers" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        // workspaceManagerDAO returns an empty ResourceList by default for enumerateStorageContainers

        val actual = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        assert(actual.errorReport.message.contains("does not have the expected storage container"))

        verify(mcWorkspaceService.workspaceManagerDAO, never())
          .cloneAzureStorageContainer(any(), any(), any, any(), any())

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if there is no storage container with the correct name" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          new ResourceList().addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name("not the correct name")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
        )

        val actual = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        assert(actual.errorReport.message.contains("does not have the expected storage container"))

        verify(mcWorkspaceService.workspaceManagerDAO, never())
          .cloneAzureStorageContainer(any(), any(), any, any(), any())

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not allow users to clone azure workspaces into gcp billing projects" in
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              testData.billingProject.projectName.value,
              UUID.randomUUID().toString,
              Map.empty
            )
          ),
          Duration.Inf
        )
      }

      result.errorReport.statusCode.value shouldBe StatusCodes.BadRequest
    }

  it should "not allow users to clone gcp workspaces into azure billing projects" in
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.workspace.toWorkspaceName,
            WorkspaceRequest(
              testData.azureBillingProject.projectName.value,
              UUID.randomUUID().toString,
              Map.empty
            )
          ),
          Duration.Inf
        )
      }

      result.errorReport.statusCode.value shouldBe StatusCodes.BadRequest
    }

  it should
    "clone an azure workspace" +
    " & create a new workspace record" +
    " & create a new job for the workspace manager resource monitor" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        when(
          mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          new ResourceList().addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
        )
        Await.result(
          for {
            _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
            clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            )
            _ = clone.toWorkspaceName shouldBe cloneName
            _ = clone.workspaceType shouldBe McWorkspace

            jobs <- slickDataSource.inTransaction { access =>
              for {
                // the newly cloned workspace should be persisted
                clone_ <- access.workspaceQuery.findByName(cloneName)
                _ = clone_.value.workspaceId shouldBe clone.workspaceId

                // a new resource monitor job should be created
                jobs <- access.WorkspaceManagerResourceMonitorRecordQuery
                  .selectByWorkspaceId(clone.workspaceIdAsUUID)
              } yield jobs
            }
          } yield {
            clone.completedCloneWorkspaceFileTransfer shouldBe None
            jobs.size shouldBe 1
            jobs.head.jobType shouldBe JobType.CloneWorkspaceResult
            jobs.head.workspaceId.value.toString shouldBe clone.workspaceId
          },
          Duration.Inf
        )
        verify(mcWorkspaceService.workspaceManagerDAO, times(1))
          .cloneAzureStorageContainer(any(),
                                      any(),
                                      any,
                                      equalTo(CloningInstructionsEnum.RESOURCE),
                                      any()
          ) shouldBe null // expects an assertion to be returned
      }
    }
}
