package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{
  LeonardoDAO,
  MockLeonardoDAO,
  SamDAO,
  WorkspaceManagerResourceMonitorRecordDao
}
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceType.McWorkspace
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeString,
  ErrorReport,
  GoogleProjectId,
  RawlsBillingProject,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceDeletionResult,
  WorkspaceName,
  WorkspacePolicy,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType,
  WorkspaceVersions
}
import org.broadinstitute.dsde.rawls.workspace.MultiCloudWorkspaceService.getStorageContainerName
import org.broadinstitute.dsde.workbench.client.leonardo
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString, eq => equalTo}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with OptionValues
    with TestDriverComponent {
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")
  implicit val workbenchMetricBaseName: ShardId = "test"

  def activeMcWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    MultiCloudWorkspaceManagerConfig("fake_app_id", 60 seconds, 120 seconds),
    AzureConfig(
      "fake-landing-zone-definition",
      "fake-protected-landing-zone-definition",
      "fake-landing-zone-version",
      Map("fake_parameter" -> "fake_value"),
      Map("fake_parameter" -> "fake_value"),
      landingZoneAllowAttach = false
    )
  )

  behavior of "createMultiCloudWorkspace"

  // TODO replace with a test that just checks the result of the transformation is passed
  it should "create a workspace with the requested policies" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[MockLeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val policies = List(
      WorkspacePolicy("protected-data", "terra", List.empty),
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      protectedData = None,
      policies = Some(policies)
    )

    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_name"),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        ArgumentMatchers.eq(
          Some(
            new WsmPolicyInputs()
              .inputs(
                Seq(
                  new WsmPolicyInput()
                    .name("protected-data")
                    .namespace("terra"),
                  new WsmPolicyInput()
                    .name("group-constraint")
                    .namespace("terra")
                    .additionalData(List(new WsmPolicyPair().key("group").value("myFakeGroup")).asJava),
                  new WsmPolicyInput()
                    .name("region-constraint")
                    .namespace("other-namespace")
                    .additionalData(
                      List(new WsmPolicyPair().key("key1").value("value1"),
                           new WsmPolicyPair().key("key2").value("value2")
                      ).asJava
                    )
                ).asJava
              )
          )
        ),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO, Mockito.times(0))
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.any[UUID](),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any(),
        any(),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.any()
      )
  }

  behavior of "cloneMultiCloudWorkspace"

  def withMockedMultiCloudWorkspaceService(runTest: MultiCloudWorkspaceService => Assertion,
                                           samDAO: SamDAO = new MockSamDAO(slickDataSource)
  ): Assertion = {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    val leonardoDAO: LeonardoDAO = spy(new MockLeonardoDAO())

    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpmDAO,
        new MockSamDAO(slickDataSource),
        config,
        leonardoDAO,
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

    // For testing of old Azure billing projects, can be removed if that code is removed.
    doReturn(Future.successful(testData.oldAzureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.oldAzureBillingProject.projectName), any())
    doReturn(Some(testData.oldAzureBillingProfile))
      .when(bpmDAO)
      .getBillingProfile(equalTo(testData.oldAzureBillingProfile.getId), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getV2WorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getV2WorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    runTest(mcWorkspaceService)
  }

  it should "fail if the destination workspace already exists" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.azureWorkspace)
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
          mcWorkspaceService.workspaceManagerDAO.cloneWorkspace(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                any(),
                                                                equalTo("kifflom"),
                                                                any(),
                                                                equalTo(testData.azureWorkspace.namespace),
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

  it should "throw an exception if the billing profile was created before 9/12/2023" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.oldAzureBillingProject.projectName.value, "unsupported")
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.azureWorkspace)
              _ <- mcWorkspaceService.cloneMultiCloudWorkspace(
                mock[WorkspaceService](RETURNS_SMART_NULLS),
                testData.azureWorkspace.toWorkspaceName,
                WorkspaceRequest(
                  cloneName.namespace,
                  cloneName.name,
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
        result.errorReport.statusCode.value shouldBe StatusCodes.Forbidden
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

        intercept[WorkspaceManagerOperationFailureException] {
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
          .cloneAzureStorageContainer(any(), any(), any(), any(), any(), any(), any())

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
          .cloneAzureStorageContainer(any(), any(), any(), any, any(), any(), any())

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

  it should "deploy a WDS instance during workspace clone" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
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
              .resourceId(sourceContainerUUID)
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
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning deploys WDS and then a very simple
          // validation of the clone success.
          verify(mcWorkspaceService.leonardoDAO, times(1))
            .createWDSInstance(anyString(),
                               equalTo(clone.toWorkspace.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspace.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe Some(McWorkspace)
          clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
        },
        Duration.Inf
      )
    }
  }

  it should "not deploy a WDS instance during workspace clone if test attribute is set as a string" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
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
              .resourceId(sourceContainerUUID)
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
              Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeString("true"))
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning does not deploy WDS.
          verify(mcWorkspaceService.leonardoDAO, never())
            .createWDSInstance(anyString(),
                               equalTo(clone.toWorkspace.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspace.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe Some(McWorkspace)
          clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
        },
        Duration.Inf
      )
    }
  }

  it should "clone the workspace even if WDS instance creation fails" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
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
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      // for this test, throw an error on WDS deployment
      when(mcWorkspaceService.leonardoDAO.createWDSInstance(anyString(), any[UUID](), any()))
        .thenAnswer(_ => throw new leonardo.ApiException(500, "intentional Leo exception for unit test"))

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
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning deploys WDS and then a very simple
          // validation of the clone success.
          verify(mcWorkspaceService.leonardoDAO, times(1))
            .createWDSInstance(anyString(),
                               equalTo(clone.toWorkspace.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspace.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe Some(McWorkspace)
          clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
        },
        Duration.Inf
      )
    }
  }

  it should
    "clone an azure workspace" +
    " & create a new workspace record with merged attributes" +
    " & create a new job for the workspace manager resource monitor" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        val sourceContainerUUID = UUID.randomUUID()
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
                .resourceId(sourceContainerUUID)
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
                Map(
                  AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
                ),
                None,
                Some("analyses/"),
                policies = Some(
                  List(
                    WorkspacePolicy("dummy-policy-name", "terra", List.empty)
                  )
                )
              )
            )
            // Ensure test data has the attribute we expect to merge in.
            _ = testData.azureWorkspace.attributes shouldBe Map(
              AttributeName.withDefaultNS("description") -> AttributeString("source description")
            )
            _ = clone.toWorkspace.toWorkspaceName shouldBe cloneName
            _ = clone.workspaceType shouldBe Some(McWorkspace)
            _ = clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
            _ = clone.attributes.get shouldBe Map(
              AttributeName.withDefaultNS("description") -> AttributeString("source description"),
              AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
            )

            jobs <- slickDataSource.inTransaction { access =>
              for {
                // the newly cloned workspace should be persisted
                clone_ <- access.workspaceQuery.findByName(cloneName)
                _ = clone_.value.workspaceId shouldBe clone.workspaceId

                // a new resource monitor job should be created
                jobs <- access.WorkspaceManagerResourceMonitorRecordQuery
                  .selectByWorkspaceId(clone.toWorkspace.workspaceIdAsUUID)
              } yield jobs
            }
          } yield {
            verify(mcWorkspaceService.workspaceManagerDAO, times(1))
              .cloneWorkspace(
                equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                equalTo(clone.toWorkspace.workspaceIdAsUUID),
                equalTo(cloneName.name),
                any(),
                equalTo(cloneName.namespace),
                any(),
                equalTo(
                  Some(
                    new WsmPolicyInputs()
                      .inputs(
                        Seq(
                          new WsmPolicyInput()
                            .name("dummy-policy-name")
                            .namespace("terra")
                        ).asJava
                      )
                  )
                )
              )
            verify(mcWorkspaceService.workspaceManagerDAO, times(1))
              .cloneAzureStorageContainer(
                equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                equalTo(clone.toWorkspace.workspaceIdAsUUID),
                equalTo(sourceContainerUUID),
                equalTo(getStorageContainerName(clone.toWorkspace.workspaceIdAsUUID)),
                equalTo(CloningInstructionsEnum.RESOURCE),
                equalTo(Some("analyses/")),
                any()
              )
            clone.completedCloneWorkspaceFileTransfer shouldBe None
            jobs.size shouldBe 1
            jobs.head.jobType shouldBe JobType.CloneWorkspaceContainerResult
            jobs.head.workspaceId.value.toString shouldBe clone.workspaceId
          },
          Duration.Inf
        )
      }
    }

  behavior of "deleteMultiCloudOrRawlsWorkspace"

  it should "delete an MC workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.azureWorkspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield result shouldBe None,
          Duration.Inf
        )

        verify(mcWorkspaceService).deleteWorkspaceInWSM(equalTo(testData.azureWorkspace.workspaceIdAsUUID))
        assertWorkspaceGone(testData.azureWorkspace)
      }
    }
  }

  it should "delete a rawls workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.workspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
        when(workspaceService.deleteWorkspace(ArgumentMatchers.eq(wsName)))
          .thenReturn(
            Future.successful(WorkspaceDeletionResult.fromGcpBucketName(testData.workspace.bucketName))
          )

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.workspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield {
            verify(workspaceService).deleteWorkspace(equalTo(wsName))
            result shouldBe Some(testData.workspace.bucketName)
          },
          Duration.Inf
        )
      }
    }
  }

  it should "leave the rawls state in place if WSM returns an error during workspace deletion" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Creating
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ => throw new ApiException(500, "error"))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]), Duration.Inf)
    }

    verify(wsmDAO).getWorkspace(workspaceId, testContext)
    verify(wsmDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
    verify(workspaceRepository, times(0)).deleteWorkspace(workspace)

  }

  it should "delete the rawls record if the WSM record is not found" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Creating
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    when(workspaceRepository.deleteWorkspace(workspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ => throw new ApiException(404, "not found"))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result =
      Await.result(service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]), Duration.Inf)
    result shouldBe None

    verify(wsmDAO).getWorkspace(workspaceId, testContext)
    verify(wsmDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
    verify(workspaceRepository).deleteWorkspace(workspace)
  }

  behavior of "deleteWorkspaceInWSM"

  it should "not attempt to delete a workspace and raise an exception if WSM returns a failure when getting the workspace" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(403, "forbidden")
    )

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.errorReport.statusCode shouldEqual Some(StatusCodes.InternalServerError)
    verify(svc.workspaceManagerDAO, times(0)).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                anyString(),
                                                                any[RawlsRequestContext]
    )
  }

  it should "not attempt to delete a workspace in WSM that does not exist in WSM" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(404, "workspace not found")
    )

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO, times(0)).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                anyString(),
                                                                any[RawlsRequestContext]
    )
  }

  it should "fail for for non 403 errors" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(
      _ => throw new ApiException(StatusCodes.BadRequest.intValue, "failure")
    )
    val samDAO = new MockSamDAO(slickDataSource)
    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[ApiException] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(1)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "fail for for non-ApiException errors" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(
      _ => throw new Exception("failure")
    )
    val samDAO = new MockSamDAO(slickDataSource)
    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[Exception] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(1)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "delete the rawls workspace when workspace manager returns 403 during polling" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0

      override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                              jobControlId: String,
                                              ctx: RawlsRequestContext
      ): JobResult = {
        times = times + 1
        if (times > 1) {
          throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden")
        } else {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.RUNNING))
        }
      }
    })

    val samDAO = new MockSamDAO(slickDataSource)

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(2)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "start deletion and poll for success when the workspace exists in workspace manager" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0
      override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                              jobControlId: String,
                                              ctx: RawlsRequestContext
      ): JobResult = {
        times = times + 1
        if (times > 1) {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.SUCCEEDED))
        } else {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.RUNNING))
        }
      }
    })

    val samDAO = new MockSamDAO(slickDataSource)

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(2)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  private def assertWorkspaceGone(workspace: Workspace) = {
    // fail if the workspace exists
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findByName(workspace.toWorkspaceName)),
      Duration.Inf
    )

    existing shouldBe empty
  }

  private def insertWorkspaceWithBillingProject(billingProject: RawlsBillingProject, workspace: Workspace) =
    slickDataSource.inTransaction { access =>
      access.rawlsBillingProjectQuery.create(billingProject) >> access.workspaceQuery.createOrUpdate(workspace)
    }
}
