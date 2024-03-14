package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.{
  CloudPlatform => SnapshotCloudPlatform,
  DatasetSummaryModel,
  SnapshotModel,
  SnapshotSourceModel
}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.mock.MockDataRepoDAO
import org.broadinstitute.dsde.rawls.model.{
  DataReferenceDescriptionField,
  DataReferenceName,
  NamedDataRepoSnapshot,
  RawlsRequestContext,
  SamResourceAction,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamUserStatusResponse,
  WorkspaceType
}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class SnapshotServiceSpec extends AnyWordSpecLike with Matchers with MockitoSugar with TestDriverComponent {

  // create a mockito-powered SamDAO that always returns true for permission checks and returns
  // a test-fixture user info object
  private def defaultMockSamDao() = {
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
      .thenReturn(Future.successful(Seq.empty))
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                               any[String],
                               any[SamResourceAction],
                               any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))
    when(
      mockSamDAO.getUserStatus(any[RawlsRequestContext])
    ).thenReturn(
      Future.successful(
        Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
      )
    )

    mockSamDAO
  }

  // create a mockito-powered WorkspaceManagerDAO that always returns a stubbed WSM workspace, and succeeds when
  // creating a snapshot reference. Tests should add to or override these default behaviors to verify other scenarios.
  private def defaultMockWorkspaceManagerDao() = {
    val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext]))
      .thenReturn(new WorkspaceDescription().stage(WorkspaceStageModel.RAWLS_WORKSPACE))
    when(
      mockWorkspaceManagerDAO.createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    )
      .thenReturn(
        new DataRepoSnapshotResource()
          .metadata(
            new ResourceMetadata()
              .resourceId(UUID.randomUUID())
              .workspaceId(UUID.randomUUID())
              .name("foo")
              .description("")
              .cloningInstructions(CloningInstructionsEnum.NOTHING)
          )
          .attributes(new DataRepoSnapshotAttributes())
      )
    mockWorkspaceManagerDAO
  }

  // create a mockito-powered DataRepoDAO that always returns a stubbed snapshot
  private def defaultDataRepoDao(): DataRepoDAO = {
    val mockDataRepoDAO = mock[DataRepoDAO](RETURNS_SMART_NULLS)
    when(
      mockDataRepoDAO.getSnapshot(
        any[UUID],
        any[OAuth2BearerToken]
      )
    )
      .thenReturn(
        new SnapshotModel()
          .id(java.util.UUID.randomUUID())
          .name("snapshot")
          .description("snapshot description")
          .source(
            java.util.List
              .of(new SnapshotSourceModel().dataset(new DatasetSummaryModel().cloudPlatform(SnapshotCloudPlatform.GCP)))
          )
      )
    mockDataRepoDAO
  }

  "SnapshotService" should {
    "create a new snapshot reference to a TDR snapshot" in withMinimalTestDatabase { _ =>
      val mockSamDAO = defaultMockSamDao()
      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      val mockDataRepoDAO = defaultDataRepoDao()
      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      val snapshotUuid = UUID.randomUUID()
      val snapRefName = DataReferenceName("refname")
      val snapRefDescription = Option(DataReferenceDescriptionField("my reference description"))

      // call createSnapshot on the service
      Await.result(
        snapshotService.createSnapshotByWorkspaceName(workspace.toWorkspaceName,
                                                      NamedDataRepoSnapshot(snapRefName,
                                                                            snapRefDescription,
                                                                            snapshotUuid
                                                      )
        ),
        Duration.Inf
      )

      // assert that the service called WSM's createDataRepoSnapshotReference
      verify(mockWorkspaceManagerDAO).createDataRepoSnapshotReference(
        ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
        ArgumentMatchers.eq(snapshotUuid),
        ArgumentMatchers.eq(snapRefName),
        ArgumentMatchers.eq(snapRefDescription),
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "create a WSM workspace if one doesn't exist when creating a snapshot reference" in withMinimalTestDatabase { _ =>
      val mockSamDAO = defaultMockSamDao()

      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      when(
        mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])
      ).thenAnswer(_ => throw new ApiException(404, "Workspace does not exist"))

      val mockDataRepoDAO: DataRepoDAO = new MockDataRepoDAO("mockDataRepo")

      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      // call createSnapshot on the service
      Await.result(
        snapshotService.createSnapshotByWorkspaceName(
          workspace.toWorkspaceName,
          NamedDataRepoSnapshot(DataReferenceName("foo"),
                                Option(DataReferenceDescriptionField("foo")),
                                UUID.randomUUID()
          )
        ),
        Duration.Inf
      )

      // assert that the service checked to see if the workspace exists
      verify(mockWorkspaceManagerDAO).getWorkspace(
        ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
        any[RawlsRequestContext]
      )

      // assert that the service called WSM's createWorkspace
      verify(mockWorkspaceManagerDAO).createWorkspace(
        ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
        ArgumentMatchers.eq(WorkspaceType.RawlsWorkspace),
        ArgumentMatchers.eq(None),
        any[RawlsRequestContext]
      )
    }

    "not create a WSM workspace if one already exists when creating a snapshot reference" in withMinimalTestDatabase {
      _ =>
        val mockSamDAO = defaultMockSamDao()

        val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()

        val mockDataRepoDAO: DataRepoDAO = new MockDataRepoDAO("mockDataRepo")

        val workspace = minimalTestData.workspace

        val snapshotService = SnapshotService.constructor(
          slickDataSource,
          mockSamDAO,
          mockWorkspaceManagerDAO,
          "fake-terra-data-repo-dev",
          mockDataRepoDAO
        )(testContext)

        // call createSnapshot on the service
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            workspace.toWorkspaceName,
            NamedDataRepoSnapshot(DataReferenceName("foo"),
                                  Option(DataReferenceDescriptionField("foo")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )

        // assert that the service checked to see if the workspace exists
        verify(mockWorkspaceManagerDAO).getWorkspace(
          ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
          any[RawlsRequestContext]
        )

        // assert that the service DID NOT call WSM's createWorkspace
        verify(mockWorkspaceManagerDAO, times(0)).createWorkspace(
          any[UUID],
          ArgumentMatchers.eq(WorkspaceType.RawlsWorkspace),
          any[Option[WsmPolicyInputs]],
          any[RawlsRequestContext]
        )
    }

    "set a group-constraint policy on a new WSM workspace if workspace has an auth domain" in withMinimalTestDatabase {
      _ =>
        val authDomainGroup = "authDomainGroup"
        val mockSamDAO = defaultMockSamDao()
        when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
          .thenReturn(Future.successful(Seq(authDomainGroup)))

        val expectedPolicyInputs = new WsmPolicyInputs().inputs(
          List(
            new WsmPolicyInput()
              .namespace("terra")
              .name("group-constraint")
              .additionalData(List(new WsmPolicyPair().key("group").value(authDomainGroup)).asJava)
          ).asJava
        )

        val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
        when(
          mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])
        ).thenAnswer(_ => throw new ApiException(404, "Workspace does not exist"))

        val mockDataRepoDAO: DataRepoDAO = new MockDataRepoDAO("mockDataRepo")

        val workspace = minimalTestData.workspace

        val snapshotService = SnapshotService.constructor(
          slickDataSource,
          mockSamDAO,
          mockWorkspaceManagerDAO,
          "fake-terra-data-repo-dev",
          mockDataRepoDAO
        )(testContext)

        // call createSnapshot on the service
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            workspace.toWorkspaceName,
            NamedDataRepoSnapshot(DataReferenceName("foo"),
                                  Option(DataReferenceDescriptionField("foo")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )

        // assert that the service checked to see if the workspace exists
        verify(mockWorkspaceManagerDAO).getWorkspace(
          ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
          any[RawlsRequestContext]
        )

        // assert that the service called WSM's createWorkspace
        verify(mockWorkspaceManagerDAO).createWorkspace(
          ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
          ArgumentMatchers.eq(WorkspaceType.RawlsWorkspace),
          ArgumentMatchers.eq(Some(expectedPolicyInputs)),
          any[RawlsRequestContext]
        )
    }

    "set a group-constraint policy on an existing WSM workspace if no such policy exists and the workspace has an auth domain" in withMinimalTestDatabase {
      _ =>
        val authDomainGroup = "authDomainGroup"
        val mockSamDAO = defaultMockSamDao()
        when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
          .thenReturn(Future.successful(Seq(authDomainGroup)))

        val expectedPolicyInputs = new WsmPolicyInputs().inputs(
          List(
            new WsmPolicyInput()
              .namespace("terra")
              .name("group-constraint")
              .additionalData(List(new WsmPolicyPair().key("group").value(authDomainGroup)).asJava)
          ).asJava
        )

        val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()

        val mockDataRepoDAO: DataRepoDAO = new MockDataRepoDAO("mockDataRepo")

        val workspace = minimalTestData.workspace

        val snapshotService = SnapshotService.constructor(
          slickDataSource,
          mockSamDAO,
          mockWorkspaceManagerDAO,
          "fake-terra-data-repo-dev",
          mockDataRepoDAO
        )(testContext)

        // call createSnapshot on the service
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            workspace.toWorkspaceName,
            NamedDataRepoSnapshot(DataReferenceName("foo"),
                                  Option(DataReferenceDescriptionField("foo")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )

        // assert that the service checked to see if the workspace exists
        verify(mockWorkspaceManagerDAO).getWorkspace(
          ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
          any[RawlsRequestContext]
        )

        // assert that the service DID NOT call WSM's createWorkspace
        verify(mockWorkspaceManagerDAO, times(0)).createWorkspace(
          any[UUID],
          ArgumentMatchers.eq(WorkspaceType.RawlsWorkspace),
          any[Option[WsmPolicyInputs]],
          any[RawlsRequestContext]
        )

        // assert that the group-constraint policy was backfilled on the existing WSM workspace
        verify(mockWorkspaceManagerDAO).updateWorkspacePolicies(any[UUID],
                                                                ArgumentMatchers.eq(expectedPolicyInputs),
                                                                any[RawlsRequestContext]
        )
    }

    "not set a group-constraint policy on an existing WSM workspace if there is already a group-constraint policy even if the workspace has an auth domain" in withMinimalTestDatabase {
      _ =>
        val authDomainGroup = "authDomainGroup"
        val mockSamDAO = defaultMockSamDao()
        when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
          .thenReturn(Future.successful(Seq(authDomainGroup)))

        val existingPolicyInputs = new WsmPolicyInputs().inputs(
          List(
            new WsmPolicyInput()
              .namespace("terra")
              .name("group-constraint")
              .additionalData(List(new WsmPolicyPair().key("group").value(authDomainGroup)).asJava)
              .additionalData(List(new WsmPolicyPair().key("group").value("additionalPolicyGroup")).asJava)
          ).asJava
        )

        val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
        when(mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
          new WorkspaceDescription().stage(WorkspaceStageModel.RAWLS_WORKSPACE).policies(existingPolicyInputs.getInputs)
        )

        val mockDataRepoDAO: DataRepoDAO = new MockDataRepoDAO("mockDataRepo")

        val workspace = minimalTestData.workspace

        val snapshotService = SnapshotService.constructor(
          slickDataSource,
          mockSamDAO,
          mockWorkspaceManagerDAO,
          "fake-terra-data-repo-dev",
          mockDataRepoDAO
        )(testContext)

        // call createSnapshot on the service
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            workspace.toWorkspaceName,
            NamedDataRepoSnapshot(DataReferenceName("foo"),
                                  Option(DataReferenceDescriptionField("foo")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )

        // assert that the service checked to see if the workspace exists
        verify(mockWorkspaceManagerDAO).getWorkspace(
          ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
          any[RawlsRequestContext]
        )

        // assert that the service DID NOT call WSM's createWorkspace
        verify(mockWorkspaceManagerDAO, times(0)).createWorkspace(
          any[UUID],
          ArgumentMatchers.eq(WorkspaceType.RawlsWorkspace),
          any[Option[WsmPolicyInputs]],
          any[RawlsRequestContext]
        )

        // assert that the workspace's existing policies were not updated
        verify(mockWorkspaceManagerDAO, times(0)).updateWorkspacePolicies(any[UUID],
                                                                          any[WsmPolicyInputs],
                                                                          any[RawlsRequestContext]
        )
    }

    "not create a protected snapshot reference in an unprotected workspace" in withMinimalTestDatabase { _ =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 any[String],
                                 any[SamResourceAction],
                                 any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )
      when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
        .thenReturn(Future.successful(Seq.empty))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
      when(
        mockWorkspaceManagerDAO.createDataRepoSnapshotReference(
          any[UUID],
          any[UUID],
          any[DataReferenceName],
          any[Option[DataReferenceDescriptionField]],
          any[String],
          any[CloningInstructionsEnum],
          any[RawlsRequestContext]
        )
      )
        .thenReturn(
          new DataRepoSnapshotResource()
            .metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .workspaceId(UUID.randomUUID())
                .name("foo")
                .description("")
                .cloningInstructions(CloningInstructionsEnum.NOTHING)
            )
            .attributes(new DataRepoSnapshotAttributes())
        )

      val mockDataRepoDAO = mock[DataRepoDAO](RETURNS_SMART_NULLS)
      when(
        mockDataRepoDAO.getSnapshot(
          any[UUID],
          any[OAuth2BearerToken]
        )
      )
        .thenReturn(
          new SnapshotModel()
            .id(java.util.UUID.randomUUID())
            .name("snapshot")
            .description("snapshot description")
            .source(
              java.util.List
                .of(new SnapshotSourceModel().dataset(new DatasetSummaryModel().secureMonitoringEnabled(true)))
            )
        )

      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      intercept[RawlsException] {
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            workspace.toWorkspaceName,
            NamedDataRepoSnapshot(DataReferenceName("foo"),
                                  Option(DataReferenceDescriptionField("foo")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )
      }

      verify(mockWorkspaceManagerDAO, times(0)).createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "create a protected snapshot reference in a protected workspace" in withProtectedWorkspaceTestDatabase { _ =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 any[String],
                                 any[SamResourceAction],
                                 any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )
      when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
        .thenReturn(Future.successful(Seq.empty))

      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      when(
        mockWorkspaceManagerDAO.createDataRepoSnapshotReference(
          any[UUID],
          any[UUID],
          any[DataReferenceName],
          any[Option[DataReferenceDescriptionField]],
          any[String],
          any[CloningInstructionsEnum],
          any[RawlsRequestContext]
        )
      )
        .thenReturn(
          new DataRepoSnapshotResource()
            .metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .workspaceId(UUID.randomUUID())
                .name("foo")
                .description("")
                .cloningInstructions(CloningInstructionsEnum.NOTHING)
            )
            .attributes(new DataRepoSnapshotAttributes())
        )

      val mockDataRepoDAO = defaultDataRepoDao()
      when(
        mockDataRepoDAO.getSnapshot(
          any[UUID],
          any[OAuth2BearerToken]
        )
      )
        .thenReturn(
          new SnapshotModel()
            .id(java.util.UUID.randomUUID())
            .name("snapshot")
            .description("snapshot description")
            .source(
              java.util.List
                .of(
                  new SnapshotSourceModel().dataset(
                    new DatasetSummaryModel().cloudPlatform(SnapshotCloudPlatform.GCP).secureMonitoringEnabled(true)
                  )
                )
            )
        )

      val workspace = protectedWorkspaceTestData.protectedWorkspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      Await.result(
        snapshotService.createSnapshotByWorkspaceName(
          workspace.toWorkspaceName,
          NamedDataRepoSnapshot(DataReferenceName("foo"),
                                Option(DataReferenceDescriptionField("foo")),
                                UUID.randomUUID()
          )
        ),
        Duration.Inf
      )

      verify(mockWorkspaceManagerDAO).createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "not create a snapshot reference of an Azure snapshot in a GCP workspace" in withDefaultTestDatabase {
      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      // stub an Azure snapshot
      val mockDataRepoDAO = defaultDataRepoDao()
      val azureSnapshot = new SnapshotModel()
        .id(UUID.randomUUID())
        .name("snapshotName")
        .description("snapshotDescription")
        .source(
          java.util.List.of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().cloudPlatform(SnapshotCloudPlatform.AZURE))
          )
        )

      when(mockDataRepoDAO.getSnapshot(any[UUID], any[OAuth2BearerToken]))
        .thenReturn(azureSnapshot)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        defaultMockSamDao(),
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      val thrown = intercept[RawlsException] {
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            testData.workspace.toWorkspaceName, // unless otherwise specified testData workspaces are GCP
            NamedDataRepoSnapshot(DataReferenceName("refname"),
                                  Option(DataReferenceDescriptionField("my reference description")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )
      }
      assert(
        thrown.getMessage === "Snapshots by reference are not supported for Azure datasets."
      )

      verify(mockWorkspaceManagerDAO, never()).createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "not create a snapshot reference of a GCP snapshot in an Azure workspace" in withDefaultTestDatabase {
      // stub an Azure workspace
      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      val azureWorkspaceDescription = new WorkspaceDescription()
        .stage(WorkspaceStageModel.MC_WORKSPACE)
        .azureContext(
          new AzureContext()
            .tenantId(UUID.randomUUID().toString)
            .subscriptionId(UUID.randomUUID().toString)
            .resourceGroupId(UUID.randomUUID().toString)
        )
        .policies(
          Seq(
            new WsmPolicyInput()
              .name("fakepolicy")
              .namespace("fakens")
              .addAdditionalDataItem(new WsmPolicyPair().key("dataKey").value("dataValue"))
          ).asJava
        )

      when(mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext]))
        .thenReturn(azureWorkspaceDescription)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        defaultMockSamDao(),
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        // snapshots are assumed to be GCP unless they belong to a Dataset with the Azure cloudPlatform
        new MockDataRepoDAO("mockDataRepo")
      )(testContext)

      val snapshotUuid = UUID.randomUUID()
      val snapRefName = DataReferenceName("refname")
      val snapRefDescription = Option(DataReferenceDescriptionField("my reference description"))

      val thrown = intercept[RawlsException] {
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(testData.azureWorkspace.toWorkspaceName,
                                                        NamedDataRepoSnapshot(snapRefName,
                                                                              snapRefDescription,
                                                                              snapshotUuid
                                                        )
          ),
          Duration.Inf
        )
      }

      assert(
        thrown.getMessage === "Snapshots by reference are not supported across the given cloud boundaries (snapshot: gcp, workspace: Azure)."
      )
      verify(mockWorkspaceManagerDAO, never()).createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "not create a snapshot reference of a GCP snapshot in an Azure workspace with no cloud context" in withDefaultTestDatabase {
      // stub an Azure workspace
      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      val azureWorkspaceDescription = new WorkspaceDescription()
        .stage(WorkspaceStageModel.MC_WORKSPACE)
        .policies(
          Seq(
            new WsmPolicyInput()
              .name("fakepolicy")
              .namespace("fakens")
              .addAdditionalDataItem(new WsmPolicyPair().key("dataKey").value("dataValue"))
          ).asJava
        )

      when(mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext]))
        .thenReturn(azureWorkspaceDescription)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        defaultMockSamDao(),
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        // snapshots are assumed to be GCP unless they belong to a Dataset with the Azure cloudPlatform
        new MockDataRepoDAO("mockDataRepo")
      )(testContext)

      val snapshotUuid = UUID.randomUUID()
      val snapRefName = DataReferenceName("refname")
      val snapRefDescription = Option(DataReferenceDescriptionField("my reference description"))

      val thrown = intercept[RawlsException] {
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(testData.deletingAzureWorkspace.toWorkspaceName,
                                                        NamedDataRepoSnapshot(snapRefName,
                                                                              snapRefDescription,
                                                                              snapshotUuid
                                                        )
          ),
          Duration.Inf
        )
      }

      assert(
        thrown.getMessage === "Snapshots by reference are not supported into a workspace with no cloud context (snapshot: gcp, workspace: None)."
      )
      verify(mockWorkspaceManagerDAO, never()).createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "not create a snapshot reference even if both the snapshot and workspace are Azure" in withDefaultTestDatabase {
      // stub an Azure workspace
      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      val azureWorkspaceDescription = new WorkspaceDescription()
        .stage(WorkspaceStageModel.MC_WORKSPACE)
        .azureContext(
          new AzureContext()
            .tenantId(UUID.randomUUID().toString)
            .subscriptionId(UUID.randomUUID().toString)
            .resourceGroupId(UUID.randomUUID().toString)
        )
        .policies(
          Seq(
            new WsmPolicyInput()
              .name("fakepolicy")
              .namespace("fakens")
              .addAdditionalDataItem(new WsmPolicyPair().key("dataKey").value("dataValue"))
          ).asJava
        )
      when(mockWorkspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext]))
        .thenReturn(azureWorkspaceDescription)

      // stub an Azure snapshot
      val mockDataRepoDAO = defaultDataRepoDao()
      val azureSnapshot = new SnapshotModel()
        .id(UUID.randomUUID())
        .name("snapshotName")
        .description("snapshotDescription")
        .source(
          Seq(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().cloudPlatform(SnapshotCloudPlatform.AZURE))
          ).asJava
        )
      when(mockDataRepoDAO.getSnapshot(any[UUID], any[OAuth2BearerToken]))
        .thenReturn(azureSnapshot)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        defaultMockSamDao(),
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)
      val thrown = intercept[UnsupportedPlatformException] {
        Await.result(
          snapshotService.createSnapshotByWorkspaceName(
            testData.azureWorkspace.toWorkspaceName,
            NamedDataRepoSnapshot(DataReferenceName("refname"),
                                  Option(DataReferenceDescriptionField("my reference description")),
                                  UUID.randomUUID()
            )
          ),
          Duration.Inf
        )
      }

      assert(
        thrown.getMessage === "Snapshots by reference are not supported for Azure datasets."
      )
      verify(mockWorkspaceManagerDAO, never()).createDataRepoSnapshotReference(
        any[UUID],
        any[UUID],
        any[DataReferenceName],
        any[Option[DataReferenceDescriptionField]],
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

    "remove all resources when a snapshot reference is deleted" in withMinimalTestDatabase { _ =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 any[String],
                                 any[SamResourceAction],
                                 any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      val snapshotDataReferenceId = UUID.randomUUID()
      when(mockWorkspaceManagerDAO.getDataRepoSnapshotReference(any[UUID], any[UUID], any[RawlsRequestContext]))
        .thenReturn(
          new DataRepoSnapshotResource()
            .metadata(
              new ResourceMetadata()
                .resourceId(snapshotDataReferenceId)
                .workspaceId(UUID.randomUUID())
                .name("foo")
                .description("")
                .cloningInstructions(CloningInstructionsEnum.NOTHING)
            )
            .attributes(new DataRepoSnapshotAttributes())
        )

      val mockDataRepoDAO = defaultDataRepoDao()
      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      val snapshotUUID = UUID.randomUUID()

      Await.result(snapshotService.deleteSnapshot(workspace.toWorkspaceName, snapshotUUID.toString), Duration.Inf)

      verify(mockWorkspaceManagerDAO).getDataRepoSnapshotReference(
        ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
        ArgumentMatchers.eq(snapshotUUID),
        any[RawlsRequestContext]
      )
      verify(mockWorkspaceManagerDAO).deleteDataRepoSnapshotReference(
        ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
        ArgumentMatchers.eq(snapshotUUID),
        any[RawlsRequestContext]
      )
    }

    "find one matching snapshot reference by referenced snapshotId if one page of references" in withMinimalTestDatabase {
      _ =>
        // generate a single page of ResourceDescriptions
        val resources = generateTestReferences(20)

        val snapshotService = mockSnapshotServiceForReferences(resources)

        // search for one of the snapshotIds that should be in the list
        val criteria = "00000000-0000-0000-0000-000000000012"
        val found = Await
          .result(snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                                    0,
                                                                    10,
                                                                    Option(UUID.fromString(criteria))
                  ),
                  Duration.Inf
          )
          .gcpDataRepoSnapshots

        found should have size 1
        found.head.getAttributes.getSnapshot shouldBe criteria
    }

    "find multiple matching snapshot references by referenced snapshotId if one page of references" in withMinimalTestDatabase {
      _ =>
        // generate a single page of ResourceDescriptions, but duplicate the first 10 snapshotIds
        val resources = generateTestReferences(20) ++ generateTestReferences(10)

        val snapshotService = mockSnapshotServiceForReferences(resources)

        // search for one of the snapshotIds that should be duplicated
        val criteria1 = "00000000-0000-0000-0000-000000000005"
        val found1 = Await
          .result(snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                                    0,
                                                                    10,
                                                                    Option(UUID.fromString(criteria1))
                  ),
                  Duration.Inf
          )
          .gcpDataRepoSnapshots

        found1 should have size 2
        found1.foreach { x =>
          x.getAttributes.getSnapshot shouldBe criteria1
        }

        // now search for one of the snapshotIds that should NOT be duplicated, to be sure
        val criteria2 = "00000000-0000-0000-0000-000000000015"
        val found2 = Await
          .result(snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                                    0,
                                                                    10,
                                                                    Option(UUID.fromString(criteria2))
                  ),
                  Duration.Inf
          )
          .gcpDataRepoSnapshots

        found2 should have size 1
        found2.foreach { x =>
          x.getAttributes.getSnapshot shouldBe criteria2
        }

    }

    "return an empty list of snapshot references by referenced snapshotId if one page of references" in withMinimalTestDatabase {
      _ =>
        // generate a single page of ResourceDescriptions
        val resources = generateTestReferences(20)

        val snapshotService = mockSnapshotServiceForReferences(resources)

        // search for one of the snapshotIds that should NOT be in the list
        val criteria = "00000000-0000-0000-0000-000000000099"
        val found = Await
          .result(snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                                    0,
                                                                    10,
                                                                    Option(UUID.fromString(criteria))
                  ),
                  Duration.Inf
          )
          .gcpDataRepoSnapshots

        found should have size 0
    }

    "find one matching snapshot reference by referenced snapshotId if multiple pages of references" in withMinimalTestDatabase {
      _ =>
        // 10 resources and batchsize of 3 means we should make 4 queries to WSM
        val resources = generateTestReferences(10)
        val batchSize = 3

        // yes we're spying on a mock
        val snapshotService = spy(mockSnapshotServiceForReferences(resources))

        // search for one of the snapshotIds that should be in the list
        val criteria = "00000000-0000-0000-0000-000000000006"
        val found = snapshotService
          .findBySnapshotId(minimalTestData.workspace.workspaceIdAsUUID, UUID.fromString(criteria), 0, 10, batchSize)
          .gcpDataRepoSnapshots

        found should have size 1
        found.head.getAttributes.getSnapshot shouldBe criteria

        // ensure we paged through the resources properly
        verify(snapshotService, times(4))
          .retrieveSnapshotReferences(ArgumentMatchers.eq(minimalTestData.workspace.workspaceIdAsUUID),
                                      any[Int],
                                      ArgumentMatchers.eq(batchSize)
          )
    }

    "find multiple matching snapshot references by referenced snapshotId if multiple pages of references" in withMinimalTestDatabase {
      _ =>
        // 21 resources total and batchsize of 5 means we should make 5 queries to WSM
        val resources = generateTestReferences(10) ++ // contains snapshotId "...0006"
          generateTestReferences(4) ++ // does not contain snapshotId "...0006"
          generateTestReferences(7) // contains snapshotId "...0006"
        val batchSize = 5

        // yes we're spying on a mock
        val snapshotService = spy(mockSnapshotServiceForReferences(resources))

        // search for one of the snapshotIds that should be in the list
        val criteria = "00000000-0000-0000-0000-000000000006"
        val found = snapshotService
          .findBySnapshotId(minimalTestData.workspace.workspaceIdAsUUID, UUID.fromString(criteria), 0, 10, batchSize)
          .gcpDataRepoSnapshots

        found should have size 2
        found.foreach { x =>
          x.getAttributes.getSnapshot shouldBe criteria
        }

        // ensure we paged through the resources properly
        verify(snapshotService, times(5))
          .retrieveSnapshotReferences(ArgumentMatchers.eq(minimalTestData.workspace.workspaceIdAsUUID),
                                      any[Int],
                                      ArgumentMatchers.eq(batchSize)
          )
    }

    "return an empty list of snapshot references by referenced snapshotId if multiple pages of references" in withMinimalTestDatabase {
      _ =>
        // 10 resources and batchsize of 3 means we should make 4 queries to WSM
        val resources = generateTestReferences(10)
        val batchSize = 3

        // yes we're spying on a mock
        val snapshotService = spy(mockSnapshotServiceForReferences(resources))

        // search for one of the snapshotIds that should be in the list
        val criteria = "00000000-0000-0000-0000-000000000099"
        val found = snapshotService
          .findBySnapshotId(minimalTestData.workspace.workspaceIdAsUUID, UUID.fromString(criteria), 0, 10, batchSize)
          .gcpDataRepoSnapshots

        found should have size 0

        // ensure we paged through the resources properly
        verify(snapshotService, times(4))
          .retrieveSnapshotReferences(ArgumentMatchers.eq(minimalTestData.workspace.workspaceIdAsUUID),
                                      any[Int],
                                      ArgumentMatchers.eq(batchSize)
          )
    }

    "return paginated results (first page) when listing snapshot references by referenced snapshotId" in {
      // should result in 25 references to each of four snapshotIds, for a total of 100 references
      val resources: List[ResourceDescription] = (1 to 25).toList flatMap { _ => generateTestReferences(4) }

      val pageOffset = 0
      val pageLimit = 11 // 11, not 10, just to be sure we're not relying on a default of "10" anywhere

      val snapshotService = mockSnapshotServiceForReferences(resources)

      // search for one of the snapshotIds that should be duplicated
      val criteria1 = "00000000-0000-0000-0000-000000000002"
      val found1 = Await
        .result(
          snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                            pageOffset,
                                                            pageLimit,
                                                            Option(UUID.fromString(criteria1))
          ),
          Duration.Inf
        )
        .gcpDataRepoSnapshots

      found1 should have size 11
      found1.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria1
      }
    }

    "return paginated results (second page) when listing snapshot references by referenced snapshotId" in {
      // should result in 25 references to each of four snapshotIds, for a total of 100 references
      val resources: List[ResourceDescription] = (1 to 25).toList flatMap { _ => generateTestReferences(4) }

      val snapshotService = mockSnapshotServiceForReferences(resources)

      // return the first 8 results for one of the snapshotIds that should be duplicated
      val criteria1 = "00000000-0000-0000-0000-000000000003"
      val found1 = Await
        .result(snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                                  0,
                                                                  8,
                                                                  Option(UUID.fromString(criteria1))
                ),
                Duration.Inf
        )
        .gcpDataRepoSnapshots

      found1 should have size 8
      found1.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria1
      }

      // now, perform the same search but with an offset of 4 and limit of 3
      val found2 = Await
        .result(snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                                  4,
                                                                  3,
                                                                  Option(UUID.fromString(criteria1))
                ),
                Duration.Inf
        )
        .gcpDataRepoSnapshots

      found2 should have size 3
      found2.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria1
      }

      // the second search (offset 4, limit 3) should return a slice of the first search
      found1.slice(4, 7) shouldBe found2

    }

    "return paginated results (last page) when listing snapshot references by referenced snapshotId" in {
      // should result in 25 references to each of four snapshotIds, for a total of 100 references
      val resources: List[ResourceDescription] = (1 to 25).toList flatMap { _ => generateTestReferences(4) }

      val pageOffset = 20
      val pageLimit = 11 // 11, not 10, just to be sure we're not relying on a default of "10" anywhere

      val snapshotService = mockSnapshotServiceForReferences(resources)

      // search for one of the snapshotIds that should be duplicated
      val criteria1 = "00000000-0000-0000-0000-000000000002"
      val found1 = Await
        .result(
          snapshotService.enumerateSnapshotsByWorkspaceName(minimalTestData.workspace.toWorkspaceName,
                                                            pageOffset,
                                                            pageLimit,
                                                            Option(UUID.fromString(criteria1))
          ),
          Duration.Inf
        )
        .gcpDataRepoSnapshots

      found1 should have size 5
      found1.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria1
      }
    }

    "create a new snapshot reference when called with workspaceId" in withMinimalTestDatabase { _ =>
      val mockSamDAO = defaultMockSamDao()
      val mockWorkspaceManagerDAO = defaultMockWorkspaceManagerDao()
      val mockDataRepoDAO = defaultDataRepoDao()
      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        "fake-terra-data-repo-dev",
        mockDataRepoDAO
      )(testContext)

      val snapshotUuid = UUID.randomUUID()
      val snapRefName = DataReferenceName("refname")
      val snapRefDescription = Option(DataReferenceDescriptionField("my reference description"))

      // call createSnapshot on the service
      Await.result(
        snapshotService.createSnapshotByWorkspaceId(workspace.workspaceIdAsUUID.toString,
                                                    NamedDataRepoSnapshot(snapRefName, snapRefDescription, snapshotUuid)
        ),
        Duration.Inf
      )

      // assert that the service called WSM's createDataRepoSnapshotReference
      verify(mockWorkspaceManagerDAO).createDataRepoSnapshotReference(
        ArgumentMatchers.eq(workspace.workspaceIdAsUUID),
        ArgumentMatchers.eq(snapshotUuid),
        ArgumentMatchers.eq(snapRefName),
        ArgumentMatchers.eq(snapRefDescription),
        any[String],
        any[CloningInstructionsEnum],
        any[RawlsRequestContext]
      )
    }

  }

  def generateTestReferences(numReferences: Int): List[ResourceDescription] =
    (1 to numReferences).toList.map { idx =>
      val paddedIdx = "%08d".format(idx)

      val metadata = new ResourceMetadata()
      metadata.setResourceType(ResourceType.DATA_REPO_SNAPSHOT)
      metadata.setName(s"snapshot_reference_$idx")
      metadata.setResourceId(UUID.randomUUID())
      metadata.setWorkspaceId(minimalTestData.workspace.workspaceIdAsUUID)

      val snaprefAttrs = new DataRepoSnapshotAttributes()
      snaprefAttrs.setSnapshot(s"00000000-0000-0000-0000-0000$paddedIdx")
      snaprefAttrs.setInstanceName("terra")

      val attrsUnion = new ResourceAttributesUnion()
      attrsUnion.setGcpDataRepoSnapshot(snaprefAttrs)

      val rd = new ResourceDescription()
      rd.setMetadata(metadata)
      rd.setResourceAttributes(attrsUnion)

      rd
    }

  def mockSnapshotServiceForReferences(resources: List[ResourceDescription]): SnapshotService = {
    // mock sam that always says we have permission
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                               any[String],
                               any[SamResourceAction],
                               any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))
    when(
      mockSamDAO.getUserStatus(any[RawlsRequestContext])
    ).thenReturn(
      Future.successful(
        Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
      )
    )

    // mock WorkspaceManagerDAO, don't set up any method responses yet
    val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

    when(
      mockWorkspaceManagerDAO.enumerateDataRepoSnapshotReferences(any[UUID],
                                                                  any[Int],
                                                                  any[Int],
                                                                  any[RawlsRequestContext]
      )
    )
      .thenAnswer { answer =>
        val offset = answer.getArgument[Int](1)
        val limit = answer.getArgument[Int](2)
        val resList = new ResourceList()
        resList.setResources(resources.slice(offset, offset + limit).asJava)
        resList
      }

    val mockDataRepoDAO = defaultDataRepoDao()

    SnapshotService.constructor(
      slickDataSource,
      mockSamDAO,
      mockWorkspaceManagerDAO,
      "fake-terra-data-repo-dev",
      mockDataRepoDAO
    )(testContext)

  }

}
