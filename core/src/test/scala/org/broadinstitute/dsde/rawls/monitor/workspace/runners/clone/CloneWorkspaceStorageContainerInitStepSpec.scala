package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.{
  AccessScope,
  CloneControlledAzureStorageContainerResult,
  ControlledResourceMetadata,
  JobReport,
  ResourceDescription,
  ResourceList,
  ResourceMetadata
}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CloningFailed
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail}
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceRepository}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, doReturn, spy, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.SeqHasAsJava

class CloneWorkspaceStorageContainerInitStepSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with Matchers
    with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userEmail: String = "user@email.com"
  val sourceWorkspaceId: UUID = UUID.randomUUID()
  val destWorkspaceId: UUID = UUID.randomUUID()

  behavior of "retrieving the source workspace storage container"

  it should "retrieve the default storage container from the source workspace with a single shared access container" in {
    val ctx = mock[RawlsRequestContext]
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      destWorkspaceId,
      RawlsUserEmail(userEmail),
      Some(Map()),
      JobType.CloneWorkspaceContainerInit
    )
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)
    val matchingContainer = new ResourceDescription()
      .metadata(
        new ResourceMetadata()
          .name(expectedContainerName)
          .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
      )
    val resourceList = new ResourceList().resources(
      List(
        matchingContainer,
        // both of these should be filtered out, either because the name doesn't match or because the access is invalid
        new ResourceDescription()
          .metadata(
            new ResourceMetadata()
              .name("some other name")
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          ),
        new ResourceDescription()
          .metadata(
            new ResourceMetadata()
              .name(expectedContainerName)
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.PRIVATE_ACCESS))
          )
      ).asJava
    )
    when(workspaceManagerDAO.enumerateStorageContainers(sourceWorkspaceId, 0, 200, ctx)).thenReturn(resourceList)
    val step = new CloneWorkspaceStorageContainerInitStep(
      workspaceManagerDAO,
      mock[WorkspaceRepository],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      destWorkspaceId,
      monitorRecord
    )

    step.findSourceWorkspaceStorageContainer(sourceWorkspaceId, expectedContainerName, ctx) shouldBe Some(
      matchingContainer
    )
  }

  it should "return None if no matching storage container is found" in {
    val ctx = mock[RawlsRequestContext]
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      destWorkspaceId,
      RawlsUserEmail(userEmail),
      Some(Map()),
      JobType.CloneWorkspaceContainerInit
    )
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)
    val resourceList = new ResourceList().resources(
      List(
        // both of these should be filtered out, either because the name doesn't match or because the access is invalid
        new ResourceDescription()
          .metadata(
            new ResourceMetadata()
              .name("some other name")
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          ),
        new ResourceDescription()
          .metadata(
            new ResourceMetadata()
              .name(expectedContainerName)
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.PRIVATE_ACCESS))
          )
      ).asJava
    )
    when(workspaceManagerDAO.enumerateStorageContainers(sourceWorkspaceId, 0, 200, ctx)).thenReturn(resourceList)
    val step = new CloneWorkspaceStorageContainerInitStep(
      workspaceManagerDAO,
      mock[WorkspaceRepository],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      destWorkspaceId,
      monitorRecord
    )

    step.findSourceWorkspaceStorageContainer(sourceWorkspaceId, expectedContainerName, ctx) shouldBe None
  }

  behavior of "the clone storage container init step"

  it should "report failure and return complete when no matching storage container is found" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      destWorkspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.SOURCE_WORKSPACE_KEY -> sourceWorkspaceId.toString)),
      JobType.CloneWorkspaceContainerInit
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.setFailedState(
        ArgumentMatchers.eq(destWorkspaceId),
        ArgumentMatchers.eq(CloningFailed),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future(1))
    val step = spy(
      new CloneWorkspaceStorageContainerInitStep(
        mock[WorkspaceManagerDAO],
        workspaceRepository,
        mock[WorkspaceManagerResourceMonitorRecordDao],
        destWorkspaceId,
        monitorRecord
      )
    )
    doReturn(None)
      .when(step)
      .findSourceWorkspaceStorageContainer(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())

    whenReady(step.runStep(mock[RawlsRequestContext]))(_ shouldBe Complete)
    verify(step)
      .findSourceWorkspaceStorageContainer(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())
  }

  it should "complete the job when no container clone result is returned" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      destWorkspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.SOURCE_WORKSPACE_KEY -> sourceWorkspaceId.toString)),
      JobType.CloneWorkspaceContainerInit
    )
    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)

    val container = new ResourceDescription()
      .metadata(
        new ResourceMetadata()
          .name(expectedContainerName)
          .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
      )
    val ctx = mock[RawlsRequestContext]
    val step = spy(
      new CloneWorkspaceStorageContainerInitStep(
        mock[WorkspaceManagerDAO],
        mock[WorkspaceRepository],
        mock[WorkspaceManagerResourceMonitorRecordDao],
        destWorkspaceId,
        monitorRecord
      )
    )

    doReturn(Some(container))
      .when(step)
      .findSourceWorkspaceStorageContainer(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())
    doReturn(None).when(step).cloneWorkspaceStorageContainer(sourceWorkspaceId, destWorkspaceId, container, None, ctx)
    whenReady(step.runStep(ctx)) {
      _ shouldBe Complete
    }
  }

  it should "complete the job and schedule the next job when the call is successful" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      destWorkspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.SOURCE_WORKSPACE_KEY -> sourceWorkspaceId.toString)),
      JobType.CloneWorkspaceContainerInit
    )
    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)

    val container = new ResourceDescription()
      .metadata(
        new ResourceMetadata()
          .name(expectedContainerName)
          .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
      )
    val ctx = mock[RawlsRequestContext]
    val cloneStorageContainerJobId = UUID.randomUUID()
    val recordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val record: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      record.workspaceId shouldBe Some(destWorkspaceId)
      record.jobType shouldBe JobType.CloneWorkspaceAwaitContainerResult
      record.jobControlId shouldBe cloneStorageContainerJobId
      Future.successful()
    }.when(recordDao).create(ArgumentMatchers.any())
    val step = spy(
      new CloneWorkspaceStorageContainerInitStep(
        mock[WorkspaceManagerDAO],
        mock[WorkspaceRepository],
        recordDao,
        destWorkspaceId,
        monitorRecord
      )
    )
    doReturn(Some(container))
      .when(step)
      .findSourceWorkspaceStorageContainer(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())
    doReturn(
      Some(
        new CloneControlledAzureStorageContainerResult()
          .jobReport(new JobReport().id(cloneStorageContainerJobId.toString))
      )
    ).when(step).cloneWorkspaceStorageContainer(sourceWorkspaceId, destWorkspaceId, container, None, ctx)

    whenReady(step.runStep(ctx)) {
      _ shouldBe Complete
    }

    verify(recordDao).create(ArgumentMatchers.any())
  }

}
