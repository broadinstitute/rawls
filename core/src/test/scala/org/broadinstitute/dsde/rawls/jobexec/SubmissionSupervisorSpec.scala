package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.credentials.FakeRawlsCredentials
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{
  HttpSamDAO,
  MockExecutionServiceDAO,
  MockGoogleServicesDAO,
  MockShardedExecutionServiceCluster
}
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.{
  CountChildren,
  RefreshGlobalJobExecGauges,
  SaveCurrentWorkflowStatusCounts,
  StartMonitorPass,
  SubmissionStarted
}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsUserEmail, SubmissionStatuses, WorkflowStatuses}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

//noinspection NameBooleanParameters,TypeAnnotation
class SubmissionSupervisorSpec
    extends TestKit(ActorSystem("SubmissionSupervisorSpec"))
    with AnyFlatSpecLike
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll
    with Eventually
    with RawlsTestUtils
    with MockitoTestUtils
    with RawlsStatsDTestUtils {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val testDbName = "SubmissionSupervisorSpec"
  val submissionSupervisorActorName = "test-subsupervisorspec-submission-supervisor"

  val mockServer = RemoteServicesMockServer()
  val gcsDAO = new MockGoogleServicesDAO("test")
  val mockSamDAO =
    new HttpSamDAO(mockServer.mockServerBaseUrl,
                   FakeRawlsCredentials(UUID.randomUUID().toString, Instant.now()),
                   1 minute,
                   15
    )
  val mockNotificationDAO: NotificationDAO = mock[NotificationDAO]

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    mockServer.stopServer
    super.afterAll()
  }

  def withSupervisor[T](trackDetailedMetrics: Boolean = true)(op: ActorRef => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO()
    val execCluster = MockShardedExecutionServiceCluster.fromDAO(execSvcDAO, slickDataSource)
    val config = SubmissionMonitorConfig(20 minutes, 30 days, trackDetailedMetrics, 20000, true, true)
    val submissionSupervisor = system.actorOf(
      SubmissionSupervisor
        .props(
          execCluster,
          new UncoordinatedDataSourceAccess(slickDataSource),
          mockSamDAO,
          gcsDAO,
          _ => mock[EntityService],
          mockNotificationDAO,
          config,
          workbenchMetricBaseName
        )
        .withDispatcher("submission-monitor-dispatcher"),
      submissionSupervisorActorName
    )
    try
      op(submissionSupervisor)
    finally
      submissionSupervisor ! PoisonPill
  }

  "SubmissionSupervisor" should "maintain correct submission metrics for multiple active submissions" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName,
                                       UUID.fromString(testData.submission1.submissionId),
                                       None,
                                       userInfo
        )
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Submitted -> 2),
          Map(SubmissionStatuses.Submitted -> 1),
          true
        )

        supervisor ! SubmissionStarted(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                       UUID.fromString(testData.submissionSuccessful1.submissionId),
                                       None,
                                       userInfo
        )
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspaceSuccessfulSubmission.toWorkspaceName,
          UUID.fromString(testData.submissionSuccessful1.submissionId),
          Map(WorkflowStatuses.Launching -> 1, WorkflowStatuses.Failed -> 1),
          Map(SubmissionStatuses.Submitted -> 1),
          true
        )
      }
    } { capturedMetrics =>
      capturedMetrics should contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission1.submissionId,
                                    WorkflowStatuses.Submitted,
                                    2
        )
      )
      capturedMetrics should contain(
        expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                    testData.submissionSuccessful1.submissionId,
                                    WorkflowStatuses.Launching,
                                    1
        )
      )
      capturedMetrics should contain(
        expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                    testData.submissionSuccessful1.submissionId,
                                    WorkflowStatuses.Failed,
                                    1
        )
      )

      capturedMetrics should contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1)
      )
      capturedMetrics should contain(
        expectedSubmissionStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                      SubmissionStatuses.Submitted,
                                      1
        )
      )
    }
  }

  it should "unregister a submission's workflow gauge when the submission completes" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        // start the submission
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName,
                                       UUID.fromString(testData.submission1.submissionId),
                                       None,
                                       userInfo
        )
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName,
                                       UUID.fromString(testData.submission2.submissionId),
                                       None,
                                       userInfo
        )

        // the first submission updates once and then completes
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Submitted -> 2),
          Map(SubmissionStatuses.Submitted -> 2),
          true
        )
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Succeeded -> 2),
          Map(SubmissionStatuses.Submitted -> 1, SubmissionStatuses.Done -> 1),
          false
        )

        // the second trundles on forever
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission2.submissionId),
          Map(WorkflowStatuses.Launching -> 2),
          Map(SubmissionStatuses.Submitted -> 1, SubmissionStatuses.Done -> 1),
          true
        )
      }
    } { capturedMetrics =>
      // Metrics for submission1 should have been unregistered, so they won't show up now.
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission1.submissionId,
                                    WorkflowStatuses.Submitted,
                                    2
        )
      )
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission1.submissionId,
                                    WorkflowStatuses.Succeeded,
                                    2
        )
      )

      // submission2 is still running so should be fine.
      capturedMetrics should contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission2.submissionId,
                                    WorkflowStatuses.Launching,
                                    2
        )
      )

      // Submission-in-workspace gauge should not have been unregistered because sub2 is still running.
      capturedMetrics should contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1)
      )
      capturedMetrics should contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Done, 1)
      )
    }
  }

  it should "unregister a workspace's submission gauge when the last submission in a workspace completes" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        // start the submission
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName,
                                       UUID.fromString(testData.submission1.submissionId),
                                       None,
                                       userInfo
        )
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName,
                                       UUID.fromString(testData.submission2.submissionId),
                                       None,
                                       userInfo
        )

        // both submissions immediately complete
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Succeeded -> 2),
          Map(SubmissionStatuses.Submitted -> 1, SubmissionStatuses.Done -> 1),
          false
        )
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission2.submissionId),
          Map(WorkflowStatuses.Succeeded -> 2),
          Map(SubmissionStatuses.Done -> 2),
          false
        )
      }
    } { capturedMetrics =>
      // Metrics for both submissions should have been unregistered, so they won't show up now.
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission1.submissionId,
                                    WorkflowStatuses.Succeeded,
                                    2
        )
      )
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission2.submissionId,
                                    WorkflowStatuses.Succeeded,
                                    2
        )
      )

      // Submission-in-workspace gauge should also have been unregistered it was the last submission in the workspace that was running.
      capturedMetrics shouldNot contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1)
      )
      capturedMetrics shouldNot contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Done, 1)
      )
      capturedMetrics shouldNot contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Done, 2)
      )
    }
  }

  it should "keep track of global workflow and submission gauges" in withCompactConstantTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        // this just looks at the database so we don't need to tell the supervisor about any submissions
        supervisor ! RefreshGlobalJobExecGauges
        Thread.sleep(1500) // give it a sec for the gauge to roll around again and retry
      }
    } { capturedMetrics =>
      capturedMetrics should contain(expectedGlobalSubmissionStatusGauge(SubmissionStatuses.Submitted, 3))
      capturedMetrics should contain(expectedGlobalWorkflowStatusGauge(WorkflowStatuses.Submitted, 6))
    }
  }

  it should "not track detailed metrics when told not to" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor(trackDetailedMetrics = false) { supervisor =>
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName,
                                       UUID.fromString(testData.submission1.submissionId),
                                       None,
                                       userInfo
        )
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspace.toWorkspaceName,
          UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Submitted -> 2),
          Map(SubmissionStatuses.Submitted -> 1),
          true
        )

        supervisor ! SubmissionStarted(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                       UUID.fromString(testData.submissionSuccessful1.submissionId),
                                       None,
                                       userInfo
        )
        supervisor ! SaveCurrentWorkflowStatusCounts(
          testData.workspaceSuccessfulSubmission.toWorkspaceName,
          UUID.fromString(testData.submissionSuccessful1.submissionId),
          Map(WorkflowStatuses.Launching -> 1, WorkflowStatuses.Failed -> 1),
          Map(SubmissionStatuses.Submitted -> 1),
          true
        )
      }
    } { capturedMetrics =>
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName,
                                    testData.submission1.submissionId,
                                    WorkflowStatuses.Submitted,
                                    2
        )
      )
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                    testData.submissionSuccessful1.submissionId,
                                    WorkflowStatuses.Launching,
                                    1
        )
      )
      capturedMetrics shouldNot contain(
        expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                    testData.submissionSuccessful1.submissionId,
                                    WorkflowStatuses.Failed,
                                    1
        )
      )

      capturedMetrics shouldNot contain(
        expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1)
      )
      capturedMetrics shouldNot contain(
        expectedSubmissionStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName,
                                      SubmissionStatuses.Submitted,
                                      1
        )
      )
    }
  }

  it should "handle getPetServiceAccountUserInfo failures gracefully and still start monitors for successful submissions" in withDefaultTestDatabase {

    // Mock the SamDAO
    val mockSamDAOWithFailure = mock[HttpSamDAO]

    // Create a new supervisor with the mocked SamDAO
    val execSvcDAO = new MockExecutionServiceDAO()
    val execCluster = MockShardedExecutionServiceCluster.fromDAO(execSvcDAO, slickDataSource)
    val config = SubmissionMonitorConfig(20 minutes, 30 days, true, 20000, true, true)

    val supervisorWithMockSam = system.actorOf(
      SubmissionSupervisor
        .props(
          execCluster,
          new UncoordinatedDataSourceAccess(slickDataSource),
          mockSamDAOWithFailure,
          gcsDAO,
          _ => mock[EntityService],
          mockNotificationDAO,
          config,
          workbenchMetricBaseName
        )
        .withDispatcher("submission-monitor-dispatcher"),
      "test-supervisor-with-mock-sam"
    )

    try {
      // set up the Sam mock to only succeed on the 2nd and 4th calls; this should result in only two
      // active submission monitors
      val successfulKey = Future.successful("fake key")
      val failedKey = Future.failed(new RuntimeException("Sam failure for unit test"))
      when(
        mockSamDAOWithFailure.getPetServiceAccountKeyForUser(
          ArgumentMatchers.any[GoogleProjectId],
          ArgumentMatchers.any[RawlsUserEmail]
        )
      )
        .thenReturn(successfulKey) // succeed for the first submission
        .thenReturn(failedKey) // fail for the second submission
        .thenReturn(successfulKey) // succeed for the third submission
        .thenReturn(failedKey, failedKey) // fail for any others

      // Send StartMonitorPass to trigger startMonitoringNewSubmissions
      supervisorWithMockSam ! StartMonitorPass

      // Wait for processing to complete
      Thread.sleep(1000)

      // Ask the supervisor how many children it has (i.e. active submission monitors).
      // If the supervisor failed to monitor any submissions, this will be zero.
      // If the supervisor correctly started monitors for all submissions in `testData`, this will be 11.
      // We expect it to be 2 because the Sam mock above only succeeds twice.
      val probe = TestProbe()
      probe.send(supervisorWithMockSam, CountChildren)
      val expectedChildCount = 2
      probe.expectMsgPF(Duration.apply("2 seconds"), s"Expected $expectedChildCount child actors") { case count: Int =>
        count shouldBe expectedChildCount
      }

    } finally
      supervisorWithMockSam ! PoisonPill
  }
}
