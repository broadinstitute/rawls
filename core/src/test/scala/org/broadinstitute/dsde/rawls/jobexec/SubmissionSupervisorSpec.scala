package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.{MockExecutionServiceDAO, MockGoogleServicesDAO, MockShardedExecutionServiceCluster}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.{SaveCurrentWorkflowStatusCounts, SubmissionStarted, UpdateGlobalJobExecGauges}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.model.{SubmissionStatuses, WorkflowStatuses, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class SubmissionSupervisorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with RawlsTestUtils with MockitoTestUtils with RawlsStatsDTestUtils {

  import driver.api._

  def this() = this(ActorSystem("SubmissionSupervisorSpec"))

  val testDbName = "SubmissionSupervisorSpec"
  val submissionSupervisorActorName = "test-subsupervisorspec-submission-supervisor"

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def withSupervisor[T](trackDetailedMetrics: Boolean = true)(op: ActorRef => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO()
    val gcsDAO = new MockGoogleServicesDAO("test")
    val execCluster = MockShardedExecutionServiceCluster.fromDAO(execSvcDAO, slickDataSource)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      execCluster,
      slickDataSource,
      gcsDAO.getBucketServiceAccountCredential,
      20 minutes,
      trackDetailedSubmissionMetrics = trackDetailedMetrics,
      workbenchMetricBaseName
    ).withDispatcher("submission-monitor-dispatcher"), submissionSupervisorActorName)
    try {
      op(submissionSupervisor)
    } finally {
      submissionSupervisor ! PoisonPill
    }
  }

  "SubmissionSupervisor" should "maintain correct submission metrics for multiple active submissions" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId))
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Submitted -> 2),
          Map(SubmissionStatuses.Submitted -> 1), true)

        supervisor ! SubmissionStarted(testData.workspaceSuccessfulSubmission.toWorkspaceName, UUID.fromString(testData.submissionSuccessful1.submissionId))
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspaceSuccessfulSubmission.toWorkspaceName, UUID.fromString(testData.submissionSuccessful1.submissionId),
          Map(WorkflowStatuses.Launching -> 1, WorkflowStatuses.Failed -> 1),
          Map(SubmissionStatuses.Submitted -> 1), true)
      }
    } { capturedMetrics =>
      capturedMetrics should contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission1.submissionId, WorkflowStatuses.Submitted, 2))
      capturedMetrics should contain (expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName, testData.submissionSuccessful1.submissionId, WorkflowStatuses.Launching, 1))
      capturedMetrics should contain (expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName, testData.submissionSuccessful1.submissionId, WorkflowStatuses.Failed, 1))

      capturedMetrics should contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1))
      capturedMetrics should contain(expectedSubmissionStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName, SubmissionStatuses.Submitted, 1))
    }
  }

  it should "unregister a submission's workflow gauge when the submission completes" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        //start the submission
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId))
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission2.submissionId))

        //the first submission updates once and then completes
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Submitted -> 2),
          Map(SubmissionStatuses.Submitted -> 2), true)
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Succeeded -> 2),
          Map(SubmissionStatuses.Submitted -> 1, SubmissionStatuses.Done -> 1), false)

        //the second trundles on forever
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission2.submissionId),
          Map(WorkflowStatuses.Launching -> 2),
          Map(SubmissionStatuses.Submitted -> 1, SubmissionStatuses.Done -> 1), true)
      }
    } { capturedMetrics =>
      //Metrics for submission1 should have been unregistered, so they won't show up now.
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission1.submissionId, WorkflowStatuses.Submitted, 2))
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission1.submissionId, WorkflowStatuses.Succeeded, 2))

      //submission2 is still running so should be fine.
      capturedMetrics should contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission2.submissionId, WorkflowStatuses.Launching, 2))

      //Submission-in-workspace gauge should not have been unregistered because sub2 is still running.
      capturedMetrics should contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1))
      capturedMetrics should contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Done, 1))
    }
  }

  it should "unregister a workspace's submission gauge when the last submission in a workspace completes" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        //start the submission
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId))
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission2.submissionId))

        //both submissions immediately complete
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Succeeded -> 2),
          Map(SubmissionStatuses.Submitted -> 1, SubmissionStatuses.Done -> 1), false)
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission2.submissionId),
          Map(WorkflowStatuses.Succeeded -> 2),
          Map(SubmissionStatuses.Done -> 2), false)
      }
    } { capturedMetrics =>
      //Metrics for both submissions should have been unregistered, so they won't show up now.
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission1.submissionId, WorkflowStatuses.Succeeded, 2))
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission2.submissionId, WorkflowStatuses.Succeeded, 2))

      //Submission-in-workspace gauge should also have been unregistered it was the last submission in the workspace that was running.
      capturedMetrics shouldNot contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1))
      capturedMetrics shouldNot contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Done, 1))
      capturedMetrics shouldNot contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Done, 2))
    }
  }

  it should "keep track of global workflow and submission gauges" in withConstantTestDatabase {
    withStatsD {
      withSupervisor() { supervisor =>
        //this just looks at the database so we don't need to tell the supervisor about any submissions
        supervisor ! UpdateGlobalJobExecGauges
        Thread.sleep(1500) //give it a sec for the gauge to roll around again and retry
      }
    } { capturedMetrics =>
      capturedMetrics should contain(expectedGlobalSubmissionStatusGauge(SubmissionStatuses.Submitted, 3))
      capturedMetrics should contain(expectedGlobalWorkflowStatusGauge(WorkflowStatuses.Submitted, 6))
    }
  }

  it should "not track detailed metrics when told not to" in withDefaultTestDatabase {
    withStatsD {
      withSupervisor(trackDetailedMetrics = false) { supervisor =>
        supervisor ! SubmissionStarted(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId))
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspace.toWorkspaceName, UUID.fromString(testData.submission1.submissionId),
          Map(WorkflowStatuses.Submitted -> 2),
          Map(SubmissionStatuses.Submitted -> 1), true)

        supervisor ! SubmissionStarted(testData.workspaceSuccessfulSubmission.toWorkspaceName, UUID.fromString(testData.submissionSuccessful1.submissionId))
        supervisor ! SaveCurrentWorkflowStatusCounts(testData.workspaceSuccessfulSubmission.toWorkspaceName, UUID.fromString(testData.submissionSuccessful1.submissionId),
          Map(WorkflowStatuses.Launching -> 1, WorkflowStatuses.Failed -> 1),
          Map(SubmissionStatuses.Submitted -> 1), true)
      }
    } { capturedMetrics =>
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspace.toWorkspaceName, testData.submission1.submissionId, WorkflowStatuses.Submitted, 2))
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName, testData.submissionSuccessful1.submissionId, WorkflowStatuses.Launching, 1))
      capturedMetrics shouldNot contain (expectedWorkflowStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName, testData.submissionSuccessful1.submissionId, WorkflowStatuses.Failed, 1))

      capturedMetrics shouldNot contain(expectedSubmissionStatusGauge(testData.workspace.toWorkspaceName, SubmissionStatuses.Submitted, 1))
      capturedMetrics shouldNot contain(expectedSubmissionStatusGauge(testData.workspaceSuccessfulSubmission.toWorkspaceName, SubmissionStatuses.Submitted, 1))
    }
  }
}
