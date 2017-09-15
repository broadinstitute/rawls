package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.{MockExecutionServiceDAO, MockShardedExecutionServiceCluster}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SaveCurrentWorkflowStatusCounts
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

  def withSupervisor[T](op: ActorRef => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO()
    val execCluster = MockShardedExecutionServiceCluster.fromDAO(execSvcDAO, slickDataSource)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      execCluster,
      slickDataSource,
      20 minutes,
      workbenchMetricBaseName
    ).withDispatcher("submission-monitor-dispatcher"), submissionSupervisorActorName)
    try {
      op(submissionSupervisor)
    } finally {
      submissionSupervisor ! PoisonPill
    }
  }

  "SubmissionSupervisor" should "maintain correct submission metrics for multiple active submissions" in withConstantTestDatabase {
    withSupervisor { supervisor =>
      val submissionId = UUID.randomUUID()
      withStatsD {
        supervisor ! SaveCurrentWorkflowStatusCounts(WorkspaceName("phooey", "blooey"), submissionId, Map(WorkflowStatuses.Submitted -> 2), Map(SubmissionStatuses.Submitted -> 1), true)
      } { capturedMetrics =>
        println(capturedMetrics)
        //capturedMetrics should contain (expectedWorkflowStatusMetric(WorkspaceName("phooey", "blooey"), submissionId.toString, WorkflowStatuses.Submitted, 2))
      }
    }
    /*
    TODO:
    * send SaveCurrentWorkflowStatusCounts once, check the global and active and sub workflow counts are correct
    * send another one for a different ws/subid, check again
    * NOTE: SaveCurrentWorkflowStatusCounts doesn't update the global gauges, you need UpdateGlobalJobExecGauges for that
     */
  }

  it should "unregister a submission's workflow gauge when the submission completes" in withConstantTestDatabase {
    //todo: test that the workflow gauge gets unregistered and also that the global submission stats are fine
  }

  it should "unregister a workspace's submission gauge when the last submission in a workspace completes" in withConstantTestDatabase {
    //todo: see above
  }
}
