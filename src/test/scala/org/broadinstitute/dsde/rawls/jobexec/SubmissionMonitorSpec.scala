package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.FSM.Shutdown
import akka.actor._
import akka.testkit.{TestActorRef, TestKit}
import org.broadinstitute.dsde.rawls.dataaccess.{ExecutionServiceDAO, GraphWorkflowDAO, GraphSubmissionDAO}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.{Workflow, WorkflowStatuses, ExecutionServiceStatus}
import org.scalatest.{Matchers, FlatSpecLike}
import spray.http.HttpCookie
import scala.concurrent.duration._

/**
 * Created by dvoet on 7/1/15.
 */
class SubmissionMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with OrientDbTestFixture {
  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val testDbName = "SubmissionMonitorSpec"

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  "SubmissionMonitor" should "mark unknown workflows" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.submission, submissionDAO, new GraphWorkflowDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)
    system.actorSelection(monitorRef.path / "*").tell(PoisonPill, testActor)
    expectMsgClass(10 seconds, classOf[Terminated])
    assertResult(true) { dataSource inTransaction { txn =>
      submissionDAO.get(testData.submission.workspaceNamespace, testData.submission.workspaceName, testData.submission.id, txn).get.workflows.forall(_.status == WorkflowStatuses.Unknown)
    }}
  }

  it should "transition to running then completed then terminate" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.submission, submissionDAO, new GraphWorkflowDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    testData.submission.workflows.foreach { workflow =>
      system.actorSelection(monitorRef.path / workflow.id).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Running)), testActor)
    }

    awaitCond({
      dataSource inTransaction { txn =>
        submissionDAO.get(testData.submission.workspaceNamespace, testData.submission.workspaceName, testData.submission.id, txn).get.workflows.forall(_.status == WorkflowStatuses.Running)
      }
    }, 10 seconds)

    testData.submission.workflows.foreach { workflow =>
      system.actorSelection(monitorRef.path / workflow.id).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Succeeded)), testActor)
    }

    expectMsgClass(10 seconds, classOf[Terminated])

    assertResult(true) { dataSource inTransaction { txn =>
      submissionDAO.get(testData.submission.workspaceNamespace, testData.submission.workspaceName, testData.submission.id, txn).get.workflows.forall(_.status == WorkflowStatuses.Succeeded)
    }}
  }

  it should "terminate when all workflows are done" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.submission, submissionDAO, new GraphWorkflowDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    // set each workflow to one of the terminal statuses, there should be 3 of each
    assertResult(WorkflowStatuses.terminalStatuses.size) {
      testData.submission.workflows.size
    }

    testData.submission.workflows.zip(WorkflowStatuses.terminalStatuses).foreach { case (workflow, status) =>
      system.actorSelection(monitorRef.path / workflow.id).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = status)), testActor)
    }
  }

}

object TestActor {
  def props()(parent: ActorRef, workflow: Workflow) = {
    Props(new TestActor(parent))
  }
}

/**
 * test actor that sends any message it receives to parent
 * @param parent
 */
class TestActor(parent: ActorRef) extends Actor {
  override def receive = {
    case x: Any => parent ! x
  }
}