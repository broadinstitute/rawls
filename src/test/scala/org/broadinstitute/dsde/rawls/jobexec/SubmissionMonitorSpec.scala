package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.FSM.Shutdown
import akka.actor._
import akka.testkit.{TestActorRef, TestKit}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
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
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(
      testData.wsName,
      testData.submission1,
      containerDAO,
      dataSource,
      10 milliseconds,
      1 second,
      TestActor.props()
    ))
    watch(monitorRef)
    system.actorSelection(monitorRef.path / "*").tell(PoisonPill, testActor)
    expectMsgClass(15 seconds, classOf[Terminated])
    assertResult(true) {
      dataSource inTransaction { txn =>
        withWorkspaceContext(testData.workspace, txn) { context =>
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Unknown)
        }
      }
    }
  }

  it should "transition to running then completed then terminate" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    println("moving to running")

    testData.submission1.workflows.foreach { workflow =>
      system.actorSelection(monitorRef.path / workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Running), None), testActor)
    }

    awaitCond({
      dataSource inTransaction { txn =>
        withWorkspaceContext(testData.workspace, txn) { context =>
          println("checking...")
          val wfs = submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows
          wfs.map( wf => println( s"${wf.workflowId} ${wf.status}" ) )
          wfs.forall(_.status == WorkflowStatuses.Running)
        }
      }
    }, 1500 seconds, 1 seconds)

    println("moving to succeeded")

    testData.submission1.workflows.foreach { workflow =>
      system.actorSelection(monitorRef.path / workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Succeeded), Option(Map("test" -> AttributeString(workflow.workflowId)))), testActor)
    }

    println("terminated yet?")

    expectMsgClass(1500 seconds, classOf[Terminated])

    println("yup")

    dataSource inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(true) {
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Succeeded)
        }
        testData.submission1.workflows.foreach { workflow =>
          assertResult(Some(AttributeString(workflow.workflowId))) {
            entityDAO.get(context, workflow.workflowEntity.entityType, workflow.workflowEntity.entityName, txn).get.attributes.get("test")
          }
        }
      }
    }
  }

  it should "save workflows with error messages" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    testData.submission1.workflows.foreach { workflow =>
      system.actorSelection(monitorRef.path / workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Failed, messages = Seq(AttributeString("message"))), None), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    dataSource inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.foreach { workflow =>
          assertResult(WorkflowStatuses.Failed) {
            workflow.status
          }
          assertResult(Seq(AttributeString("message"))) {
            workflow.messages
          }
        }
        submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Failed)
      }
    }
  }

  it should "terminate when all workflows are done" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionTerminateTest, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    // set each workflow to one of the terminal statuses, there should be 4 of each
    assertResult(WorkflowStatuses.terminalStatuses.size) {
      testData.submissionTerminateTest.workflows.size
    }

    testData.submissionTerminateTest.workflows.zip(WorkflowStatuses.terminalStatuses).foreach { case (workflow, status) =>
      system.actorSelection(monitorRef.path / workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = status), None), testActor)
    }
  }

}

object TestActor {
  def props()(parent: ActorRef, workspaceName: WorkspaceName, submission: Submission, workflow: Workflow) = {
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