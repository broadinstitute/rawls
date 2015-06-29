package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.{Terminated, ActorSystem}
import akka.testkit.{ImplicitSender, TestActors, TestKit, TestActorRef}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{ExecutionServiceDAO, GraphWorkflowDAO}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.{WorkflowStatuses, ExecutionServiceStatus, Workflow}
import org.joda.time.DateTime
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.HttpCookie
import scala.concurrent.duration._

/**
 * Created by dvoet on 6/30/15.
 */
class WorkflowMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with OrientDbTestFixture with ImplicitSender {
  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val testDbName = "WorkflowMonitorSpec"
  val workflowDAO: GraphWorkflowDAO = new GraphWorkflowDAO

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  "WorkflowMonitor" should "throw exception for non-existent workflow" in withDefaultTestDatabase { dataSource =>
    val workflow = Workflow("wns", "wn", "id-string", WorkflowStatuses.Running, new DateTime(0), "entity")
    val monitorRef = TestActorRef[WorkflowMonitor](WorkflowMonitor.props(1 millisecond, new WorkflowTestExecutionServiceDAO(WorkflowStatuses.Running.toString), workflowDAO, dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, workflow))
    intercept[RawlsException] {
      monitorRef.underlyingActor.checkWorkflowStatus()
    }
    monitorRef.stop()
  }

  it should "do nothing for unchanged state" in withDefaultTestDatabase { dataSource =>
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, new WorkflowTestExecutionServiceDAO(testData.submission.workflow.head.status.toString), workflowDAO, dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.submission.workflow.head))
    expectNoMsg(1 seconds)
    system.stop(monitorRef)
  }

  it should "emit update message for changed state" in withDefaultTestDatabase { dataSource =>
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, new WorkflowTestExecutionServiceDAO(WorkflowStatuses.Running.toString), workflowDAO, dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.submission.workflow.head))
    expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission.workflow.head.copy(status = WorkflowStatuses.Running)))
    system.stop(monitorRef)
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when ${status}" in withDefaultTestDatabase { dataSource =>
      val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, new WorkflowTestExecutionServiceDAO(status.toString), workflowDAO, dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.submission.workflow.head))
      watch(monitorRef)
      expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission.workflow.head.copy(status = status)))
      fishForMessage(1 second) {
        case m: Terminated => true
        case x => println(x); false
      }
      system.stop(monitorRef)
    }
  }

}

class WorkflowTestExecutionServiceDAO(workflowStatus: String) extends ExecutionServiceDAO {
  override def submitWorkflow(wdl: String, inputs: String, authCookie: HttpCookie): ExecutionServiceStatus = ExecutionServiceStatus("test_id", workflowStatus)
  override def status(id: String, authCookie: HttpCookie): ExecutionServiceStatus = ExecutionServiceStatus(id, workflowStatus)
}