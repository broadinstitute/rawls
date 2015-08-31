package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.{Terminated, ActorSystem}
import akka.testkit.{ImplicitSender, TestActors, TestKit, TestActorRef}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.Failed
import org.broadinstitute.dsde.rawls.model._
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
    val workflow = Workflow("id-string", WorkflowStatuses.Running, new DateTime(0), AttributeEntityReference("entityType", "entity"))
    val monitorRef = TestActorRef[WorkflowMonitor](WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(WorkflowStatuses.Running.toString), dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.wsName, testData.submission1, workflow))
    intercept[RawlsException] {
      monitorRef.underlyingActor.checkWorkflowStatus()
    }
    monitorRef.stop()
  }

  it should "do nothing for unchanged state" in withDefaultTestDatabase { dataSource =>
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(testData.submission1.workflows.head.status.toString), dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.wsName, testData.submission1, testData.submission1.workflows.head))
    expectNoMsg(1 seconds)
    system.stop(monitorRef)
  }

  it should "emit update message for changed state" in withDefaultTestDatabase { dataSource =>
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(WorkflowStatuses.Running.toString), dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.wsName, testData.submission1, testData.submission1.workflows.head))
    expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = WorkflowStatuses.Running), None))
    system.stop(monitorRef)
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when ${status}" in withDefaultTestDatabase { dataSource =>
      val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(status.toString), dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.wsName, testData.submission1, testData.submission1.workflows.head))
      watch(monitorRef)
      status match {
        case WorkflowStatuses.Failed => expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = status, messages = testData.submission1.workflows.head.messages :+ AttributeString("Workflow execution failed, check outputs for details")), None))
        case WorkflowStatuses.Succeeded => expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = status), Some(Map("output" -> AttributeString("foo")))))
        case _ => expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = status), None))
      }
      fishForMessage(1 second) {
        case m: Terminated => true
        case x => println(x); false
      }
      system.stop(monitorRef)
    }
  }

  it should "fail a workflow if the method config can't be found" in withDefaultTestDatabase { dataSource =>
    val status = WorkflowStatuses.Succeeded
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(status.toString), dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.wsName, testData.submission1.copy(methodConfigurationName = "DNE"), testData.submission1.workflows.head))
    watch(monitorRef)
    expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = WorkflowStatuses.Failed, messages = Seq(AttributeString(s"Could not find method config ${testData.submission1.methodConfigurationNamespace}/DNE, was it deleted?"))), None))
    fishForMessage(1 second) {
      case m: Terminated => true
      case x => println(x); false
    }
    system.stop(monitorRef)
  }

  it should "fail a workflow if outputs can't be found" in withDefaultTestDatabase { dataSource =>
    val status = WorkflowStatuses.Succeeded
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(status.toString), dataSource, HttpCookie("iPlanetDirectoryPro", "test_token"))(testActor, testData.wsName, testData.submission2, testData.submission2.workflows.head))
    watch(monitorRef)
    fishForMessage(1 second) {
      case m: SubmissionMonitor.WorkflowStatusChange =>
        assertResult(WorkflowStatuses.Failed) { m.workflow.status }
        assertResult(Set(AttributeString("output named out1 does not exist"), AttributeString("output named out2 does not exist"))) { m.workflow.messages.toSet }
        true
      case _ => false
    }
    fishForMessage(1 second) {
      case m: Terminated => true
      case x => println(x); false
    }
    system.stop(monitorRef)
  }

}

class WorkflowTestExecutionServiceDAO(workflowStatus: String) extends ExecutionServiceDAO {
  override def submitWorkflow(wdl: String, inputs: String, authCookie: HttpCookie): ExecutionServiceStatus = ExecutionServiceStatus("test_id", workflowStatus)

  override def outputs(id: String, authCookie: HttpCookie): ExecutionServiceOutputs = ExecutionServiceOutputs(id, Map("o1" -> AttributeString("foo")))
  override def logs(id: String, authCookie: HttpCookie): ExecutionServiceLogs = ExecutionServiceLogs(id, Map("task1" -> Map("wf.t1.foo" -> "foo", "wf.t1.bar" -> "bar")))

  override def status(id: String, authCookie: HttpCookie): ExecutionServiceStatus = ExecutionServiceStatus(id, workflowStatus)
  override def abort(id: String, authCookie: HttpCookie): ExecutionServiceStatus = ExecutionServiceStatus(id, workflowStatus)
}