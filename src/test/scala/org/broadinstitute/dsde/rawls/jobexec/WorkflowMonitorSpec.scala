package org.broadinstitute.dsde.rawls.jobexec

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.{Terminated, ActorSystem}
import akka.testkit.{ImplicitSender, TestActors, TestKit, TestActorRef}
import org.joda.time.DateTime
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.HttpCookie

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.Failed
import org.broadinstitute.dsde.rawls.model._

import scala.util.Success

/**
 * Created by dvoet on 6/30/15.
 */
class WorkflowMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with OrientDbTestFixture with ImplicitSender {
  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val testDbName = "WorkflowMonitorSpec"
  val workflowDAO: GraphWorkflowDAO = new GraphWorkflowDAO(new GraphSubmissionDAO())
  val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")

  val mockCredential = new MockGoogleCredential.Builder().build()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  "WorkflowMonitor" should "throw exception for non-existent workflow" in withDefaultTestDatabase { dataSource =>
    val workflow = Workflow("id-string", WorkflowStatuses.Running, new DateTime(0), Option(AttributeEntityReference("entityType", "entity")), testData.inputResolutions)
    val monitorRef = TestActorRef[WorkflowMonitor](WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(WorkflowStatuses.Running.toString), dataSource, mockCredential)(testActor, testData.wsName, testData.submission1.submissionId, workflow))
    intercept[RawlsException] {
      monitorRef.underlyingActor.updateWorkflowStatus(ExecutionServiceStatus(workflow.workflowId, "Succeeded"))
    }
    monitorRef.stop()
  }

  it should "do nothing for unchanged state" in withDefaultTestDatabase { dataSource =>
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(testData.submission1.workflows.head.status.toString), dataSource, mockCredential)(testActor, testData.wsName, testData.submission1.submissionId, testData.submission1.workflows.head))
    expectNoMsg(1 seconds)
    system.stop(monitorRef)
  }

  it should "emit update message for changed state" in withDefaultTestDatabase { dataSource =>
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(WorkflowStatuses.Running.toString), dataSource, mockCredential)(testActor, testData.wsName, testData.submission1.submissionId, testData.submission1.workflows.head))
    expectMsg(SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = WorkflowStatuses.Running), None))
    system.stop(monitorRef)
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when ${status}" in withDefaultTestDatabase { dataSource =>
      val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(status.toString), dataSource, mockCredential)(testActor, testData.wsName, testData.submission1.submissionId, testData.submission1.workflows.head))
      watch(monitorRef)
      status match {
        case WorkflowStatuses.Failed => fishForMessage(1 second) {
          case x: SubmissionMonitor.WorkflowStatusChange if x == SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = status, messages = testData.submission1.workflows.head.messages :+ AttributeString("Workflow execution failed, check outputs for details")), None) => true
          case _ => false
        }
        case WorkflowStatuses.Succeeded => fishForMessage(10 second) {
          case x: SubmissionMonitor.WorkflowStatusChange if x == SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = status), Some(Map("output" -> AttributeString("foo")))) => true
          case x => println(x); false
        }
        case _ => fishForMessage(1 second) {
          case x: SubmissionMonitor.WorkflowStatusChange if x == SubmissionMonitor.WorkflowStatusChange(testData.submission1.workflows.head.copy(status = status), None) => true
          case _ => false
        }
      }
      fishForMessage(1 second) {
        case m: Terminated => true
        case x => println(x); false
      }
      system.stop(monitorRef)
    }
  }

  it should "fail a workflow if the method config can't be found" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val wsCtx = containerDAO.workspaceDAO.loadContext(testData.wsName, txn).get
      containerDAO.submissionDAO.save(wsCtx, testData.submission1.copy(methodConfigurationName = "DNE"), txn)
    }

    val status = WorkflowStatuses.Succeeded
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(status.toString), dataSource, mockCredential)(testActor, testData.wsName, testData.submission1.submissionId, testData.submission1.workflows.head))
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
    val monitorRef = system.actorOf(WorkflowMonitor.props(1 millisecond, containerDAO, new WorkflowTestExecutionServiceDAO(status.toString), dataSource, mockCredential)(testActor, testData.wsName, testData.submission2.submissionId, testData.submission2.workflows.head))
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
  override def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo) = Future.successful(ExecutionServiceStatus("test_id", workflowStatus))
  override def validateWorkflow(wdl: String, inputs: String, userInfo: UserInfo) = Future.successful(ExecutionServiceValidation(true, "No errors"))

  override def outputs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceOutputs(id, Map("o1" -> AttributeString("foo"))))
  override def logs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceLogs(id, Map("task1" -> Seq(ExecutionServiceCallLogs(stdout = "foo", stderr = "bar")))))

  override def status(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceStatus(id, workflowStatus))
  override def abort(id: String, userInfo: UserInfo) = Future.successful(Success(ExecutionServiceStatus(id, workflowStatus)))
  override def callLevelMetadata(id: String, userInfo: UserInfo) = Future.successful(null)
}