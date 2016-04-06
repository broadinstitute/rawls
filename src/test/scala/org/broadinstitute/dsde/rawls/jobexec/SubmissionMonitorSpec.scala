package org.broadinstitute.dsde.rawls.jobexec

import akka.actor._
import akka.testkit.{TestActorRef, TestKit}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{Matchers, FlatSpecLike}
import scala.collection.mutable.{Map=>MMap}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.scalatest.BeforeAndAfterAll

/**
 * Created by dvoet on 7/1/15.
 */
class SubmissionMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll {
  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val testDbName = "SubmissionMonitorSpec"

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  //actorSelection isn't super reliable, so this function waits for workflow actors to spin up and returns a map from workflowId -> wfActor
  private def waitForWorkflowActors(workspaceContext: SlickWorkspaceContext, submission: Submission, subMonActor: TestActorRef[SubmissionMonitor]) = {
    val wfActors = MMap[String, ActorRef]()

    val workflowsWithIds = runAndWait(workflowQuery.getWithWorkflowIds(workspaceContext, submission.submissionId))

    //Give the submission monitor a chance to spawn the workflow actors first
    workflowsWithIds.foreach { case (id, workflow) =>
      awaitCond({
        val tr = Try(Await.result(system.actorSelection(subMonActor.path / id.toString).resolveOne(250 milliseconds), Duration.Inf))
        tr.foreach { actorRef =>
          wfActors(workflow.workflowId) = actorRef
        }
        tr.isSuccess
      }, 1 seconds)
    }

    wfActors
  }

  "SubmissionMonitor" should "mark unknown workflows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(
      testData.wsName,
      testData.submission1.submissionId,
      dataSource,
      10 milliseconds,
      1 second,
      TestActor.props()
    ))
    watch(monitorRef)
    waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submission1, monitorRef)
    system.actorSelection(monitorRef.path / "*").tell(PoisonPill, testActor)
    expectMsgClass(15 seconds, classOf[Terminated])
    assertResult(true) {
      val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))
      submission.get.workflows.forall(_.status == WorkflowStatuses.Unknown)
    }
  }

  // TODO: deadlock issues in MySQL
  ignore should "transition to running then completed then terminate" in withDefaultTestDatabase { dataSource: SlickDataSource =>
	  val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1.submissionId, dataSource, 10 milliseconds, 1 second, TestActor.props()))
	  watch(monitorRef)

	  val wfActors = waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submission1, monitorRef)

	  //Tell all the workflows to move to Running
	  testData.submission1.workflows.foreach { workflow =>
		  wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Running), None), testActor)
	  }

	  awaitCond({
	    val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))
	    submission.get.workflows.forall(_.status == WorkflowStatuses.Running)
	  }, 15 seconds)

	  testData.submission1.workflows.foreach { workflow =>
		  wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Succeeded), Option(Map("this.test" -> AttributeString(workflow.workflowId)))), testActor)
	  }

	  expectMsgClass(15 seconds, classOf[Terminated])

      assertResult(true) {
  	    val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))
  	    submission.get.workflows.forall(_.status == WorkflowStatuses.Succeeded)
      }
      testData.submission1.workflows.foreach { workflow =>
        assertResult(Some(AttributeString(workflow.workflowId))) {
          val entity = runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), workflow.workflowEntity.get.entityType, workflow.workflowEntity.get.entityName))
          entity.get.attributes.get("test")
        }
      }
    unwatch(monitorRef)
  }

  // TODO: deadlock issues in MySQL
  ignore should "transition to running then aborting then aborted" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1.submissionId, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submission1, monitorRef)

    //Tell all the workflows to move to Running
    testData.submission1.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Aborting), None), testActor)
    }

	  awaitCond({
	    val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))
	    submission.get.workflows.forall(_.status == WorkflowStatuses.Aborting)
	  }, 15 seconds)

    //Set the current status of the submission to Aborting
	  runAndWait(submissionQuery.updateStatus(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId, SubmissionStatuses.Aborting))

    //Tell all of the workflows to move to Aborted
    testData.submission1.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Aborted), Option(Map("this.test" -> AttributeString(workflow.workflowId)))), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))
    assertResult(true) {
	    submission.get.workflows.forall(_.status == WorkflowStatuses.Aborted)
    }
    assertResult(SubmissionStatuses.Aborted) {
	    submission.get.status
    }
    testData.submission1.workflows.foreach { workflow =>
      assertResult(Some(AttributeString(workflow.workflowId))) {
  	    val entity = runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), workflow.workflowEntity.get.entityType, workflow.workflowEntity.get.entityName))
  	    entity.get.attributes.get("test")
      }
    }
    unwatch(monitorRef)
  }

  // TODO: deadlock issues in MySQL
  ignore should "save workflows with error messages" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1.submissionId, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submission1, monitorRef)

    testData.submission1.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Failed, messages = Seq(AttributeString("message"))), None), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))
    submission.get.workflows.foreach { workflow =>
      assertResult(WorkflowStatuses.Failed) {
        workflow.status
      }
      assertResult(Seq(AttributeString("message")), workflow) {
        workflow.messages
      }
    }
    submission.get.workflows.forall(_.status == WorkflowStatuses.Failed)
  }

  it should "terminate when all workflows are done" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionTerminateTest.submissionId, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submissionTerminateTest, monitorRef)

    // set each workflow to one of the terminal statuses, there should be 4 of each
    assertResult(WorkflowStatuses.terminalStatuses.size) {
      testData.submissionTerminateTest.workflows.size
    }

    testData.submissionTerminateTest.workflows.zip(WorkflowStatuses.terminalStatuses).foreach { case (workflow, status) =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = status), None), testActor)
    }
    unwatch(monitorRef)
  }

  it should "update attributes on entities" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    //don't use methodConfigEntityUpdate. use a different method config where the output name is called o1 and maps to an attribute value of this.foo
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionUpdateEntity.submissionId, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submissionUpdateEntity, monitorRef)

    testData.submissionUpdateEntity.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(
        SubmissionMonitor.WorkflowStatusChange(
          workflow.copy(status = WorkflowStatuses.Succeeded, messages = Seq()),
          Some(Map("this.myAttribute" -> AttributeString("foo")))), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    val entity = runAndWait(entityQuery.get(
      SlickWorkspaceContext(testData.workspace),
      testData.submissionUpdateEntity.submissionEntity.get.entityType,
      testData.submissionUpdateEntity.submissionEntity.get.entityName)).get
      
    assertResult(AttributeString("foo"), entity.attributes) {
      entity.attributes.getOrElse("myAttribute", None)
    }
    unwatch(monitorRef)
  }

  it should "update attributes on workspaces" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    //don't use methodConfigEntityUpdate. use a different method config where the output name is called o1 and maps to an attribute value of this.foo
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionUpdateWorkspace.submissionId, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(SlickWorkspaceContext(testData.workspace), testData.submissionUpdateWorkspace, monitorRef)

    testData.submissionUpdateWorkspace.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(
        SubmissionMonitor.WorkflowStatusChange(
          workflow.copy(status = WorkflowStatuses.Succeeded, messages = Seq()),
          Some(Map("workspace.myAttribute" -> AttributeString("foo")))), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    val workspace = runAndWait(workspaceQuery.findById(testData.workspace.workspaceId)).get
    assertResult(AttributeString("foo"), workspace.attributes) {
      workspace.attributes.getOrElse("myAttribute", None)
    }
    unwatch(monitorRef)
  }
}

object TestActor {
  def props()(parent: ActorRef, workspaceName: WorkspaceName, submissionId: String, workflow: Workflow) = {
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