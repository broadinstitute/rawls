package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.FSM.Shutdown
import akka.actor._
import akka.testkit.{TestActorRef, TestKit}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{Matchers, FlatSpecLike}
import scala.collection.mutable.{Map=>MMap}
import spray.http.HttpCookie
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Try, Success}

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

  //actorSelection isn't super reliable, so this function waits for workflow actors to spin up and returns a map from workflowId -> wfActor
  private def waitForWorkflowActors(submission: Submission, subMonActor: TestActorRef[SubmissionMonitor]) = {
    val wfActors = MMap[String, ActorRef]()

    //Give the submission monitor a chance to spawn the workflow actors first
    submission.workflows.foreach { workflow =>
      awaitCond({
        val tr = Try(Await.result(system.actorSelection(subMonActor.path / workflow.workflowId).resolveOne(100 milliseconds), Duration.Inf))
        tr.foreach { actorRef =>
          wfActors(workflow.workflowId) = actorRef
        }
        tr.isSuccess
      }, 250 milliseconds)
    }

    wfActors
  }

  "SubmissionMonitor" should "mark unknown workflows" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(
      testData.wsName,
      testData.submission1.submissionId,
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
      dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
        withWorkspaceContext(testData.workspace, txn) { context =>
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Unknown)
        }
      }
    }
  }

  it should "transition to running then completed then terminate" in withDefaultTestDatabase { dataSource =>
	  val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1.submissionId, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
	  watch(monitorRef)

	  val wfActors = waitForWorkflowActors(testData.submission1, monitorRef)

	  //Tell all the workflows to move to Running
	  testData.submission1.workflows.foreach { workflow =>
		  wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Running), None), testActor)
	  }

	  awaitCond({
      dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
        withWorkspaceContext(testData.workspace, txn) { context =>
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Running)
        }
      }
	  }, 15 seconds)

	  testData.submission1.workflows.foreach { workflow =>
		  wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Succeeded), Option(Map("this.test" -> AttributeString(workflow.workflowId)))), testActor)
	  }

	  expectMsgClass(15 seconds, classOf[Terminated])

    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(true) {
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Succeeded)
        }
        testData.submission1.workflows.foreach { workflow =>
          assertResult(Some(AttributeString(workflow.workflowId))) {
            entityDAO.get(context, workflow.workflowEntity.get.entityType, workflow.workflowEntity.get.entityName, txn).get.attributes.get("test")
          }
        }
      }
    }
    unwatch(monitorRef)
  }

  it should "transition to running then aborting then aborted" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1.submissionId, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(testData.submission1, monitorRef)

    //Tell all the workflows to move to Running
    testData.submission1.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Aborting), None), testActor)
    }

    awaitCond({
      dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
        withWorkspaceContext(testData.workspace, txn) { context =>
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Aborting)
        }
      }
    }, 15 seconds)

    //Set the current status of the submission to Aborting
    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        submissionDAO.update(context, testData.submission1.copy(status = SubmissionStatuses.Aborting), txn)
      }
    }

    //Tell all of the workflows to move to Aborted
    testData.submission1.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Aborted), Option(Map("this.test" -> AttributeString(workflow.workflowId)))), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(true) {
          submissionDAO.get(context, testData.submission1.submissionId, txn).get.workflows.forall(_.status == WorkflowStatuses.Aborted)
        }
        assertResult(SubmissionStatuses.Aborted) {
          withWorkspaceContext(testData.workspace, txn) { context =>
            submissionDAO.get(context, testData.submission1.submissionId, txn).get.status
          }
        }
        testData.submission1.workflows.foreach { workflow =>
          assertResult(Some(AttributeString(workflow.workflowId))) {
            entityDAO.get(context, workflow.workflowEntity.get.entityType, workflow.workflowEntity.get.entityName, txn).get.attributes.get("test")
          }
        }
      }
    }
    unwatch(monitorRef)
  }


  it should "save workflows with error messages" in withDefaultTestDatabase { dataSource =>
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submission1.submissionId, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(testData.submission1, monitorRef)

    testData.submission1.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = WorkflowStatuses.Failed, messages = Seq(AttributeString("message"))), None), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionTerminateTest.submissionId, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(testData.submissionTerminateTest, monitorRef)

    // set each workflow to one of the terminal statuses, there should be 4 of each
    assertResult(WorkflowStatuses.terminalStatuses.size) {
      testData.submissionTerminateTest.workflows.size
    }

    testData.submissionTerminateTest.workflows.zip(WorkflowStatuses.terminalStatuses).foreach { case (workflow, status) =>
      wfActors(workflow.workflowId).tell(SubmissionMonitor.WorkflowStatusChange(workflow.copy(status = status), None), testActor)
    }
    unwatch(monitorRef)
  }

  it should "update attributes on entities" in withDefaultTestDatabase { dataSource =>
    //don't use methodConfigEntityUpdate. use a different method config where the output name is called o1 and maps to an attribute value of this.foo
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionUpdateEntity.submissionId, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(testData.submissionUpdateEntity, monitorRef)

    testData.submissionUpdateEntity.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(
        SubmissionMonitor.WorkflowStatusChange(
          workflow.copy(status = WorkflowStatuses.Succeeded, messages = Seq()),
          Some(Map("this.myAttribute" -> AttributeString("foo")))), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { wsCtx =>
        val entity = entityDAO.get(
          wsCtx,
          testData.submissionUpdateEntity.submissionEntity.get.entityType,
          testData.submissionUpdateEntity.submissionEntity.get.entityName,
          txn).get
        assertResult(AttributeString("foo"), entity.attributes) {
          entity.attributes.getOrElse("myAttribute", None)
        }
      }
    }
    unwatch(monitorRef)
  }

  it should "update attributes on workspaces" in withDefaultTestDatabase { dataSource =>
    //don't use methodConfigEntityUpdate. use a different method config where the output name is called o1 and maps to an attribute value of this.foo
    val monitorRef = TestActorRef[SubmissionMonitor](SubmissionMonitor.props(testData.wsName, testData.submissionUpdateWorkspace.submissionId, containerDAO, dataSource, 10 milliseconds, 1 second, TestActor.props()))
    watch(monitorRef)

    val wfActors = waitForWorkflowActors(testData.submissionUpdateWorkspace, monitorRef)

    testData.submissionUpdateWorkspace.workflows.foreach { workflow =>
      wfActors(workflow.workflowId).tell(
        SubmissionMonitor.WorkflowStatusChange(
          workflow.copy(status = WorkflowStatuses.Succeeded, messages = Seq()),
          Some(Map("workspace.myAttribute" -> AttributeString("foo")))), testActor)
    }

    expectMsgClass(15 seconds, classOf[Terminated])

    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      val workspace = workspaceDAO.loadContext(testData.wsName, txn).get.workspace
      assertResult(AttributeString("foo"), workspace.attributes) {
        workspace.attributes.getOrElse("myAttribute", None)
      }
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