package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.testkit.{TestActorRef, TestKit}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential.Builder
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkflowRecord}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor.{ExecutionServiceStatusResponse, StatusCheckComplete}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils}
import org.broadinstitute.dsde.rawls.expressions.{BoundOutputExpression, OutputExpression}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.OutputType
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

/**
 * Created by dvoet on 7/1/15.
 */
class SubmissionMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with RawlsTestUtils with MockitoTestUtils with RawlsStatsDTestUtils {
  import driver.api._

  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val testDbName = "SubmissionMonitorSpec"

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private def await[T](f: Future[T]): T = Await.result(f, 5 minutes)

  "SubmissionMonitor" should "queryExecutionServiceForStatus success" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assertResult(ignoreStatusLastChangedDate(ExecutionServiceStatusResponse(
      workflowsRecs.map { workflowRec => scala.util.Success(Option((workflowRec.copy(status = WorkflowStatuses.Succeeded.toString), Some(ExecutionServiceOutputs(workflowRec.externalId.get, Map("o1" -> Left(AttributeString("foo")))))))) }
    ))) {
      ignoreStatusLastChangedDate(await(monitor.queryExecutionServiceForStatus()))
    }
  }

  it should "queryExecutionServiceForStatus submitted" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Submitted.toString))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assertResult(ignoreStatusLastChangedDate(ExecutionServiceStatusResponse(
      workflowsRecs.map { workflowRec => scala.util.Success(None) }
    ))) {
      ignoreStatusLastChangedDate(await(monitor.queryExecutionServiceForStatus()))
    }
  }

  (WorkflowStatuses.runningStatuses.toSet ++ WorkflowStatuses.terminalStatuses -- Set(WorkflowStatuses.Succeeded, WorkflowStatuses.Submitted)).foreach { status =>
    it should s"queryExecutionServiceForStatus $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))

      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

      assertResult(ignoreStatusLastChangedDate(ExecutionServiceStatusResponse(
        workflowsRecs.map { workflowRec => scala.util.Success(Option((workflowRec.copy(status = status.toString), None))) }
      ))) {
        ignoreStatusLastChangedDate(await(monitor.queryExecutionServiceForStatus()))
      }
    }
  }

  val abortableStatuses = Seq(WorkflowStatuses.Queued) ++ WorkflowStatuses.runningStatuses

  abortableStatuses.foreach { status =>
    it should s"abort all ${status.toString} workflows for a submission marked as aborting" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      withStatsD {
        val workflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

        runAndWait(workflowQuery.batchUpdateStatus(workflowRecs, status))
        runAndWait(submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Aborting))

        val submission = runAndWait(submissionQuery.loadSubmission(UUID.fromString(testData.submission1.submissionId))).get
        assert(submission.status == SubmissionStatuses.Aborting)

        assertResult(Seq.fill(workflowRecs.size) {
          status.toString
        }) {
          runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result)
        }

        val monitorRef = createSubmissionMonitorActor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Submitted.toString))
        watch(monitorRef)

        awaitCond(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result).forall(_ == WorkflowStatuses.Aborted.toString), 10 seconds)
        expectMsgClass(5 seconds, classOf[Terminated])
      } { capturedMetrics =>
        capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Aborted))
      }
    }
  }

  it should "queryExecutionServiceForStatus exception" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val exception = new RuntimeException
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(throw exception))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assertResult(ExecutionServiceStatusResponse(
      workflowsRecs.map { workflowRec => scala.util.Failure(exception) }
    )) {
      await(monitor.queryExecutionServiceForStatus())
    }
  }

  WorkflowStatuses.queuedStatuses.foreach { status =>
    it should s"checkOverallStatus queued status - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

      runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(status.toString))

      val initialStatus = SubmissionStatuses.Submitted
      runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(initialStatus.toString))

      assert(!runAndWait(monitor.updateSubmissionStatus(this)), "Queued workflows should not result in the submission changing state")

      assertResult(initialStatus.toString) {
        runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).result.head).status
      }
    }
  }

  WorkflowStatuses.runningStatuses.foreach { status =>
    it should s"checkOverallStatus running status - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

      runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(status.toString))

      Set(SubmissionStatuses.Aborting, SubmissionStatuses.Submitted).foreach { initialStatus =>
        runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(initialStatus.toString))

        assert(!runAndWait(monitor.updateSubmissionStatus(this)))

        assertResult(initialStatus.toString) {
          runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).result.head).status
        }
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"checkOverallStatus terminal status - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      withStatsD {
        val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

        runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(status.toString))

        Set(SubmissionStatuses.Aborting, SubmissionStatuses.Submitted).foreach { initialStatus =>
          val expectedStatus = if (initialStatus == SubmissionStatuses.Aborting) SubmissionStatuses.Aborted else SubmissionStatuses.Done
          runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(initialStatus.toString))

          assert(runAndWait(monitor.updateSubmissionStatus(this)))

          assertResult(expectedStatus.toString) {
            runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).result.head).status
          }
        }
      } { capturedMetrics =>
        capturedMetrics should contain (expectedSubmissionStatusMetric(testData.workspace, SubmissionStatuses.Aborted))
        capturedMetrics should contain (expectedSubmissionStatusMetric(testData.workspace, SubmissionStatuses.Done))
      }
    }
  }

  private val outputs = ExecutionServiceOutputs("foo", Map("output" -> Left(AttributeString("hello world!")), "output2" -> Left(AttributeString("hello world.")), "output3" -> Left(AttributeString("hello workspace.")), "extra" -> Left(AttributeString("hello world!"))))

  it should "attachOutputs normal" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0, None), outputs))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExpressions: Map[String, String] = Map("output" -> "this.bar", "output2" -> "this.baz", "output3" -> "workspace.garble")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Left(
      WorkflowEntityUpdate(entity.toReference, Map(AttributeName.withDefaultNS("bar") -> AttributeString("hello world!"), AttributeName.withDefaultNS("baz") -> AttributeString("hello world."))),
      Option(testData.workspace.copy(attributes = testData.workspace.attributes + (AttributeName.withDefaultNS("garble") -> AttributeString("hello workspace."))))))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExpressions)
    }
  }

  it should "attachOutputs with library attributes" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0, None), outputs))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExpressions: Map[String, String] = Map("output" -> "this.library:bar", "output2" -> "this.library:baz", "output3" -> "workspace.library:garble")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    val expected = Seq(Left(
      (
        WorkflowEntityUpdate(entity.toReference, Map(
          AttributeName("library", "bar") -> AttributeString("hello world!"),
          AttributeName("library", "baz") -> AttributeString("hello world."))
        ),
        Option(testData.workspace.copy(attributes = testData.workspace.attributes +
          (AttributeName("library", "garble") -> AttributeString("hello workspace."))
        ))
      )))

    assertResult(expected) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExpressions)
    }
  }

  it should "attachOutputs only entities" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0, None), outputs))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExpressions: Map[String, String] = Map("output" -> "this.bar", "output2" -> "this.baz")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Left(
      (WorkflowEntityUpdate(entity.toReference, Map(AttributeName.withDefaultNS("bar") -> AttributeString("hello world!"), AttributeName.withDefaultNS("baz") -> AttributeString("hello world."))),
        None)))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExpressions)
    }
  }

  it should "attachOutputs none" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0, None), outputs))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExpressions: Map[String, String] = Map.empty

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Left((WorkflowEntityUpdate(entity.toReference, Map()), None)))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExpressions)
    }
  }

  it should "attachOutputs missing expected output" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowRecord = WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0, None)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((workflowRecord, ExecutionServiceOutputs("foo", Map("output" -> Left(AttributeString("hello world!"))))))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExpressions: Map[String, String] = Map("missing" -> "this.bar")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Right((workflowRecord, Seq(AttributeString(s"output named missing does not exist")))))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExpressions)
    }
  }

  it should "save workflow error messages" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    withStatsD {
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

      runAndWait(monitor.saveErrors(workflowsRecs.map(r => (r, Seq(AttributeString("a"), AttributeString("b")))), this))

      val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId)).get

      assert(submission.workflows.forall(_.status == WorkflowStatuses.Failed))

      submission.workflows.foreach { workflow =>
        assertResult(Seq(AttributeString("a"), AttributeString("b"))) {
          workflow.messages
        }
      }
    } { capturedMetrics =>
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Failed))
    }
  }

  it should "handle outputs" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))
    val workflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

    runAndWait(monitor.handleOutputs(workflowRecs.map(r => (r, ExecutionServiceOutputs(r.externalId.get, Map("o1" -> Left(AttributeString("result")))))), this))

    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes + (AttributeName.withDefaultNS("foo") -> AttributeString("result"))))) {
      testData.submissionUpdateEntity.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }
  }

  it should "handle inputs and outputs with library attributes" in withDefaultTestDatabase { dataSource: SlickDataSource =>

    val mcUpdateEntityLibraryOutputs = MethodConfiguration("ns", "testConfig12", "Sample", Map(), Map(), Map("o1_lib" -> AttributeString("this.library:foo")), MethodRepoMethod("ns-config", "meth1", 1))

    val subUpdateEntityLibraryOutputs = createTestSubmission(testData.workspace, mcUpdateEntityLibraryOutputs, testData.indiv1, testData.userOwner,
      Seq(testData.indiv1), Map(testData.indiv1 -> testData.inputResolutions),
      Seq(testData.indiv2), Map(testData.indiv2 -> testData.inputResolutions2))

    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(methodConfigurationQuery.create(context, mcUpdateEntityLibraryOutputs))
      runAndWait(submissionQuery.create(context, subUpdateEntityLibraryOutputs))
    }

    val monitor = createSubmissionMonitor(dataSource, subUpdateEntityLibraryOutputs, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))
    val workflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(subUpdateEntityLibraryOutputs.submissionId)))

    runAndWait(monitor.handleOutputs(workflowRecs.map(r => (r, ExecutionServiceOutputs(r.externalId.get, Map("o1_lib" -> Left(AttributeString("result")))))), this))

    val expectedOut = Map(AttributeName("library", "foo") -> AttributeString("result"))
    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes ++ expectedOut))) {
      subUpdateEntityLibraryOutputs.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }

    val mcUpdateEntityLibraryInputs = MethodConfiguration("ns", "testConfig11", "Sample", Map(), Map("i_lib" -> AttributeString("this.library:foo")), Map("o2_lib" -> AttributeString("this.library:bar")), MethodRepoMethod("ns-config", "meth1", 1))

    val subUpdateEntityLibraryInputs = createTestSubmission(testData.workspace, mcUpdateEntityLibraryInputs, testData.indiv1, testData.userOwner,
      Seq(testData.indiv1), Map(testData.indiv1 -> testData.inputResolutions),
      Seq(testData.indiv2), Map(testData.indiv2 -> testData.inputResolutions2))

    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(methodConfigurationQuery.create(context, mcUpdateEntityLibraryInputs))
      runAndWait(submissionQuery.create(context, subUpdateEntityLibraryInputs))
    }

    val monitor2 = createSubmissionMonitor(dataSource, subUpdateEntityLibraryInputs, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))
    val workflowRecs2 = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(subUpdateEntityLibraryInputs.submissionId)))

    runAndWait(monitor2.handleOutputs(workflowRecs2.map(r => (r, ExecutionServiceOutputs(r.externalId.get, Map("o2_lib" -> Left(AttributeString("result2")))))), this))

    val expectedIn = Map(AttributeName("library", "bar") -> AttributeString("result2"))
    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes ++ expectedOut ++ expectedIn))) {
      subUpdateEntityLibraryOutputs.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }

  }

  it should "handleStatusResponses with no workflows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submissionNoWorkflows, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Running.toString))

    assertResult(StatusCheckComplete(true)) {
      await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(Seq.empty)))
    }
  }

  WorkflowStatuses.queuedStatuses.foreach { status =>
    it should s"handleStatusResponses from exec svc - queued - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)) flatMap { workflowRecs =>
        workflowQuery.batchUpdateStatus(workflowRecs, status)
      })
      val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))

      assertResult(StatusCheckComplete(false)) {
        await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(Seq.empty)))
      }
    }
  }

  WorkflowStatuses.runningStatuses.foreach { status =>
    it should s"handleStatusResponses from exec svc - running - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))
      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

      assertResult(StatusCheckComplete(false)) {
        await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(workflowsRecs.map(r => scala.util.Success(Option((r.copy(status = status.toString), None)))))))
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"handleStatusResponses from exec svc - terminal - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))
      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

      assertResult(StatusCheckComplete(true)) {
        await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(workflowsRecs.map(r => scala.util.Success(Option((r.copy(status = status.toString), None)))))))
      }
    }
  }

  it should s"handleStatusResponses from exec svc - success with outputs" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val status = WorkflowStatuses.Succeeded
    val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))
    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

    assertResult(StatusCheckComplete(true)) {
      await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(workflowsRecs.map(r => scala.util.Success(Option((r.copy(status = status.toString), Option(ExecutionServiceOutputs(r.externalId.get, Map("o1" -> Left(AttributeString("result"))))))))))))
    }

    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes + (AttributeName.withDefaultNS("foo") -> AttributeString("result"))))) {
      testData.submissionUpdateEntity.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }
  }

  it should "handleOutputs which are unbound by ignoring them" in withDefaultTestDatabase { dataSource: SlickDataSource =>

    val unboundExprStr = AttributeString("")
    val unboundAttr = AttributeString("result1")
    val boundExprStr = AttributeString("this.ok")
    val boundAttr = AttributeString("result2")
    val outputExpressions = Map("unbound" -> unboundExprStr, "bound" ->  boundExprStr)
    val execOutputs = Map("unbound" -> Left(unboundAttr), "bound" ->  Left(boundAttr))

    val expectedAttributeUpdate = OutputExpression.build(boundExprStr.value, boundAttr) match {
      case scala.util.Success(BoundOutputExpression(_, attrName, attr)) => attrName -> attr
      case _ => fail
    }

    val mcUnboundExpr = MethodConfiguration("ns", "testConfig12", "Sample", Map(), Map(), outputExpressions, MethodRepoMethod("ns-config", "meth1", 1))

    val subUnboundExpr = createTestSubmission(testData.workspace, mcUnboundExpr, testData.indiv1, testData.userOwner,
      Seq(testData.indiv1), Map(testData.indiv1 -> testData.inputResolutions),
      Seq(testData.indiv2), Map(testData.indiv2 -> testData.inputResolutions2))

    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(methodConfigurationQuery.create(context, mcUnboundExpr))
      runAndWait(submissionQuery.create(context, subUnboundExpr))
    }

    val monitor = createSubmissionMonitor(dataSource, subUnboundExpr, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))
    val workflowRec = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(subUnboundExpr.submissionId))).head

    runAndWait(monitor.handleOutputs(Seq((workflowRec, ExecutionServiceOutputs(workflowRec.externalId.get, execOutputs))), this))

    // only the bound attribute was updated
    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes + expectedAttributeUpdate))) {
      subUnboundExpr.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }

    val resultWorkflow = withWorkspaceContext(testData.workspace) { context =>
      runAndWait(workflowQuery.get(context, subUnboundExpr.submissionId, testData.indiv1.entityType, testData.indiv1.name))
    }.get

    // the workflow status was not changed
    assertResult(workflowRec.status) {
      resultWorkflow.status.toString
    }

    // no error was recorded
    assert {
      ! resultWorkflow.messages.exists {
        _.value.contains("Invalid")
      }
    }
  }

  it should "fail workflows with invalid output expressions" in withDefaultTestDatabase { dataSource: SlickDataSource =>

    val badExprs = Seq(
      "this.",
      "this.bad|character",
      "this.case_sample.attribute",
      "workspace.",
      "workspace........",
      "workspace.nope.nope.nope",
      "where_does_this_even_go",
      "*")

    badExprs foreach { badExpr =>
      val mcBadExprs = MethodConfiguration("ns", "testConfig12", "Sample", Map(), Map(), Map("bad1" -> AttributeString(badExpr)), MethodRepoMethod("ns-config", "meth1", 1))

      val subBadExprs = createTestSubmission(testData.workspace, mcBadExprs, testData.indiv1, testData.userOwner,
        Seq(testData.indiv1), Map(testData.indiv1 -> testData.inputResolutions),
        Seq(testData.indiv2), Map(testData.indiv2 -> testData.inputResolutions2))

      withWorkspaceContext(testData.workspace) { context =>
        runAndWait(methodConfigurationQuery.create(context, mcBadExprs))
        runAndWait(submissionQuery.create(context, subBadExprs))
      }

      val monitor = createSubmissionMonitor(dataSource, subBadExprs, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))
      val workflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(subBadExprs.submissionId)))

      runAndWait(monitor.handleOutputs(workflowRecs.map(r => (r, ExecutionServiceOutputs(r.externalId.get, Map("bad1" -> Left(AttributeString("result")))))), this))

      // the entity was not updated
      assertResult(Seq(testData.indiv1)) {
        subBadExprs.workflows.map { wf =>
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
        }
      }

      val resultWorkflow = withWorkspaceContext(testData.workspace) { context =>
        runAndWait(workflowQuery.get(context, subBadExprs.submissionId, testData.indiv1.entityType, testData.indiv1.name))
      }.get

      // the workflow was marked as failed
      assertResult(WorkflowStatuses.Failed) {
        resultWorkflow.status
      }

      // the error was recorded
      assert {
        resultWorkflow.messages.exists {
          _.value.contains("Invalid")
        }
      }
    }
  }

  it should "handleStatusResponses and fail workflows that are missing outputs" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    runAndWait {
      withWorkspaceContext(testData.workspace) { context =>
        submissionQuery.create(context, testData.submissionMissingOutputs)
      }
    }

    def getWorkflowRec = {
      runAndWait(
        workflowQuery.findWorkflowByExternalIdAndSubmissionId(
          testData.submissionMissingOutputs.workflows.head.workflowId.get,
          UUID.fromString(testData.submissionMissingOutputs.submissionId)).result).head
    }

    val workflowRecBefore = getWorkflowRec
    val monitor = createSubmissionMonitor(dataSource, testData.submissionMissingOutputs, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    import spray.json._
    val outputsJsonBad = s"""{
                           |  "outputs": {
                           |  },
                           |  "id": "${workflowRecBefore.externalId.get}"
                           |}""".stripMargin
    val jss = org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport

    val badOutputs = jss.ExecutionServiceOutputsFormat.read(outputsJsonBad.parseJson)
    val badESSResponse = ExecutionServiceStatusResponse(Seq(Try(Option(workflowRecBefore, Option(badOutputs)))))

    Await.result( monitor.handleStatusResponses(badESSResponse), Duration.Inf )

    val workflowRecAfterBad = getWorkflowRec
    assert(workflowRecAfterBad.status == WorkflowStatuses.Failed.toString)
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when workflow is done - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      withStatsD {
        val monitorRef = createSubmissionMonitorActor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))
        watch(monitorRef)
        expectMsgClass(5 seconds, classOf[Terminated])

        assertResult(SubmissionStatuses.Done) {
          runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submissionUpdateEntity.submissionId)).get.status
        }
      } { capturedMetrics =>
        capturedMetrics should contain (expectedSubmissionStatusMetric(testData.workspace, SubmissionStatuses.Done))
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when workflow is aborted - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      withStatsD {
        runAndWait(submissionQuery.findById(UUID.fromString(testData.submissionUpdateEntity.submissionId)).map(_.status).update(SubmissionStatuses.Aborting.toString))
        val monitorRef = createSubmissionMonitorActor(dataSource, testData.submissionUpdateEntity, testData.wsName, new SubmissionTestExecutionServiceDAO(status.toString))
        watch(monitorRef)
        expectMsgClass(5 seconds, classOf[Terminated])

        assertResult(SubmissionStatuses.Aborted) {
          runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submissionUpdateEntity.submissionId)).get.status
        }
      } { capturedMetrics =>
        capturedMetrics should contain (expectedSubmissionStatusMetric(testData.workspace, SubmissionStatuses.Aborted))
      }
    }
  }

  it should "stop trying to monitor a submission that's been deleted" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitorRef = createSubmissionMonitorActor(dataSource, testData.submission1, testData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Running.toString))
    watch(monitorRef)

    runAndWait(submissionQuery.delete(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId))

    expectMsgClass(5 seconds, classOf[Terminated])
  }

  val manySubmissionsTestData = new ManySubmissionsTestData
  it should "attach outputs and not deadlock with multiple submissions all updating the same entity at once" in withCustomTestDatabase(manySubmissionsTestData) { dataSource: SlickDataSource =>
    val submissions = manySubmissionsTestData.submissions
    val numSubmissions = submissions.length
    submissions.foreach ( sub => createSubmissionMonitorActor(dataSource, sub, manySubmissionsTestData.wsName, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString)) )

    //they're all being monitored. they should all complete just fine, without deadlocking forever or otherwise barfing
    awaitCond({
      val submissionList = runAndWait(DBIO.sequence(submissions map { sub: Submission =>
        submissionQuery.findById(UUID.fromString(sub.submissionId)).result
      })).flatten
      submissionList.forall(_.status == SubmissionStatuses.Done.toString) && submissionList.length == numSubmissions
    }, max = 30 seconds, interval = 1 second)

    //check that all the outputs got bound correctly too
    val subKeys = (1 to numSubmissions).map ( subNum => AttributeName.fromDelimitedName(s"sub_$subNum") )

    withWorkspaceContext(manySubmissionsTestData.workspace) { ctx =>
      val indiv1 = runAndWait(entityQuery.get(ctx, testData.indiv1.entityType, testData.indiv1.name)).get
      val indiv2 = runAndWait(entityQuery.get(ctx, testData.indiv2.entityType, testData.indiv2.name)).get

      indiv1.attributes.keys.filter(an => an.name.startsWith("sub_")) should contain theSameElementsAs subKeys
      indiv2.attributes.keys.filter(an => an.name.startsWith("sub_")) should contain theSameElementsAs subKeys
    }
  }

  class ManySubmissionsTestData extends EmptyWorkspace() {
    val numSubmissions = 50

    val (submissions, methodConfigs) = (1 to numSubmissions).map { subNumber =>
      val methodConfig = testData.methodConfigEntityUpdate.copy(name = s"this.sub_$subNumber", outputs = Map("o1" -> AttributeString(s"this.sub_$subNumber")))
      val testSub = createTestSubmission(testData.workspace, methodConfig, testData.indiv1, testData.userOwner,
        Seq(testData.indiv1, testData.indiv2), Map(testData.indiv1 -> testData.inputResolutions, testData.indiv2 -> testData.inputResolutions),
        Seq(), Map())

      (testSub, methodConfig)
    }.unzip

    override def save() = {
      super.save() flatMap { _ =>
        withWorkspaceContext(workspace) { ctx =>
          DBIO.seq(
            entityQuery.save(ctx, Seq(testData.aliquot1, testData.aliquot2, testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sample8, testData.pair1, testData.pair2, testData.ps1, testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty, testData.indiv1, testData.indiv2)),
            DBIO.sequence(methodConfigs.map(m => methodConfigurationQuery.create(ctx, m)).toSeq),
            DBIO.sequence(submissions.map(s => submissionQuery.create(ctx, s)).toSeq),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }
      }
    }
  }

  def createSubmissionMonitorActor(dataSource: SlickDataSource, submission: Submission, wsName: WorkspaceName, execSvcDAO: ExecutionServiceDAO): TestActorRef[SubmissionMonitorActor] = {
    TestActorRef[SubmissionMonitorActor](SubmissionMonitorActor.props(
      wsName,
      UUID.fromString(submission.submissionId),
      dataSource,
      MockShardedExecutionServiceCluster.fromDAO(execSvcDAO, dataSource),
      new Builder().build(),
      1 second,
      "test"
    ))
  }

  def createSubmissionMonitor(dataSource: SlickDataSource, submission: Submission, wsName: WorkspaceName, execSvcDAO: ExecutionServiceDAO): SubmissionMonitor = {
    new TestSubmissionMonitor(
      wsName,
      UUID.fromString(submission.submissionId),
      dataSource,
      MockShardedExecutionServiceCluster.fromDAO(execSvcDAO, dataSource),
      new Builder().build(),
      1 minutes,
      "test"
    )
  }

  private def ignoreStatusLastChangedDate(response: ExecutionServiceStatusResponse): ExecutionServiceStatusResponse = {
    ExecutionServiceStatusResponse(response.statusResponse.map {
      case scala.util.Success(Some((workflowRec, execOutputs))) => scala.util.Success(Some(workflowRec.copy(statusLastChangedDate = null), execOutputs))
      case otherwise => otherwise
    })
  }
}

class SubmissionTestExecutionServiceDAO(workflowStatus: => String) extends ExecutionServiceDAO {
  val abortedMap: scala.collection.concurrent.TrieMap[String, String] = new scala.collection.concurrent.TrieMap[String, String]()

  override def submitWorkflows(wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo) = Future.successful(Seq(Left(ExecutionServiceStatus("test_id", workflowStatus))))

  override def outputs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceOutputs(id, Map("o1" -> Left(AttributeString("foo")))))
  override def logs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceLogs(id, Map("task1" -> Seq(ExecutionServiceCallLogs(stdout = "foo", stderr = "bar")))))

  override def status(id: String, userInfo: UserInfo) = {
    if(abortedMap.keySet.contains(id)) Future(ExecutionServiceStatus(id, WorkflowStatuses.Aborted.toString))
    else Future(ExecutionServiceStatus(id, workflowStatus))
  }
  override def abort(id: String, userInfo: UserInfo) = {
    abortedMap += id -> WorkflowStatuses.Aborting.toString
    Future.successful(Success(ExecutionServiceStatus(id, WorkflowStatuses.Aborting.toString)))
  }
  override def callLevelMetadata(id: String, userInfo: UserInfo) = Future.successful(null)

  override def version = Future.successful(ExecutionServiceVersion("25"))
}

class TestSubmissionMonitor(val workspaceName: WorkspaceName,
                            val submissionId: UUID,
                            val datasource: SlickDataSource,
                            val executionServiceCluster: ExecutionServiceCluster,
                            val credential: Credential,
                            val submissionPollInterval: Duration,
                            override val workbenchMetricBaseName: String) extends SubmissionMonitor
