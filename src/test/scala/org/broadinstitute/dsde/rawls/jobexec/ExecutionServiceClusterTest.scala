package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, TestDriverComponent, WorkflowRecord}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{PrivateMethodTester, FlatSpecLike, Matchers}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

class ExecutionServiceClusterTest(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with PrivateMethodTester {
  import driver.api._

  def this() = this(ActorSystem("ExecutionServiceClusterTest"))

  // create a cluster of execution service DAOs
  val instanceMap:Map[ExecutionServiceId, ExecutionServiceDAO] = ((0 to 5) map {idx =>
      val key = s"instance$idx"
      (ExecutionServiceId(key) -> new MockExecutionServiceDAO(identifier = key))
    }).toMap
  // arbitrary choices for default and the instance we'll use for tests; neither first nor last instances
  val defaultInstanceKey = "instance2"
  val instanceKeyForTests = "instance4"
  val cluster = new ShardedHttpExecutionServiceCluster(instanceMap, ExecutionServiceId(defaultInstanceKey), slickDataSource)

  // private method wrappers for testing
  val getDefaultEntry = PrivateMethod[(ExecutionServiceId, ExecutionServiceDAO)]('getDefaultEntry)
  val getMember = PrivateMethod[Future[ExecutionServiceDAO]]('getMember)

  // UUIDs
  val subMissingExecutionKeys = UUID.randomUUID().toString
  val subWithExecutionKeys = UUID.randomUUID().toString
  val workflowExternalIdMissingExecutionKey = UUID.randomUUID().toString
  val workflowExternalIdWithExecutionKey = UUID.randomUUID().toString

  // additional test data
  val execClusterTestData = new ExecClusterTestData()

  class ExecClusterTestData() extends TestData {
    val wsName = WorkspaceName("ExecClusterTestDataNamespace", "ExecClusterTestDataName")
    val user = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("ExecClusterTestDataOwnerGroup", Set(user))
    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "ExecClusterTestDataBucket", currentTime(), currentTime(), "testUser", Map.empty, Map(WorkspaceAccessLevels.Owner -> ownerGroup), Map(WorkspaceAccessLevels.Owner -> ownerGroup))

    val sample1 = Entity("sample1", "Sample", Map("type" -> AttributeString("normal")))
    val sample2 = Entity("sample2", "Sample", Map("type" -> AttributeString("normal")))

    val submissionMissingExecutionKeys = Submission(subMissingExecutionKeys,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(Some(workflowExternalIdMissingExecutionKey),WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionWithExecutionKeys = Submission(subWithExecutionKeys,testDate, testData.userOwner, "std","someMethod",sample2.toReference,
      Seq(Workflow(Some(workflowExternalIdWithExecutionKey),WorkflowStatuses.Submitted,testDate,sample2.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(user),
        rawlsGroupQuery.save(ownerGroup),
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sample1),
            entityQuery.save(context, sample2),
            methodConfigurationQuery.save(context, MethodConfiguration("std", "someMethod", "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("std", "someMethod", 1))),
            submissionQuery.create(context, submissionMissingExecutionKeys),
            submissionQuery.create(context, submissionWithExecutionKeys)
          )
        },
        // update workflowExternalIdWithExecutionKey with an execution key
        workflowQuery.findWorkflowByExternalId(workflowExternalIdWithExecutionKey).result flatMap {wr =>
          workflowQuery.batchUpdateStatusAndExecutionServiceKey(wr, WorkflowStatuses.Submitted, ExecutionServiceId(instanceKeyForTests))
        }
      )
    }
  }

  "ShardedHttpExecutionServiceCluster" should "return default instance" in {
    val defaultEntry = cluster invokePrivate getDefaultEntry()
    assertResult(defaultInstanceKey){ defaultEntry._1.id }
  }

  "a WorkflowRecord with an executionServiceKey" should "return the correct instance" in withCustomTestDatabase(execClusterTestData) {  dataSource: SlickDataSource =>
    val dbquery = uniqueResult[WorkflowRecord](workflowQuery.findWorkflowByExternalId(workflowExternalIdWithExecutionKey))
    runAndWait(dbquery) match {
      case None => fail("did not find WorkflowRecord with workflowExternalIdWithExecutionKey")
      case Some(rec) =>
        System.out.println(rec)
        assertResult(instanceKeyForTests) {
          val execInstanceFuture = cluster invokePrivate getMember(WorkflowExecution(rec))
          Await.result(execInstanceFuture, Duration.Inf).asInstanceOf[MockExecutionServiceDAO].identifier
        }
    }
  }

  "a WorkflowRecord without an executionServiceKey" should "return the default instance" in withCustomTestDatabase(execClusterTestData) { dataSource: SlickDataSource =>
    val dbquery = uniqueResult[WorkflowRecord](workflowQuery.findWorkflowByExternalId(workflowExternalIdMissingExecutionKey))
    runAndWait(dbquery) match {
      case None => fail("did not find WorkflowRecord with workflowExternalIdMissingExecutionKey")
      case Some(rec) =>
        assertResult(defaultInstanceKey) {
          val execInstanceFuture = cluster invokePrivate getMember(WorkflowExecution(rec))
          Await.result(execInstanceFuture, Duration.Inf).asInstanceOf[MockExecutionServiceDAO].identifier
        }
    }
  }

  "a Workflow with an executionServiceKey" should "return the correct instance" in withCustomTestDatabase(execClusterTestData) { dataSource: SlickDataSource =>
    val dbquery = workflowQuery.getByExternalId(workflowExternalIdWithExecutionKey, subWithExecutionKeys)
    runAndWait(dbquery) match {
      case None => fail("did not find WorkflowRecord with workflowExternalIdWithExecutionKey")
      case Some(rec) =>
        assertResult(instanceKeyForTests) {
          val execInstanceFuture = cluster invokePrivate getMember(WorkflowExecution(rec))
          Await.result(execInstanceFuture, Duration.Inf).asInstanceOf[MockExecutionServiceDAO].identifier
        }
    }
  }

  "a Workflow without an executionServiceKey" should "return the default instance" in withCustomTestDatabase(execClusterTestData) { dataSource: SlickDataSource =>
    val dbquery = workflowQuery.getByExternalId(workflowExternalIdMissingExecutionKey, subMissingExecutionKeys)
    runAndWait(dbquery) match {
      case None => fail("did not find WorkflowRecord with workflowExternalIdMissingExecutionKey")
      case Some(rec) =>
        assertResult(defaultInstanceKey) {
          val execInstanceFuture = cluster invokePrivate getMember(WorkflowExecution(rec))
          Await.result(execInstanceFuture, Duration.Inf).asInstanceOf[MockExecutionServiceDAO].identifier
        }
    }
  }

  "ShardedHttpExecutionServiceCluster" should "calculate target index with 4 targets" in {
    /*
      with 4 targets, the routing algorithm should be: if the seed ends in ...
        00-24: 0
        25-49: 1
        50-74: 2
        75-99: 3
     */
    val numTargets = 4
    assertResult(0, 0){ cluster.targetIndex(0, numTargets) }
    assertResult(0, 10){ cluster.targetIndex(10, numTargets) }
    assertResult(0, 24){ cluster.targetIndex(24, numTargets) }
    assertResult(0, 1001100100){ cluster.targetIndex(1001100100, numTargets) }
    assertResult(0, 1001100110){ cluster.targetIndex(1001100110, numTargets) }
    assertResult(0, 1001100124){ cluster.targetIndex(1001100124, numTargets) }

    assertResult(1, 25){ cluster.targetIndex(25, numTargets) }
    assertResult(1, 35){ cluster.targetIndex(35, numTargets) }
    assertResult(1, 49){ cluster.targetIndex(49, numTargets) }
    assertResult(1, 1001100125){ cluster.targetIndex(1001100125, numTargets) }
    assertResult(1, 1001100135){ cluster.targetIndex(1001100135, numTargets) }
    assertResult(1, 1001100149){ cluster.targetIndex(1001100149, numTargets) }

    assertResult(2, 50){ cluster.targetIndex(50, numTargets) }
    assertResult(2, 65){ cluster.targetIndex(65, numTargets) }
    assertResult(2, 74){ cluster.targetIndex(74, numTargets) }
    assertResult(2, 1001100150){ cluster.targetIndex(1001100150, numTargets) }
    assertResult(2, 1001100165){ cluster.targetIndex(1001100165, numTargets) }
    assertResult(2, 1001100174){ cluster.targetIndex(1001100174, numTargets) }

    assertResult(3, 75){ cluster.targetIndex(75, numTargets) }
    assertResult(3, 85){ cluster.targetIndex(85, numTargets) }
    assertResult(3, 99){ cluster.targetIndex(99, numTargets) }
    assertResult(3, 1001100175){ cluster.targetIndex(1001100175, numTargets) }
    assertResult(3, 1001100185){ cluster.targetIndex(1001100185, numTargets) }
    assertResult(3, 1001100199){ cluster.targetIndex(1001100199, numTargets) }
  }

  it should "calculate target index properly with 5 targets" in {
    /*
      with 5 targets, the routing algorithm should be: if the seed ends in ...
        00-19: 0
        20-39: 1
        40-59: 2
        60-79: 3
        80-99: 4
    */
    val numTargets = 5
    assertResult(0, 11100){ cluster.targetIndex(11100, numTargets) }
    assertResult(0, 11119){ cluster.targetIndex(11119, numTargets) }

    assertResult(1, 11120){ cluster.targetIndex(11120, numTargets) }
    assertResult(1, 11139){ cluster.targetIndex(11139, numTargets) }

    assertResult(2, 11140){ cluster.targetIndex(11140, numTargets) }
    assertResult(2, 11159){ cluster.targetIndex(11159, numTargets) }

    assertResult(3, 11160){ cluster.targetIndex(11160, numTargets) }
    assertResult(3, 11179){ cluster.targetIndex(11179, numTargets) }

    assertResult(4, 11180){ cluster.targetIndex(11180, numTargets) }
    assertResult(4, 11199){ cluster.targetIndex(11199, numTargets) }
  }

}
