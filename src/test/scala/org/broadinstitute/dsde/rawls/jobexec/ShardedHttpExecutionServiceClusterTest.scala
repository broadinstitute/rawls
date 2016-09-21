package org.broadinstitute.dsde.rawls.jobexec

import java.sql.Timestamp
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, TestDriverComponent, WorkflowRecord}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{PrivateMethodTester, FlatSpecLike, Matchers}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.Random

class ShardedHttpExecutionServiceClusterTest(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with PrivateMethodTester {
  import driver.api._

  def this() = this(ActorSystem("ExecutionServiceClusterTest"))

  // create a cluster of execution service DAOs
  val instanceMap:Map[ExecutionServiceId, ExecutionServiceDAO] = ((0 to 4) map {idx =>
      val key = s"instance$idx"
      (ExecutionServiceId(key) -> new MockExecutionServiceDAO(identifier = key))
    }).toMap

  // arbitrary choice for the instance we'll use for tests; neither first nor last instances
  val instanceKeyForTests = "instance3"
  val cluster = new ShardedHttpExecutionServiceCluster(instanceMap, slickDataSource)

  // private method wrapper for testing
  val getMember = PrivateMethod[ExecutionServiceDAO]('getMember)

  val submissionId = UUID.randomUUID()

  // dummy WorkflowRecord
  val testWorkflowRecord = WorkflowRecord(
    1, Some(UUID.randomUUID().toString), submissionId, "Submitted",
    new Timestamp(System.currentTimeMillis()), 1, 1, Some("default"))

  // UUIDs
  val subWithExecutionKeys = UUID.randomUUID()
  val workflowExternalIdWithExecutionKey = UUID.randomUUID()

  // additional test data
  val execClusterTestData = new ExecClusterTestData()

  class ExecClusterTestData() extends TestData {
    val wsName = WorkspaceName("ExecClusterTestDataNamespace", "ExecClusterTestDataName")
    val user = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("ExecClusterTestDataOwnerGroup", Set(user))
    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "ExecClusterTestDataBucket", currentTime(), currentTime(), "testUser", Map.empty, Map(WorkspaceAccessLevels.Owner -> ownerGroup), Map(WorkspaceAccessLevels.Owner -> ownerGroup))

    val sample1 = Entity("sample1", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))

    val submissionWithExecutionKeys = Submission(subWithExecutionKeys.toString, testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(Some(workflowExternalIdWithExecutionKey.toString),WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(user),
        rawlsGroupQuery.save(ownerGroup),
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sample1),
            methodConfigurationQuery.save(context, MethodConfiguration("std", "someMethod", "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("std", "someMethod", 1))),
            submissionQuery.create(context, submissionWithExecutionKeys)
          )
        },
        // update exec key for all test data workflows that have been started.
        updateWorkflowExecutionServiceKey("instance3")
      )
    }
  }

 it should "return the correct instance from a WorkflowRecord with an executionServiceKey" in withCustomTestDatabase(execClusterTestData) {  dataSource: SlickDataSource =>
    val dbquery = uniqueResult[WorkflowRecord](workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowExternalIdWithExecutionKey.toString, subWithExecutionKeys))
    runAndWait(dbquery) match {
      case None => fail("did not find WorkflowRecord with workflowExternalIdWithExecutionKey")
      case Some(rec) =>
        assertResult(instanceKeyForTests) {
          val execInstance = cluster invokePrivate getMember(rec)
          execInstance.asInstanceOf[MockExecutionServiceDAO].identifier
        }
    }
  }

  it should "throw exception on getMember when missing an external id" in {
    val wr = testWorkflowRecord.copy(externalId = None)
    intercept[RawlsException] {
      cluster invokePrivate getMember(wr)
    }
  }

  it should "throw exception on getMember when missing an execution service key" in {
    val wr = testWorkflowRecord.copy(executionServiceKey = None)
    intercept[RawlsException] {
      cluster invokePrivate getMember(wr)
    }
  }

  it should "throw exception on getMember when missing both external id and execution service key" in {
    val wr = testWorkflowRecord.copy(externalId = None, executionServiceKey = None)
    intercept[RawlsException] {
      cluster invokePrivate getMember(wr)
    }
  }

  it should "calculate target index with 4 targets" in {
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

  it should "return appropriate cluster members when submitting workflows"  in withCustomTestDatabase(execClusterTestData) { dataSource: SlickDataSource =>
    // we constructed our test cluster with 5 instances; see instanceMap above
    // when submitting workflows, we determine which instance we'll submit to by reading the first workflow's timestamp.
    // therefore, we replicate some of the "calculate target index properly with 5 targets" unit test above.
    Seq(
      (11119 -> "instance0"),
      (11120 -> "instance1"),
      (11159 -> "instance2"),
      (11160 -> "instance3"),
      (11180 -> "instance4")
    ) map {
      case (seed, expectedInstanceId) => {
        val batch = batchTestWorkflows(seed)
        val submissionResult = Await.result(cluster.submitWorkflows(batch, "wdl", Seq.fill(batch.size)("inputs"), None, userInfo), Duration.Inf)
        assertResult(batch.size, s"size for seed $seed") { submissionResult._2.size }
        assertResult(ExecutionServiceId(expectedInstanceId), expectedInstanceId) {submissionResult._1}

        val submittedRecordsQuery = dataSource.inTransaction { dataAccess => workflowQuery.findWorkflowByIds(batch map (_.id)).result}
        val submittedRecords = Await.result(submittedRecordsQuery, Duration.Inf)
        assert(submittedRecords.forall(_.executionServiceKey == expectedInstanceId))
      }
    }
  }

  private def batchTestWorkflows(seed: Long) = {
    val leadWorkflow = testWorkflowRecord.copy(statusLastChangedDate = new Timestamp(seed))

    val workflowsToSubmit:Seq[WorkflowRecord] = (30 to 40+new Random().nextInt(20)) map {idx => testWorkflowRecord.copy(statusLastChangedDate = new Timestamp(seed+idx))}

    Seq(leadWorkflow) ++ workflowsToSubmit
  }

  // Note: we do not test the remaining facade methods in ShardedHttpExecutionCluster, because they are so thin

}
