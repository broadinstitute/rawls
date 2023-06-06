package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, TestDriverComponent, WorkflowRecord}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import spray.json.JsonParser

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

//noinspection TypeAnnotation,RedundantBlock,ScalaUnnecessaryParentheses,ScalaUnusedSymbol
class ShardedHttpExecutionServiceClusterTest(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with PrivateMethodTester
    with ScalaFutures
    with TestDriverComponent
    with RawlsTestUtils {
  import driver.api._

  def this() = this(ActorSystem("ExecutionServiceClusterTest"))

  // create a cluster of execution service DAOs
  val instanceMap: Set[ClusterMember] = ((0 to 4) map { idx =>
    val key = s"instance$idx"
    ClusterMember(ExecutionServiceId(key), new MockExecutionServiceDAO(identifier = key))
  }).toSet

  // arbitrary choice for the instance we'll use for tests; neither first nor last instances
  val instanceKeyForTests = "instance3"
  val cluster = new ShardedHttpExecutionServiceCluster(instanceMap, instanceMap, slickDataSource)

  // private method wrappers for testing
  val getMember = PrivateMethod[ClusterMember]('getMember)
  val parseSubWorkflowIdsFromMetadata = PrivateMethod[Seq[String]]('parseSubWorkflowIdsFromMetadata)

  val submissionId = UUID.randomUUID()

  // dummy WorkflowRecord
  val testWorkflowRecord = WorkflowRecord(1,
                                          Some(UUID.randomUUID().toString),
                                          submissionId,
                                          "Submitted",
                                          new Timestamp(System.currentTimeMillis()),
                                          Some(1),
                                          1,
                                          Some("default"),
                                          None
  )

  // UUIDs
  val subWithExecutionKeys = UUID.randomUUID()
  val workflowExternalIdWithExecutionKey = UUID.randomUUID()

  // additional test data
  val execClusterTestData = new ExecClusterTestData()

  class ExecClusterTestData() extends TestData {
    val wsName = WorkspaceName("ExecClusterTestDataNamespace", "ExecClusterTestDataName")
    val user = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("ExecClusterTestDataOwnerGroup", Set(user))
    val workspace = Workspace(
      wsName.namespace,
      wsName.name,
      UUID.randomUUID().toString,
      "ExecClusterTestDataBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty
    )

    val sample1 = Entity("sample1", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))

    val submissionWithExecutionKeys = Submission(
      submissionId = subWithExecutionKeys.toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(testData.userOwner.userEmail.value),
      methodConfigurationNamespace = "std",
      methodConfigurationName = "someMethod",
      submissionEntity = Option(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option(workflowExternalIdWithExecutionKey.toString),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sample1),
            methodConfigurationQuery.create(context,
                                            MethodConfiguration("std",
                                                                "someMethod",
                                                                Some("Sample"),
                                                                None,
                                                                Map.empty,
                                                                Map.empty,
                                                                AgoraMethod("std", "someMethod", 1)
                                            )
            ),
            submissionQuery.create(context, submissionWithExecutionKeys)
          )
        },
        // update exec key for all test data workflows that have been started.
        updateWorkflowExecutionServiceKey(instanceKeyForTests)
      )
  }

  it should "return the correct instance from a WorkflowRecord with an executionServiceKey" in withCustomTestDatabase(
    execClusterTestData
  ) { dataSource: SlickDataSource =>
    val dbquery = uniqueResult[WorkflowRecord](
      workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowExternalIdWithExecutionKey.toString,
                                                            subWithExecutionKeys
      )
    )
    runAndWait(dbquery) match {
      case None => fail("did not find WorkflowRecord with workflowExternalIdWithExecutionKey")
      case Some(rec) =>
        assertResult(instanceKeyForTests) {
          val execInstance = cluster invokePrivate getMember(rec)
          execInstance.dao.asInstanceOf[MockExecutionServiceDAO].identifier
        }
    }
  }

  it should "return the correct instance from a raw executionServiceKey" in withCustomTestDatabase(
    execClusterTestData
  ) { dataSource: SlickDataSource =>
    assertResult(instanceKeyForTests) {
      val execInstance = cluster invokePrivate getMember(ExecutionServiceId(instanceKeyForTests))
      execInstance.dao.asInstanceOf[MockExecutionServiceDAO].identifier
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

  it should "find the execution service, if present" in {
    // shortcut the execution service cluster when supplying an Execution ID
    val testId = ExecutionServiceId("Internal to this test")
    assertResult(testId) {
      Await.result(cluster.findExecService("garbage", "also garbage", userInfo, Some(testId)), 10.seconds)
    }

    val subId = "my-submission"
    val wfId = "my-workflow"
    val testExecId = ExecutionServiceId("instance" + Random.nextInt(5))

    val execInstance = cluster invokePrivate getMember(testExecId)
    // normally the Workflow would need to already exist on this instance.  Not true for the mock version.
    execInstance.dao.patchLabels(wfId, userInfo: UserInfo, Map(cluster.SUBMISSION_ID_KEY -> subId))

    assertResult(testExecId) {
      Await.result(cluster.findExecService(subId, wfId, userInfo, None), 10.seconds)
    }

    // confirm exception with relevant info when the instance is not found

    val mapWithoutInstance = instanceMap.filterNot { case ClusterMember(id, _) => id == testExecId }
    val clusterWithoutInstance =
      new ShardedHttpExecutionServiceCluster(mapWithoutInstance, mapWithoutInstance, slickDataSource)

    val exception = clusterWithoutInstance.findExecService(subId, wfId, userInfo, None).failed.futureValue
    exception shouldBe a[RawlsException]
    exception.getMessage should include(subId)
    exception.getMessage should include(wfId)
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
    assertResult(0, 0)(cluster.targetIndex(0, numTargets))
    assertResult(0, 10)(cluster.targetIndex(10, numTargets))
    assertResult(0, 24)(cluster.targetIndex(24, numTargets))
    assertResult(0, 1001100100)(cluster.targetIndex(1001100100, numTargets))
    assertResult(0, 1001100110)(cluster.targetIndex(1001100110, numTargets))
    assertResult(0, 1001100124)(cluster.targetIndex(1001100124, numTargets))

    assertResult(1, 25)(cluster.targetIndex(25, numTargets))
    assertResult(1, 35)(cluster.targetIndex(35, numTargets))
    assertResult(1, 49)(cluster.targetIndex(49, numTargets))
    assertResult(1, 1001100125)(cluster.targetIndex(1001100125, numTargets))
    assertResult(1, 1001100135)(cluster.targetIndex(1001100135, numTargets))
    assertResult(1, 1001100149)(cluster.targetIndex(1001100149, numTargets))

    assertResult(2, 50)(cluster.targetIndex(50, numTargets))
    assertResult(2, 65)(cluster.targetIndex(65, numTargets))
    assertResult(2, 74)(cluster.targetIndex(74, numTargets))
    assertResult(2, 1001100150)(cluster.targetIndex(1001100150, numTargets))
    assertResult(2, 1001100165)(cluster.targetIndex(1001100165, numTargets))
    assertResult(2, 1001100174)(cluster.targetIndex(1001100174, numTargets))

    assertResult(3, 75)(cluster.targetIndex(75, numTargets))
    assertResult(3, 85)(cluster.targetIndex(85, numTargets))
    assertResult(3, 99)(cluster.targetIndex(99, numTargets))
    assertResult(3, 1001100175)(cluster.targetIndex(1001100175, numTargets))
    assertResult(3, 1001100185)(cluster.targetIndex(1001100185, numTargets))
    assertResult(3, 1001100199)(cluster.targetIndex(1001100199, numTargets))
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
    assertResult(0, 11100)(cluster.targetIndex(11100, numTargets))
    assertResult(0, 11119)(cluster.targetIndex(11119, numTargets))

    assertResult(1, 11120)(cluster.targetIndex(11120, numTargets))
    assertResult(1, 11139)(cluster.targetIndex(11139, numTargets))

    assertResult(2, 11140)(cluster.targetIndex(11140, numTargets))
    assertResult(2, 11159)(cluster.targetIndex(11159, numTargets))

    assertResult(3, 11160)(cluster.targetIndex(11160, numTargets))
    assertResult(3, 11179)(cluster.targetIndex(11179, numTargets))

    assertResult(4, 11180)(cluster.targetIndex(11180, numTargets))
    assertResult(4, 11199)(cluster.targetIndex(11199, numTargets))
  }

  it should "return appropriate cluster members when submitting workflows" in withCustomTestDatabase(
    execClusterTestData
  ) { dataSource: SlickDataSource =>
    // we constructed our test cluster with 5 instances; see instanceMap above
    // when submitting workflows, we determine which instance we'll submit to by reading the first workflow's timestamp.
    // therefore, we replicate some of the "calculate target index properly with 5 targets" unit test above.
    Seq(
      11119 -> "instance0",
      11120 -> "instance1",
      11159 -> "instance2",
      11160 -> "instance3",
      11180 -> "instance4"
    ) map { case (seed, expectedInstanceId) =>
      val batch = batchTestWorkflows(seed)
      val submissionResult = Await.result(
        cluster.submitWorkflows(batch, WdlSource("wdl"), Seq.fill(batch.size)("inputs"), None, None, None, userInfo),
        Duration.Inf
      )
      assertResult(batch.size, s"size for seed $seed")(submissionResult._2.size)
      assertResult(ExecutionServiceId(expectedInstanceId), expectedInstanceId)(submissionResult._1)

      val submittedRecordsQuery = dataSource.inTransaction { dataAccess =>
        workflowQuery.findWorkflowByIds(batch map (_.id)).result
      }
      val submittedRecords = Await.result(submittedRecordsQuery, Duration.Inf)
      assert(submittedRecords.forall(_.executionServiceKey == Option(expectedInstanceId)))
    }
  }

  it should "return execution service version" in withCustomTestDatabase(execClusterTestData) {
    dataSource: SlickDataSource =>
      val version = Await.result(cluster.version, Duration.Inf)
      assertResult("25") {
        version.cromwell
      }
  }

  it should "parse SubWorkflow IDs from metadata" in {
    val metadataJson =
      """
        |{
        |  "some_key_we_dont_care_about": "a_value",
        |  "calls": {
        |    "calls_have_names_but_we_ignore_them": [
        |      {
        |        "shard": 1,
        |        "subWorkflowId": "sub1",
        |        "another_call_key_to_ignore": { "with": "an_object_value" }
        |      },
        |      {
        |        "shard": 2,
        |        "subWorkflowId": "sub2",
        |        "another_call_key_to_ignore": { "with": "an_object_value" }
        |      }
        |    ],
        |    "another_call": [
        |      {
        |        "a_call_key_to_ignore": 5,
        |        "subWorkflowId": "sub3",
        |        "another_call_key_to_ignore": { "with": "an_object_value" }
        |      }
        |    ]
        |  }
        |}
      """.stripMargin

    val ids = cluster invokePrivate parseSubWorkflowIdsFromMetadata(JsonParser(metadataJson).asJsObject())
    assertSameElements(ids, Seq("sub1", "sub2", "sub3"))
  }

  it should "gracefully handle parsing metadata with no SubWorkflows" in {
    val noSubsJson =
      """
        |{
        |  "calls": {
        |    "a_call": [ { } ]
        |  }
        |}
      """.stripMargin

    val emptyJson = "{}"

    assert((cluster invokePrivate parseSubWorkflowIdsFromMetadata(JsonParser(noSubsJson).asJsObject())).isEmpty)
    assert((cluster invokePrivate parseSubWorkflowIdsFromMetadata(JsonParser(emptyJson).asJsObject())).isEmpty)
  }

  private def batchTestWorkflows(seed: Long) = {
    val leadWorkflow = testWorkflowRecord.copy(statusLastChangedDate = new Timestamp(seed))

    val workflowsToSubmit: Seq[WorkflowRecord] = (30 to 40 + new Random().nextInt(20)) map { idx =>
      testWorkflowRecord.copy(statusLastChangedDate = new Timestamp(seed + idx))
    }

    Seq(leadWorkflow) ++ workflowsToSubmit
  }

  // Note: we do not test the remaining facade methods in ShardedHttpExecutionCluster, because they are so thin

}
