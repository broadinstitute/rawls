package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.immutable.HashMap
import scala.util.Try

/**
 * @author tsharpe
 */

class GraphSubmissionDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  val workspace = testData.workspace
  val workflowDAO = new GraphWorkflowDAO
  val dao: SubmissionDAO = new GraphSubmissionDAO(workflowDAO)
  
  def withSubmissionData(testCode:RawlsTransaction => Any):Unit = {
    withDefaultTestDatabase { dataSource =>
      dataSource.inTransaction { txn =>
        testCode(txn)
      }
    }
  }

  private val submission3 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, Seq(testData.sample1, testData.sample2, testData.sample3))
  private val submission4 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, Seq(testData.sample1, testData.sample2, testData.sample3))


  "GraphSubmissionDAO" should "save, get, list, and delete a submission status" in withSubmissionData { txn =>
    dao.save(workspace.namespace,workspace.name,submission3,txn)
    assertResult(Some(submission3)) {
      dao.get(workspace.namespace,workspace.name,submission3.submissionId,txn)
    }
    assert(dao.list(workspace.namespace,workspace.name,txn).toSet.contains(submission3))

    assert(dao.delete(workspace.namespace,workspace.name,submission3.submissionId,txn))
    assertResult(None) {
      dao.get(workspace.namespace,workspace.name,submission3.submissionId,txn)
    }
    assert(!dao.list(workspace.namespace,workspace.name,txn).toSet.contains(submission3))
  }

  it should "save, get, list, and delete two submission statuses" in withSubmissionData { txn =>
    dao.save(workspace.namespace,workspace.name,submission3,txn)
    dao.save(workspace.namespace,workspace.name,submission4,txn)
    assertResult(Some(submission3)) {
      dao.get(workspace.namespace,workspace.name,submission3.submissionId,txn) }
    assertResult(Some(submission4)) {
      dao.get(workspace.namespace,workspace.name,submission4.submissionId,txn) }

    assert(dao.list(workspace.namespace,workspace.name,txn).toSet.contains(submission3))
    assert(dao.list(workspace.namespace,workspace.name,txn).toSet.contains(submission4))

    assert(dao.delete(workspace.namespace,workspace.name,submission3.submissionId,txn))
    assert(dao.delete(workspace.namespace,workspace.name,submission4.submissionId,txn))

    assert(!dao.list(workspace.namespace,workspace.name,txn).toSet.contains(submission3))
    assert(!dao.list(workspace.namespace,workspace.name,txn).toSet.contains(submission4))
  }


  it should "fail to save into workspaces that don't exist" in withSubmissionData { txn =>
    intercept[IllegalArgumentException]{
      dao.save(workspace.namespace,"noSuchThing",testData.submission1,txn)
    }
  }

  it should "fail to delete submissions that don't exist" in withSubmissionData { txn =>
    assert(!dao.delete(workspace.namespace,workspace.name,"doesn't exist",txn))
  }

  it should "update submissions" in withSubmissionData { txn =>
//      dao.save(workspace.namespace,workspace.name,testData.submission1,txn)
    dao.update(testData.submission1.copy(status = SubmissionStatuses.Done), txn)
    assertResult(Option(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      dao.get(workspace.namespace,workspace.name,testData.submission1.submissionId,txn)
    }
  }

  "GraphWorkflowDAO" should "let you modify Workflows within a submission" in withSubmissionData { txn =>
//      dao.save(workspace.namespace,workspace.name,testData.submission1,txn)
      val workflow0 = testData.submission1.workflows(0)
      assertResult(Some(workflow0)) {
        workflowDAO.get(workspace.toWorkspaceName,workflow0.workflowId,txn)
      }
      val workflow1 = testData.submission1.workflows(1)
      assertResult(Some(workflow1)) {
        workflowDAO.get(workspace.toWorkspaceName,workflow1.workflowId,txn)
      }
      val workflow2 = testData.submission1.workflows(2)
      assertResult(Some(workflow2)) {
        workflowDAO.get(workspace.toWorkspaceName,workflow2.workflowId,txn)
      }
      val workflow3 = Workflow(workspace.toWorkspaceName,workflow1.workflowId,WorkflowStatuses.Failed,DateTime.now,workflow1.workflowEntity)
      workflowDAO.update(workspace.toWorkspaceName,workflow3,txn)
      assertResult(Some(workflow3)) {
        workflowDAO.get(workspace.toWorkspaceName,workflow3.workflowId,txn)
      }
      assert(workflowDAO.delete(workspace.toWorkspaceName,workflow3.workflowId,txn))
      val submission = testData.submission1.copy(workflows=Seq(workflow0,workflow2))
      assertResult(Some(submission)) {
        dao.get(workspace.namespace,workspace.name,submission.submissionId,txn)
      }
    }

}
