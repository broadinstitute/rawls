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
  val dao = new GraphSubmissionDAO
  val workflowDAO = new GraphWorkflowDAO(dao)
  
  def withSubmissionData(testCode: (WorkspaceContext, RawlsTransaction) => Any):Unit = {
    withDefaultTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
        withWorkspaceContext(testData.workspace, txn) { context =>
          testCode(context, txn)
        }
      }
    }
  }

  private val submission3 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, Seq(testData.sample1, testData.sample2, testData.sample3))
  private val submission4 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, Seq(testData.sample1, testData.sample2, testData.sample3))

  "GraphSubmissionDAO" should "save, get, list, and delete a submission status" in withSubmissionData { (context, txn) =>
    dao.save(context, submission3, txn)
    assertResult(Some(submission3)) {
      dao.get(context, submission3.submissionId, txn)
    }
    assert(dao.list(context, txn).toSet.contains(submission3))

    assert(dao.delete(context, submission3.submissionId, txn))
    assertResult(None) {
      dao.get(context, submission3.submissionId, txn)
    }
    assert(!dao.list(context, txn).toSet.contains(submission3))
  }

  it should "save, get, list, and delete two submission statuses" in withSubmissionData { (context, txn) =>
    dao.save(context, submission3, txn)
    dao.save(context, submission4, txn)
    assertResult(Some(submission3)) {
      dao.get(context, submission3.submissionId, txn) }
    assertResult(Some(submission4)) {
      dao.get(context, submission4.submissionId, txn) }

    assert(dao.list(context, txn).toSet.contains(submission3))
    assert(dao.list(context, txn).toSet.contains(submission4))

    assert(dao.delete(context, submission3.submissionId, txn))
    assert(dao.delete(context, submission4.submissionId, txn))

    assert(!dao.list(context, txn).toSet.contains(submission3))
    assert(!dao.list(context, txn).toSet.contains(submission4))
  }

  // TODO make this work
//  it should "fail to save into workspaces that don't exist" in withSubmissionData { (context, txn) =>
//    intercept[IllegalArgumentException]{
//      dao.save(workspace.namespace,"noSuchThing", testData.submission1, txn)
//    }
//  }

  it should "fail to delete submissions that don't exist" in withSubmissionData { (context, txn) =>
    assert(!dao.delete(context,"doesn't exist", txn))
  }

  it should "update submissions" in withSubmissionData { (context, txn) =>
    dao.update(context, testData.submission1.copy(status = SubmissionStatuses.Done), txn)
    assertResult(Option(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      dao.get(context, testData.submission1.submissionId, txn)
    }
  }

  "GraphWorkflowDAO" should "let you modify Workflows within a submission" in withSubmissionData { (context, txn) =>
      val workflow0 = testData.submission1.workflows(0)
      assertResult(Some(workflow0)) {
        workflowDAO.get(context, testData.submission1.submissionId, workflow0.workflowId, txn)
      }
      val workflow1 = testData.submission1.workflows(1)
      assertResult(Some(workflow1)) {
        workflowDAO.get(context, testData.submission1.submissionId, workflow1.workflowId, txn)
      }
      val workflow2 = testData.submission1.workflows(2)
      assertResult(Some(workflow2)) {
        workflowDAO.get(context, testData.submission1.submissionId, workflow2.workflowId, txn)
      }
      val workflow3 = Workflow(workflow1.workflowId, WorkflowStatuses.Failed, DateTime.now, workflow1.workflowEntity)
      workflowDAO.update(context, testData.submission1.submissionId, workflow3, txn)
      assertResult(Some(workflow3)) {
        workflowDAO.get(context, testData.submission1.submissionId, workflow3.workflowId, txn)
      }
      assert(workflowDAO.delete(context, testData.submission1.submissionId, workflow3.workflowId, txn))
      val submission = testData.submission1.copy(workflows=Seq(workflow0, workflow2))
      assertResult(Some(submission)) {
        dao.get(context, submission.submissionId, txn)
      }
    }
}
