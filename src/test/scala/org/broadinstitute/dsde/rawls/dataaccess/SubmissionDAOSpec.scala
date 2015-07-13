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

class SubmissionDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
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



    "SubmissionDAO" should "save, get, list, and delete a submission status" in withSubmissionData { txn =>
//      dao.save(workspace.namespace,workspace.name,testData.submission1,txn)
      assertResult(Some(testData.submission1)) {
        dao.get(workspace.namespace,workspace.name,testData.submission1.id,txn) }
      assertResult(Seq(testData.submission1, testData.submission2)) {
        dao.list(workspace.namespace,workspace.name,txn).toSeq
      }
      assert(dao.delete(workspace.namespace,workspace.name,testData.submission1.id,txn))
      assertResult(None) {
        dao.get(workspace.namespace,workspace.name,testData.submission1.id,txn) }
      assertResult(1) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
    }

    it should "save, get, list, and delete two submission statuses" in withSubmissionData { txn =>
//      dao.save(workspace.namespace,workspace.name,testData.submission1,txn)
//      dao.save(workspace.namespace,workspace.name,testData.submission2,txn)
      assertResult(Some(testData.submission1)) {
        dao.get(workspace.namespace,workspace.name,testData.submission1.id,txn) }
      assertResult(Some(testData.submission2)) {
        dao.get(workspace.namespace,workspace.name,testData.submission2.id,txn) }
      assertResult(2) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
      assert(dao.delete(workspace.namespace,workspace.name,testData.submission1.id,txn))
      assertResult(1) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
      assert(dao.delete(workspace.namespace,workspace.name,testData.submission2.id,txn))
      assertResult(0) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
    }


    it should "fail to save into workspaces that don't exist" in withSubmissionData { txn =>
      assert(Try(dao.save(workspace.namespace,"noSuchThing",testData.submission1,txn)).isFailure)
    }

    it should "fail to delete submissions that don't exist" in withSubmissionData { txn =>
      assert(!dao.delete(workspace.namespace,workspace.name,"doesn't exist",txn))
    }

    it should "update submissions" in withSubmissionData { txn =>
//      dao.save(workspace.namespace,workspace.name,testData.submission1,txn)
      dao.update(testData.submission1.copy(status = SubmissionStatuses.Done), txn)
      assertResult(Option(testData.submission1.copy(status = SubmissionStatuses.Done))) {
        dao.get(workspace.namespace,workspace.name,testData.submission1.id,txn)
      }
    }

  "WorkflowDAO" should "let you dink with Workflows" in withSubmissionData { txn =>
//      dao.save(workspace.namespace,workspace.name,testData.submission1,txn)
      val workflow0 = testData.submission1.workflows(0)
      assertResult(Some(workflow0)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow0.id,txn)
      }
      val workflow1 = testData.submission1.workflows(1)
      assertResult(Some(workflow1)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow1.id,txn)
      }
      val workflow2 = testData.submission1.workflows(2)
      assertResult(Some(workflow2)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow2.id,txn)
      }
      val workflow3 = Workflow(workspace.namespace,workspace.name,workflow1.id,WorkflowStatuses.Failed,DateTime.now,workflow1.entityType,workflow1.entityName)
      workflowDAO.update(workspace.namespace,workspace.name,workflow3,txn)
      assertResult(Some(workflow3)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow3.id,txn)
      }
      assert(workflowDAO.delete(workspace.namespace,workspace.name,workflow3.id,txn))
      val submission = testData.submission1.copy(workflows=Seq(workflow0,workflow2))
      assertResult(Some(submission)) {
        dao.get(workspace.namespace,workspace.name,submission.id,txn)
      }
    }

}
