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
  val testDbName = "ExecutionDAOSpec"
  val workspace = Workspace("dsde","ws",DateTime.now,"me",Map.empty)
  val dao: SubmissionDAO = new GraphSubmissionDAO
  val workflowDAO: WorkflowDAO = new GraphWorkflowDAO

  val submissionStatus1 = Submission("submission1",testDate,workspace.namespace,workspace.name,"std","someMethod","eType",
    Seq(Workflow(workspace.namespace,workspace.name,"workflow1",WorkflowStatuses.Submitted,testDate,"entity1","entityType"),
      Workflow(workspace.namespace,workspace.name,"workflow2",WorkflowStatuses.Submitted,testDate,"entity2","entityType"),
      Workflow(workspace.namespace,workspace.name,"workflow3",WorkflowStatuses.Submitted,testDate,"entity3","entityType")), Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)
  val submissionStatus2 = Submission("submission2",testDate,workspace.namespace,workspace.name,"std","someMethod","eType",
    Seq(Workflow(workspace.namespace,workspace.name,"workflow4",WorkflowStatuses.Submitted,testDate,"entity1","entityType"),
      Workflow(workspace.namespace,workspace.name,"workflow5",WorkflowStatuses.Submitted,testDate,"entity2","entityType"),
      Workflow(workspace.namespace,workspace.name,"workflow6",WorkflowStatuses.Submitted,testDate,"entity3","entityType")), Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

  class DefaultTestData() extends TestData {
    override def save(txn:RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
    }
  }
  val submissionData = new DefaultTestData()

  def withSubmissionData(testCode:RawlsTransaction => Any):Unit = {
    withCustomTestDatabase(submissionData) { dataSource =>
      dataSource.inTransaction { txn =>
        testCode(txn)
      }
    }
  }




    "SubmissionDAO" should "save, get, list, and delete a submission status" in withSubmissionData { txn =>
      dao.save(workspace.namespace,workspace.name,submissionStatus1,txn)
      assertResult(Some(submissionStatus1)) {
        dao.get(workspace.namespace,workspace.name,submissionStatus1.id,txn) }
      assertResult(Seq(submissionStatus1)) {
        dao.list(workspace.namespace,workspace.name,txn).toSeq
      }
      assert(dao.delete(workspace.namespace,workspace.name,submissionStatus1.id,txn))
      assertResult(0) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
    }

    it should "save, get, list, and delete two submission statuses" in withSubmissionData { txn =>
      dao.save(workspace.namespace,workspace.name,submissionStatus1,txn)
      dao.save(workspace.namespace,workspace.name,submissionStatus2,txn)
      assertResult(Some(submissionStatus1)) {
        dao.get(workspace.namespace,workspace.name,submissionStatus1.id,txn) }
      assertResult(Some(submissionStatus2)) {
        dao.get(workspace.namespace,workspace.name,submissionStatus2.id,txn) }
      assertResult(2) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
      assert(dao.delete(workspace.namespace,workspace.name,submissionStatus1.id,txn))
      assertResult(1) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
      assert(dao.delete(workspace.namespace,workspace.name,submissionStatus2.id,txn))
      assertResult(0) {
        dao.list(workspace.namespace,workspace.name,txn).size
      }
    }


    it should "fail to save into workspaces that don't exist" in withSubmissionData { txn =>
      assert(Try(dao.save(workspace.namespace,"noSuchThing",submissionStatus1,txn)).isFailure)
    }

    it should "fail to delete submissions that don't exist" in withSubmissionData { txn =>
      assert(!dao.delete(workspace.namespace,workspace.name,"doesn't exist",txn))
    }

    it should "update submissions" in withSubmissionData { txn =>
      dao.save(workspace.namespace,workspace.name,submissionStatus1,txn)
      dao.update(submissionStatus1.copy(status = SubmissionStatuses.Done), txn)
      assertResult(Option(submissionStatus1.copy(status = SubmissionStatuses.Done))) {
        dao.get(workspace.namespace,workspace.name,submissionStatus1.id,txn)
      }
    }

  "WorkflowDAO" should "let you dink with Workflows" in withSubmissionData { txn =>
      dao.save(workspace.namespace,workspace.name,submissionStatus1,txn)
      val workflow0 = submissionStatus1.workflows(0)
      assertResult(Some(workflow0)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow0.id,txn)
      }
      val workflow1 = submissionStatus1.workflows(1)
      assertResult(Some(workflow1)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow1.id,txn)
      }
      val workflow2 = submissionStatus1.workflows(2)
      assertResult(Some(workflow2)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow2.id,txn)
      }
      val workflow3 = Workflow(workspace.namespace,workspace.name,workflow1.id,WorkflowStatuses.Failed,DateTime.now,workflow1.entityName,workflow1.entityType)
      workflowDAO.update(workspace.namespace,workspace.name,workflow3,txn)
      assertResult(Some(workflow3)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow3.id,txn)
      }
      assert(workflowDAO.delete(workspace.namespace,workspace.name,workflow3.id,txn))
      val submission = submissionStatus1.copy(workflows=Seq(workflow0,workflow2))
      assertResult(Some(submission)) {
        dao.get(workspace.namespace,workspace.name,submission.id,txn)
      }
    }

}
