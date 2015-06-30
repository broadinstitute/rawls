package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}
import scala.util.Try

/**
 * @author tsharpe
 */

class SubmissionDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
    val testDbName = "ExecutionDAOSpec"
    val now = DateTime.now
    val workspace = Workspace("dsde","ws",DateTime.now,"me",Map.empty)
    new GraphWorkspaceDAO().save(workspace, txn)

    val submissionStatus1 = Submission("submission1",now,workspace.namespace,workspace.name,"std","someMethod","eType",
        Seq(Workflow("workflow1","Submitted",now,"entity1"),
            Workflow("workflow2","Submitted",now,"entity2"),
            Workflow("workflow3","Submitted",now,"entity3")))

    val dao: SubmissionDAO = new GraphSubmissionDAO

    "SubmissionDAO" should "save, get, list, and delete a submission status" in {
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

    val submissionStatus2 = Submission("submission2",now,workspace.namespace,workspace.name,"std","someMethod","eType",
        Seq(Workflow("workflow4","Submitted",now,"entity1"),
            Workflow("workflow5","Submitted",now,"entity2"),
            Workflow("workflow6","Submitted",now,"entity3")))

    "SubmissionDAO" should "save, get, list, and delete two submission statuses" in {
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

    val workflowDAO: WorkflowDAO = new GraphWorkflowDAO

    "WorkflowDAO" should "let you dink with Workflows" in {
      dao.save(workspace.namespace,workspace.name,submissionStatus1,txn)
      val workflow0 = submissionStatus1.workflow(0)
      assertResult(Some(workflow0)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow0.id,txn)
      }
      val workflow1 = submissionStatus1.workflow(1)
      assertResult(Some(workflow1)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow1.id,txn)
      }
      val workflow2 = submissionStatus1.workflow(2)
      assertResult(Some(workflow2)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow2.id,txn)
      }
      val workflow3 = Workflow(workflow1.id,"Failed",DateTime.now,workflow1.entityName)
      assert(workflowDAO.update(workspace.namespace,workspace.name,workflow3,txn))
      assertResult(Some(workflow3)) {
        workflowDAO.get(workspace.namespace,workspace.name,workflow3.id,txn)
      }
      assert(workflowDAO.delete(workspace.namespace,workspace.name,workflow3.id,txn))
      val submission = submissionStatus1.copy(workflow=Seq(workflow0,workflow2))
      assertResult(Some(submission)) {
        dao.get(workspace.namespace,workspace.name,submission.id,txn)
      }
    }

    "SubmissionDAO" should "fail to save into workspaces that don't exist" in {
      assert(Try(dao.save(workspace.namespace,"noSuchThing",submissionStatus1,txn)).isFailure)
    }

    "SubmissionDAO" should "fail to delete submissions that don't exist" in {
      assert(!dao.delete(workspace.namespace,workspace.name,"doesn't exist",txn))
    }

}
