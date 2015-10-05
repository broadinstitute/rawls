package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Vertex
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{Submission, Workflow, ActiveSubmission}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
 * @author tsharpe
 */

//Workflows are no longer vertices directly hanging off workspaces, so this is now just a wrapper around Submission CRUD.
class GraphWorkflowDAO(graphSubmissionDAO: GraphSubmissionDAO) extends WorkflowDAO with GraphDAO {

  /** get a workflow by workspace, submissionId and workflowId */
  override def get(workspaceContext: WorkspaceContext, submissionId: String, workflowId: String, txn: RawlsTransaction): Option[Workflow] =
    txn withGraph { db =>
      graphSubmissionDAO.get(workspaceContext, submissionId, txn).flatMap { submission =>
        submission.workflows.find( wf => wf.workflowId == workflowId )
      }
    }

  /** update a workflow */
  override def update(workspaceContext: WorkspaceContext, submissionId: String, workflow: Workflow, txn: RawlsTransaction): Workflow =
    txn withGraph { db =>
      graphSubmissionDAO.get(workspaceContext, submissionId, txn).foreach { submission =>
        submission.workflows.indexWhere( wf => wf.workflowId == workflow.workflowId ) match {
          case -1 => throw new RawlsException(s"workflow does not exist: ${workflow}")
          case idx =>
            graphSubmissionDAO.update(workspaceContext, submission.copy(workflows = submission.workflows.updated(idx, workflow)), txn)
        }
      }
      workflow
    }

  /** delete a workflow */
  override def delete(workspaceContext: WorkspaceContext, submissionId: String, workflowId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      graphSubmissionDAO.get(workspaceContext, submissionId, txn).flatMap { submission =>
        submission.workflows.indexWhere( wf => wf.workflowId == workflowId ) match {
          case -1 => None
          case idx =>
            graphSubmissionDAO.save(workspaceContext, submission.copy(workflows = submission.workflows.patch(idx, Nil, 1)), txn)
            Some(submission)
        }
      }.isDefined
    }
}

class GraphSubmissionDAO extends SubmissionDAO with GraphDAO {

  /** get a submission by workspace and submissionId */
  override def get(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction): Option[Submission] =
    txn withGraph { db =>
      getSubmissionVertex(workspaceContext, submissionId) map { loadObject[Submission] }
    }

  /** list all submissions in the workspace */
  override def list(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[Submission] =
    txn withGraph { db =>
      workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(submissionEdge)).transform((v: Vertex) => fromVertex(workspaceContext, v)).toList.asScala
    }

  /** create a submission (and its workflows) */
  override def save(workspaceContext: WorkspaceContext, submission: Submission, txn: RawlsTransaction) =
    txn withGraph { db =>
      saveSubObject[Submission](submissionEdge, submission, workspaceContext.workspaceVertex, workspaceContext, db )
      submission
    }

  override def update(workspaceContext: WorkspaceContext, submission: Submission, txn: RawlsTransaction) = {
    txn withGraph { db =>
      getSubmissionVertex(workspaceContext, submission.submissionId) match {
        case Some(vertex) => saveObject[Submission](submission, vertex, workspaceContext, db)
        case None => throw new RawlsException("submission does not exist to be updated: " + submission)
      }
      submission
    }
  }

  /** delete a submission (and its workflows) */
  override def delete(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      getSubmissionVertex(workspaceContext, submissionId) match {
        case Some(vertex) => {
          removeObject(vertex, db)
          true
        }
        case None => false
      }
    }

  override def listAllActiveSubmissions(txn: RawlsTransaction): Seq[ActiveSubmission] = {
    txn withGraph { db =>
      getAllActiveSubmissions(db).map{
        case (workspaceName,submissionVertex) =>
          ActiveSubmission(workspaceName.namespace,workspaceName.name,loadObject[Submission](submissionVertex))}.toSeq
    }
  }

  private def fromVertex(workspaceContext: WorkspaceContext, vertex: Vertex): Submission = {
    loadObject[Submission](vertex)
  }
}
