package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.RawlsException
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, WorkflowFailure, Submission, Workflow}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
 * @author tsharpe
 */

class GraphWorkflowDAO extends WorkflowDAO with GraphDAO {

  /** get a workflow by workspace and workflowId */
  override def get(workspaceContext: WorkspaceContext, workflowId: String, txn: RawlsTransaction): Option[Workflow] =
    txn withGraph { db =>
      getWorkflowVertex(workspaceContext, workflowId) map { loadFromVertex[Workflow](_, Some(workspaceContext.workspaceName)) }
    }

  /** update a workflow */
  override def update(workspaceContext: WorkspaceContext, workflow: Workflow, txn: RawlsTransaction): Workflow =
    txn withGraph { db =>
      getWorkflowVertex(workspaceContext, workflow.workflowId) match {
        case Some(vertex) => saveToVertex[Workflow](db, workspaceContext, workflow, vertex)
        case None => throw new RawlsException(s"workflow does not exist: ${workflow}")
      }
      workflow
    }

  /** delete a workflow */
  override def delete(workspaceContext: WorkspaceContext, workflowId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      getWorkflowVertex(workspaceContext, workflowId) match {
        case Some(vertex) => {
          vertex.remove
          true
        }
        case None => false
      }
    }
}

class GraphSubmissionDAO(graphWorkflowDAO: GraphWorkflowDAO) extends SubmissionDAO with GraphDAO {

  /** get a submission by workspace and submissionId */
  override def get(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction): Option[Submission] =
    txn withGraph { db =>
      getSubmissionVertex(workspaceContext, submissionId) map { fromVertex(workspaceContext, _) }
    }

  /** list all submissions in the workspace */
  override def list(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[Submission] =
    txn withGraph { db =>
      workspacePipeline(workspaceContext).out(submissionEdge).transform((v: Vertex) => fromVertex(workspaceContext, v)).toList.asScala
    }

  /** create a submission (and its workflows) */
  override def save(workspaceContext: WorkspaceContext, submission: Submission, txn: RawlsTransaction) =
    txn withGraph { db =>
      val submissionVertex = saveToVertex[Submission](db, workspaceContext, submission, addVertex(db, VertexSchema.Submission))
      addEdge(workspaceContext.workspaceVertex, submissionEdge, submissionVertex)
      submission
    }

  override def update(workspaceContext: WorkspaceContext, submission: Submission, txn: RawlsTransaction) = {
    txn withGraph { db =>
      getSubmissionVertex(workspaceContext, submission.submissionId) match {
        case Some(vertex) => saveToVertex[Submission](db, workspaceContext, submission, vertex)
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
          new GremlinPipeline(vertex).out(workflowEdge, workflowFailureEdge).remove
          vertex.remove
          true
        }
        case None => false
      }
    }

  private def fromVertex(workspaceContext: WorkspaceContext, vertex: Vertex): Submission = {
    loadFromVertex[Submission](vertex, Some(workspaceContext.workspaceName))
  }
}
