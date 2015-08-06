package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.RawlsException
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, WorkflowFailure, Submission, Workflow}
import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * @author tsharpe
 */

class GraphWorkflowDAO extends WorkflowDAO with GraphDAO {

  /** get a workflow by workspace and workflowId */
  override def get(workspaceName: WorkspaceName, workflowId: String, txn: RawlsTransaction): Option[Workflow] =
    txn withGraph { db =>
      getSinglePipelineResult(getPipeline(db,workspaceName.namespace,workspaceName.name,workflowId)) map { loadFromVertex[Workflow](_, Option(workspaceName)) }
    }

  /** update a workflow */
  override def update(workspaceName: WorkspaceName, workflow: Workflow, txn: RawlsTransaction): Workflow =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceName.namespace,workspaceName.name,workflow.workflowId))
      if ( vertex.isDefined ) saveToVertex(workflow, vertex.get, db, workspaceName)
      else throw new RawlsException(s"workflow does not exist: ${workflow}")
      workflow
    }

  /** delete a workflow */
  override def delete(workspaceName: WorkspaceName, workflowId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceName.namespace,workspaceName.name,workflowId))
      if ( vertex.isDefined ) vertex.get.remove
      vertex.isDefined
    }

  private def getPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, workflowId: String) =
    workspacePipeline(db, workspaceNamespace, workspaceName).out(workflowEdge).filter(hasPropertyValue("workflowId",workflowId))
}

class GraphSubmissionDAO(graphWorkflowDAO: GraphWorkflowDAO) extends SubmissionDAO with GraphDAO {

  /** get a submission by workspace and submissionId */
  override def get(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Option[Submission] =
    txn withGraph { db =>
      getSinglePipelineResult[Vertex](getPipeline(db,workspaceNamespace,workspaceName,submissionId)) map { fromVertex(workspaceNamespace,workspaceName,_) }
    }

  /** list all submissions in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Submission] =
    txn withGraph { db =>
      workspacePipeline(db, workspaceNamespace, workspaceName).out(submissionEdge).transform[Submission] { vertex: Vertex => fromVertex(workspaceNamespace,workspaceName,vertex) }.iterator
    }

  /** create a submission (and its workflows) */
  override def save(workspaceNamespace: String, workspaceName: String, submission: Submission, txn: RawlsTransaction) =
    txn withGraph { db =>
      val workspaceVertex = getWorkspaceVertex(db, workspaceNamespace, workspaceName).getOrElse(throw new IllegalArgumentException(s"workspace ${workspaceNamespace}/${workspaceName} does not exist"))
      val submissionVertex = saveToVertex(submission, addVertex(db, VertexSchema.Submission), db, WorkspaceName(workspaceNamespace, workspaceName))
      addEdge(workspaceVertex, submissionEdge, submissionVertex)
    }

  override def update(submission: Submission, txn: RawlsTransaction): Unit = {
    txn withGraph { db =>
      getSinglePipelineResult[Vertex](getPipeline(db, submission.workspaceName.namespace, submission.workspaceName.name, submission.submissionId)) match {
        case Some(vertex) => saveToVertex(submission, vertex, db, submission.workspaceName)
        case None => throw new RawlsException("submission does not exist to be updated: " + submission)
      }
    }
  }

  /** delete a submission (and its workflows) */
  override def delete(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceNamespace,workspaceName,submissionId))
      if ( vertex.isDefined ) {
        new GremlinPipeline(vertex.get).out(workflowEdge, workflowFailureEdge).remove
        vertex.get.remove
      }
      vertex.isDefined
    }

  private def getPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, submissionId: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(submissionEdge).filter(hasPropertyValue("submissionId",submissionId))
  }

  private def fromVertex(workspaceNamespace: String, workspaceName: String, vertex: Vertex): Submission = {
    loadFromVertex[Submission](vertex, Option(WorkspaceName(workspaceNamespace, workspaceName)))
  }
}
