package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.model.{SubmissionStatus,WorkflowStatus}
import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * @author tsharpe
 */

object ExecutionEdgeTypes {
  val submissionEdgeType = "_SubmissionStatus"
  val workflowEdgeType = "_Workflow"
}

class GraphWorkflowDAO extends WorkflowDAO with GraphDAO {

  /** get a workflow by workspace and workflowId */
  override def get(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Option[WorkflowStatus] =
    txn withGraph { db =>
      getVertexProperties(getPipeline(db,workspaceNamespace,workspaceName,workflowId)) map { fromPropertyMap[WorkflowStatus](_) }
    }

  /** update a workflow */
  override def update(workspaceNamespace: String, workspaceName: String, workflowStatus: WorkflowStatus, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceNamespace,workspaceName,workflowStatus.id))
      if ( vertex.isDefined ) setVertexProperties(workflowStatus, vertex.get)
      vertex.isDefined
    }

  /** delete a workflow */
  override def delete(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceNamespace,workspaceName,workflowId))
      if ( vertex.isDefined ) vertex.get.remove
      vertex.isDefined
    }

  private def getPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, workflowId: String) =
    workspacePipeline(db, workspaceNamespace, workspaceName).out(ExecutionEdgeTypes.workflowEdgeType).filter(hasProperty("_id",workflowId))
}

class GraphSubmissionDAO extends SubmissionDAO with GraphDAO {

  /** get a submission by workspace and submissionId */
  override def get(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Option[SubmissionStatus] =
    txn withGraph { db =>
      getSinglePipelineResult[Vertex](getPipeline(db,workspaceNamespace,workspaceName,submissionId)) map { fromVertex(workspaceNamespace,workspaceName,_) }
    }

  /** list all submissions in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[SubmissionStatus] =
    txn withGraph { db =>
      workspacePipeline(db, workspaceNamespace, workspaceName).out(ExecutionEdgeTypes.submissionEdgeType).transform[SubmissionStatus] { vertex: Vertex => fromVertex(workspaceNamespace,workspaceName,vertex) }.iterator
    }

  /** create a submission (and its workflows) */
  override def save(workspaceNamespace: String, workspaceName: String, submissionStatus: SubmissionStatus, txn: RawlsTransaction) =
    txn withGraph { db =>
      val workspace = getWorkspaceVertex(db, workspaceNamespace, workspaceName).getOrElse(throw new IllegalArgumentException(s"workspace ${workspaceNamespace}/${workspaceName} does not exist"))
      val submissionVertex = setVertexProperties(submissionStatus, db.addVertex(null))
      workspace.addEdge(ExecutionEdgeTypes.submissionEdgeType, submissionVertex)
      submissionStatus.workflowStatus.foreach { workflow =>
        val workflowVertex = setVertexProperties(workflow, db.addVertex(null))
        workspace.addEdge(ExecutionEdgeTypes.workflowEdgeType, workflowVertex)
        submissionVertex.addEdge(ExecutionEdgeTypes.workflowEdgeType, workflowVertex)
      }
    }

  /** delete a submission (and its workflows) */
  override def delete(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Boolean =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceNamespace,workspaceName,submissionId))
      if ( vertex.isDefined ) {
        new GremlinPipeline(vertex.get).out(ExecutionEdgeTypes.workflowEdgeType).remove
        vertex.get.remove
      }
      vertex.isDefined
    }

  private def getPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, submissionId: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(ExecutionEdgeTypes.submissionEdgeType).filter(hasProperty("_id",submissionId))
  }

  private def fromVertex(workspaceNamespace: String, workspaceName: String, vertex: Vertex): SubmissionStatus = {
    val propertiesMap = getVertexProperties[Any](new GremlinPipeline(vertex)).get
    val workflows = getPropertiesOfVertices[Any](new GremlinPipeline(vertex).out(ExecutionEdgeTypes.workflowEdgeType)) map { fromPropertyMap[WorkflowStatus](_) }
    val fullMap = propertiesMap ++ Map("workspaceNamespace"->workspaceNamespace,"workspaceName"->workspaceName,"workflowStatus"->workflows.toSeq)
    fromPropertyMap[SubmissionStatus](fullMap)
  }
}
