package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.RawlsException
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.model.{WorkflowFailure, Submission, Workflow}
import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * @author tsharpe
 */

object ExecutionEdgeTypes {
  val submissionEdgeType = "_Submission"
  val workflowEdgeType = "_Workflow"
  val workflowFailureEdgeType = "_WorkflowFailure"
  val runOnEdgeType = "_RunOn"
}

class GraphWorkflowDAO extends WorkflowDAO with GraphDAO {

  /** get a workflow by workspace and workflowId */
  override def get(workspaceNamespace: String, workspaceName: String, workflowId: String, txn: RawlsTransaction): Option[Workflow] =
    txn withGraph { db =>
      getSinglePipelineResult(getPipeline(db,workspaceNamespace,workspaceName,workflowId)) map { fromVertex(_, workspaceNamespace, workspaceName) }
    }

  def fromVertex(workflowVertex: Vertex, workspaceNamespace: String, workspaceName: String): Workflow = {
    val entityVertex = {
      val vertexes = workflowVertex.getVertices(Direction.OUT, ExecutionEdgeTypes.runOnEdgeType).toList
      if (vertexes.size != 1) {
        throw new RawlsException(s"workflow with vertex ${workflowVertex} does not have 1 and only 1 entity")
      }
      vertexes.head
    }

    val entityProperties = Map("entityType" -> entityVertex.getProperty("_entityType"), "entityName" -> entityVertex.getProperty("_name"))

    val workspaceProperties = Map("workspaceNamespace"->workspaceNamespace,"workspaceName"->workspaceName)
    fromVertex[Workflow](workflowVertex, entityProperties ++ workspaceProperties)
  }

  /** update a workflow */
  override def update(workspaceNamespace: String, workspaceName: String, workflow: Workflow, txn: RawlsTransaction): Workflow =
    txn withGraph { db =>
      val vertex = getSinglePipelineResult(getPipeline(db,workspaceNamespace,workspaceName,workflow.id))
      if ( vertex.isDefined ) setVertexProperties(workflow, vertex.get)
      else throw new RawlsException(s"workflow does not exist: ${workflow}")
      workflow
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

class GraphSubmissionDAO(graphWorkflowDAO: GraphWorkflowDAO) extends SubmissionDAO with GraphDAO {

  /** get a submission by workspace and submissionId */
  override def get(workspaceNamespace: String, workspaceName: String, submissionId: String, txn: RawlsTransaction): Option[Submission] =
    txn withGraph { db =>
      getSinglePipelineResult[Vertex](getPipeline(db,workspaceNamespace,workspaceName,submissionId)) map { fromVertex(workspaceNamespace,workspaceName,_) }
    }

  /** list all submissions in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Submission] =
    txn withGraph { db =>
      workspacePipeline(db, workspaceNamespace, workspaceName).out(ExecutionEdgeTypes.submissionEdgeType).transform[Submission] { vertex: Vertex => fromVertex(workspaceNamespace,workspaceName,vertex) }.iterator
    }

  /** create a submission (and its workflows) */
  override def save(workspaceNamespace: String, workspaceName: String, submission: Submission, txn: RawlsTransaction) =
    txn withGraph { db =>
      val workspaceVertex = getWorkspaceVertex(db, workspaceNamespace, workspaceName).getOrElse(throw new IllegalArgumentException(s"workspace ${workspaceNamespace}/${workspaceName} does not exist"))
      val submissionVertex = setVertexProperties(submission, addVertex(db, null))
      val submissionEntityVertex =  getSinglePipelineResult(new GremlinPipeline(workspaceVertex).out(submission.entityType).filter(hasProperty("_name", submission.entityName))).
        getOrElse(throw new IllegalArgumentException(s"entity of type ${submission.entityType} named ${submission.entityName} does not exist in workspace ${workspaceNamespace}/${workspaceName}"))
      submissionVertex.addEdge(ExecutionEdgeTypes.runOnEdgeType, submissionEntityVertex)
      workspaceVertex.addEdge(ExecutionEdgeTypes.submissionEdgeType, submissionVertex)

      submission.workflows.foreach { workflow =>
        val workflowVertex = setVertexProperties(workflow, addVertex(db, null))
        val entityVertex =  getSinglePipelineResult(new GremlinPipeline(workspaceVertex).out(workflow.entityType).filter(hasProperty("_name", workflow.entityName))).
                               getOrElse(throw new IllegalArgumentException(s"entity of type ${workflow.entityType} named ${workflow.entityName} does not exist in workspace ${workspaceNamespace}/${workspaceName}"))
        workflowVertex.addEdge(ExecutionEdgeTypes.runOnEdgeType, entityVertex)
        workspaceVertex.addEdge(ExecutionEdgeTypes.workflowEdgeType, workflowVertex)
        submissionVertex.addEdge(ExecutionEdgeTypes.workflowEdgeType, workflowVertex)
      }
      submission.notstarted.foreach { failure =>
        val failureVertex = setVertexProperties(failure, db.addVertex(null))
        submissionVertex.addEdge(ExecutionEdgeTypes.workflowFailureEdgeType, failureVertex)
      }
    }

  override def update(submission: Submission, txn: RawlsTransaction): Unit = {
    txn withGraph { db =>
      getSinglePipelineResult[Vertex](getPipeline(db,submission.workspaceNamespace,submission.workspaceName,submission.id)) match {
        case Some(vertex) => setVertexProperties(submission, vertex)
        case None => throw new RawlsException("submission does not exist to be updated: " + submission)
      }
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

  private def fromVertex(workspaceNamespace: String, workspaceName: String, vertex: Vertex): Submission = {
    val workflowFails = getPropertiesOfVertices[Any](new GremlinPipeline(vertex).out(ExecutionEdgeTypes.workflowFailureEdgeType)) map { fail =>
      fromPropertyMap[WorkflowFailure](fail ++ Map("workspaceNamespace"->workspaceNamespace,"workspaceName"->workspaceName))
    }
    val entityVertex = {
      val vertexes = vertex.getVertices(Direction.OUT, ExecutionEdgeTypes.runOnEdgeType).toList
      if (vertexes.size != 1) {
        throw new RawlsException(s"submission with vertex ${vertex} does not have 1 and only 1 entity")
      }
      vertexes.head
    }
    val workflows = new GremlinPipeline(vertex).out(ExecutionEdgeTypes.workflowEdgeType).toList map { graphWorkflowDAO.fromVertex(_, workspaceNamespace, workspaceName) }
    fromVertex[Submission](vertex, Map(
      "workspaceNamespace"->workspaceNamespace,
      "workspaceName"->workspaceName,
      "workflows"->workflows.toSeq,
      "notstarted"->workflowFails.toSeq,
      "entityType" -> entityVertex.getProperty("_entityType"),
      "entityName" -> entityVertex.getProperty("_name")
    ))
  }
}
