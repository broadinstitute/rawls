package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._
import spray.json.JsObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Random, Try}


class ShardedHttpExecutionServiceCluster (members: Map[ExecutionServiceId,ExecutionServiceDAO], dataSource: SlickDataSource) extends ExecutionServiceCluster {

  // make a copy of the members map as an array for easy reads; routing algorithm will return an index in this array.
  // ensure we sort the array by key for determinism/easy understanding
  private val memberArray:Array[ClusterMember] = (members map {case (id, dao) => ClusterMember(id, dao)})
      .toList.sortBy(_.key.id).toArray

  // ====================
  // facade methods
  // ====================

  // by nature, this is only called for workflows that have not yet been submitted.
  // therefore, we want to send the workflows to the cromwell instance chosen
  // by the routing algorithm defined in this class.
  def submitWorkflows(workflowRecs: Seq[WorkflowRecord], wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[(ExecutionServiceId, Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]])] = {
    val targetMember = targetMemberForSubmission(workflowRecs)
    targetMember.dao.submitWorkflows(wdl, inputs, options, userInfo) map {results =>
      (targetMember.key, results)
    }
  }

  // following are called on a workflow that has already been submitted.
  // therefore, we want to use the cromwell instance that has been persisted
  // onto that workflow.
  def status(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceStatus] =
    getMember(workflowRec).status(workflowRec.externalId.get, userInfo)

  def callLevelMetadata(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[JsObject] =
    getMember(workflowRec).callLevelMetadata(workflowRec.externalId.get, userInfo)

  def outputs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceOutputs] =
    getMember(workflowRec).outputs(workflowRec.externalId.get, userInfo)

  def logs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceLogs] =
    getMember(workflowRec).logs(workflowRec.externalId.get, userInfo)

  def abort(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] =
    getMember(workflowRec).abort(workflowRec.externalId.get, userInfo)

  def version(userInfo: UserInfo): Future[ExecutionServiceVersion] =
    getRandomMember.version(userInfo)

  // ====================
  // facade-to-cluster entry points
  // ====================
  // for an already-submitted workflow, get the instance to which it was submitted
  private def getMember(workflowRec: WorkflowRecord): ExecutionServiceDAO = {
    (workflowRec.externalId, workflowRec.executionServiceKey) match {
      case (Some(extId), Some(execKey)) => {
        members.get(ExecutionServiceId(execKey)) match {
          case Some(dao) => dao
          case None => throw new RawlsException(s"member with key $execKey does not exist")
        }
      }
      case _ => throw new RawlsException(s"can only process WorkflowRecord objects with an external id and an execution service key: ${workflowRec.toString}")
    }
  }

  private def getRandomMember: ExecutionServiceDAO = {
    memberArray(Random.nextInt(memberArray.length)).dao
  }

  // for unsubmitted workflows, get the best instance to which we should submit
  // we expect that all workflowRecs passed to this method will be a single batch;
  // we return one target cromwell instance for the entire batch.
  private def targetMemberForSubmission(workflowRecs: Seq[WorkflowRecord]): ClusterMember = {
    // inspect the first workflow in the batch, retrieve a long value of its last-updated timestamp
    // (could also use workflowId or some other number)
    val shardingSeed = workflowRecs.head.statusLastChangedDate.getTime
    val shardIndex = targetIndex(shardingSeed, members.size)

    memberArray(shardIndex)
  }

  // ====================
  // clustering methods
  // ====================
  def targetIndex(seed: Long, numTargets: Int):Int = Math.ceil( (seed % 100) / (100 / numTargets) ).toInt
}

case class ClusterMember(
  key: ExecutionServiceId,
  dao: ExecutionServiceDAO
)



