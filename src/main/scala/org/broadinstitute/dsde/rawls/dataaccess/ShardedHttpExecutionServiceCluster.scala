package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ShardedHttpExecutionServiceCluster (members: Map[ExecutionServiceId,ExecutionServiceDAO]) extends ExecutionServiceCluster {

  // make a copy of the members map as an array for easy reads
  private val memberArray:Array[ClusterMember] = (members map {case (id, dao) => ClusterMember(id, dao)}).toArray



  // ====================
  // facade methods
  // ====================

  // only used in tests??? TODO: DA update tests to use the same thing runtime does, or remove this.
  def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): Future[ExecutionServiceStatus] =
    members.values.head.submitWorkflow(wdl, inputs, options, userInfo)

  // by nature, this is only called for workflows that have not yet been submitted.
  // therefore, we want to send the workflows to the cromwell instance chosen
  // by the routing algorithm defined in this class.
  def submitWorkflows(workflowRecs: Seq[WorkflowRecord], wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[(ExecutionServiceId, Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]])] = {
    val targetMember = nextAvailableMember(workflowRecs)
    targetMember.dao.submitWorkflows(wdl, inputs, options, userInfo) map {results =>
      (targetMember.key, results)
    }
  }

  // following are called on a workflow that has already been submitted.
  // therefore, we want to use the cromwell instance that has been persisted
  // onto that workflow.
  def status(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionServiceStatus] =
    getMember(wfe).status(wfe.id, userInfo)

  def callLevelMetadata(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionMetadata] =
    getMember(wfe).callLevelMetadata(wfe.id, userInfo)

  def outputs(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionServiceOutputs] =
    getMember(wfe).outputs(wfe.id, userInfo)

  def logs(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionServiceLogs] =
    getMember(wfe).logs(wfe.id, userInfo)

  def abort(wfe: WorkflowExecution, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] =
    getMember(wfe).abort(wfe.id, userInfo)


  // ====================
  // facade-to-cluster entry points
  // ====================
  // for an already-submitted workflow, get the instance to which it was submitted
  private def getMember(wfe: WorkflowExecution):ExecutionServiceDAO = getMember(wfe.executionServiceId)

  // for unsubmitted workflows, get the best instance to which we should submit
  // we expect that all workflowRecs passed to this method will be a single batch;
  // we return one target cromwell instance for the entire batch.
  private def nextAvailableMember(workflowRecs: Seq[WorkflowRecord]): ClusterMember = {
    // inspect the first workflow in the batch, retrieve a long value of its last-updated timestamp
    // (could also use workflowId or some other number)
    val shardingSeed = workflowRecs.head.statusLastChangedDate.getTime
    val shardIndex = targetIndex(shardingSeed, members.size)

    memberArray(shardIndex)
  }



  // ====================
  // clustering methods
  // ====================
  private def getMember(strKey: String): ExecutionServiceDAO = getMember(ExecutionServiceId(strKey))
  private def getMember(key: ExecutionServiceId): ExecutionServiceDAO = members.get(key).orElse(throw new RawlsException(s"member with key $key does not exist")).get

  def targetIndex(seed: Long, numTargets: Int):Int = Math.ceil( (seed % 100) / (100 / numTargets) ).toInt

}

case class ClusterMember(
  key: ExecutionServiceId,
  dao: ExecutionServiceDAO
)

