package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._
import spray.json.JsObject

import scala.collection.immutable.Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Random, Try}


class ShardedHttpExecutionServiceCluster (readMembers: Set[ClusterMember], submitMembers: Set[ClusterMember], dataSource: SlickDataSource) extends ExecutionServiceCluster {

  // make a copy of the members map as an array for easy reads; routing algorithm will return an index in this array.
  // ensure we sort the array by key for determinism/easy understanding
  private val readMemberArray:Array[ClusterMember] = readMembers.toList.sortBy(_.key.id).toArray
  private val submitMemberArray:Array[ClusterMember] = submitMembers.toList.sortBy(_.key.id).toArray

  private val readMembersById = readMembers.map(m => m.key -> m).toMap
  private val submitMembersById = submitMembers.map(m => m.key -> m).toMap

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
    getMember(workflowRec).dao.status(workflowRec.externalId.get, userInfo)

  def outputs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceOutputs] =
    getMember(workflowRec).dao.outputs(workflowRec.externalId.get, userInfo)

  def logs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceLogs] =
    getMember(workflowRec).dao.logs(workflowRec.externalId.get, userInfo)

  def abort(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] =
    // the abort operation is special, it needs to go to a specific cromwell
    getMember(workflowRec).effectiveAbortDao.abort(workflowRec.externalId.get, userInfo)

  def version: Future[ExecutionServiceVersion] =
    getRandomReadMember.dao.version

  // two possibilities here:
  //
  // (classic case) if the workflow is a top-level workflow of a submission, it has a row in the DB and an
  // association with a specific execution service shard.  executionServiceKeyOpt = Some(shard)
  //
  // if it's a subworkflow (or sub-sub-workflow, etc) it's not present in the Rawls DB and we don't know which
  // execution service shard has processed it.  executionServiceKeyOpt = None.  Query all execution service shards
  // for the workflow to learn its submission association and which shard processed it.  In practice, one shard does
  // everything except for some older workflows on shard 2.  Revisit this if that changes!

  val SUBMISSION_ID_KEY = "workbench-submission-id"

  private def parseSubWorkflowIdsFromMetadata(metadata: JsObject): Seq[String] = {

  //    Workflow metadata has this structure:
  //
  //    {
  //      "calls" : {
  //        "foo.bar" : [
  //          {
  //            "subWorkflowId" : "69581e76-2eb0-4179-99b9-958d210ebc4b", ...
  //          }, ...
  //        ], ...
  //      },  ...
  //    }


    import spray.json._
    import spray.json.DefaultJsonProtocol._

    for {
      callsObj <- metadata.getFields("calls")
      call <- callsObj.asJsObject().fields.values
      shard <- call.asInstanceOf[JsArray].elements
      id <- shard.asJsObject().getFields("subWorkflowId")
    } yield id.convertTo[String]
  }

  // parse subworkflow IDs from the parent workflow's metadata and label each of them with the Submission ID
  // so they can be found by future workflow metadata queries

  private def labelSubWorkflowsWithSubmissionId(submissionId: String, executionServiceId: ExecutionServiceId, parentWorkflowMetadata: JsObject, userInfo: UserInfo): Future[Seq[ExecutionServiceLabelResponse]] = {
    // traverse = stop on "first" failure and propagate
    Future.traverse(parseSubWorkflowIdsFromMetadata(parentWorkflowMetadata)) { subWorkflowId =>
      getMember(executionServiceId).dao.patchLabels(subWorkflowId, userInfo, Map(SUBMISSION_ID_KEY -> submissionId))
    }
  }

  private def findExecService(services: Map[ExecutionServiceId, ClusterMember], submissionId: String, workflowId: String, userInfo: UserInfo): Future[ExecutionServiceId] = {
    val idLabelMap = services.map { case (executionServiceId, member) =>
      member.dao.getLabels(workflowId, userInfo) map { labelResponse =>
        (executionServiceId, labelResponse.labels)
      }
    }

    // find: gets "first" success, ignoring failures.  We expect a single hit.
    // more than one hit shouldn't happen - noting here that we pick one arbitrarily if it does.

    Future.find(idLabelMap) { case (_, labels) =>
      labels.exists(_ == SUBMISSION_ID_KEY -> submissionId)
    } map {
      case Some((executionServiceId, _)) => executionServiceId
      case _ =>
        val errReport = ErrorReport(s"Could not find a Workflow with ID $workflowId with Submission $submissionId in any Execution Service", Option(StatusCodes.NotFound), Seq.empty, Seq.empty, None)
        throw new RawlsExceptionWithErrorReport(errReport)
    }
  }

  def callLevelMetadata(submissionId: String, workflowId: String, execId: Option[ExecutionServiceId], userInfo: UserInfo): Future[JsObject] = {
    val execIdFut = execId match {
      case Some(executionServiceId) =>
        // single-workflow or top-level work case: workflow found in DB and confirmed to be a member of this submission
        // we have the execution service key from the DB, so query the correct execution service
        Future.successful(executionServiceId)

      case _ =>
        // we don't have the execution service key because the workflow is not in the DB.  It might be a subworkflow.
        // query all execution services for Workflow labels and search for a Submission match

        // optimize for the Production Firecloud case: it's much more likely for the workflow to be in the single submitMember

        findExecService(submitMembersById, submissionId, workflowId, userInfo) recoverWith { case _ =>
          // we expect readMembers to be a superset of submitMembers so don't check those again
          findExecService(readMembersById -- submitMembersById.keys, submissionId, workflowId, userInfo)
        }
    }

    for {
      executionServiceId <- execIdFut
      metadata <- getMember(executionServiceId).dao.callLevelMetadata(workflowId, userInfo)
      _ <- labelSubWorkflowsWithSubmissionId(submissionId, executionServiceId, metadata, userInfo)
    } yield metadata
  }

  // ====================
  // facade-to-cluster entry points
  // ====================
  // for an already-submitted workflow, get the instance to which it was submitted
  private def getMember(workflowRec: WorkflowRecord): ClusterMember = {
    (workflowRec.externalId, workflowRec.executionServiceKey) match {
      case (Some(extId), Some(execKey)) => getMember(ExecutionServiceId(execKey))
      case _ => throw new RawlsException(s"can only process WorkflowRecord objects with an external id and an execution service key: ${workflowRec.toString}")
    }
  }

  private def getMember(execKey: ExecutionServiceId): ClusterMember = {
    readMembersById.getOrElse(execKey, throw new RawlsException(s"member with key ${execKey.id} does not exist"))
  }

  private def getRandomReadMember: ClusterMember = {
    readMemberArray(Random.nextInt(readMemberArray.length))
  }

  // for unsubmitted workflows, get the best instance to which we should submit
  // we expect that all workflowRecs passed to this method will be a single batch;
  // we return one target cromwell instance for the entire batch.
  private def targetMemberForSubmission(workflowRecs: Seq[WorkflowRecord]): ClusterMember = {
    // inspect the first workflow in the batch, retrieve a long value of its last-updated timestamp
    // (could also use workflowId or some other number)
    val shardingSeed = workflowRecs.head.statusLastChangedDate.getTime
    val shardIndex = targetIndex(shardingSeed, submitMembers.size)

    submitMemberArray(shardIndex)
  }

  // ====================
  // clustering methods
  // ====================
  def targetIndex(seed: Long, numTargets: Int):Int = Math.ceil( (seed % 100) / (100 / numTargets) ).toInt
}

case class ClusterMember(
  key: ExecutionServiceId,
  dao: ExecutionServiceDAO,
  abortDao: Option[ExecutionServiceDAO] = None
) {
  val effectiveAbortDao = abortDao.getOrElse(dao)
}

