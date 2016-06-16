package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.Future
import scala.util.Try

class ShardedHttpExecutionServiceCluster (members: Map[Int,ExecutionServiceDAO]) extends ExecutionServiceCluster {

  // ====================
  // facade methods
  // ====================

  // only used in tests??? TODO: update tests to use the same thing runtime does, or remove this.
  def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): Future[ExecutionServiceStatus] =
    members.values.head.submitWorkflow(wdl, inputs, options, userInfo)

  // by nature, this is only called for workflows that have not yet been submitted.
  // therefore, we want to send the workflows to the cromwell instance chosen
  // by the routing algorithm defined in this class.
  def submitWorkflows[T](wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo)(wf: T): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]] =
    nextAvailableMember(wf).submitWorkflows(wdl, inputs, options, userInfo)

  // following are called on a workflow that has already been submitted.
  // therefore, we want to use the cromwell instance that has been persisted
  // onto that workflow.
  def status[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionServiceStatus] =
    getMember(wf).status(id, userInfo)

  def callLevelMetadata[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionMetadata] =
    getMember(wf).callLevelMetadata(id, userInfo)

  def outputs[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionServiceOutputs] =
    getMember(wf).outputs(id, userInfo)

  def logs[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionServiceLogs] =
    getMember(wf).logs(id, userInfo)

  def abort[T](id: String, userInfo: UserInfo)(wf: T): Future[Try[ExecutionServiceStatus]] =
    getMember(wf).abort(id, userInfo)


  // ====================
  // facade-to-cluster entry points
  // ====================
  // for an already-submitted workflow, get the instance to which it was submitted
  private def getMember[T](obj: T):ExecutionServiceDAO = obj match {
      case wf:Workflow => getMember(wf)
      case wr:WorkflowRecord => getMember(wr)
      case seq:Seq[WorkflowRecord] => getMember(seq)
      case _ => throw new RawlsException("Must be a Workflow, WorkflowRecord, or Seq[WorkflowRecord]")
    }

  // for unsubmitted workflows, get the best instance to which we should submit
  private def nextAvailableMember[T](obj: T):ExecutionServiceDAO = obj match {
      case seq:Seq[WorkflowRecord] => nextAvailableMember(seq)
      case _ => throw new RawlsException("Must be Seq[WorkflowRecord]")
    }


  // ====================
  // clustering methods
  // ====================
  // TODO: implement these!
  // inspect an already-submitted workflow to determine which cromwell instance
  // it landed on, and return that instance.
  private def getMember(wf: Workflow):ExecutionServiceDAO = members.values.head
  private def getMember(wr: WorkflowRecord):ExecutionServiceDAO = members.values.head
  private def getMember(wrs: Seq[WorkflowRecord]):ExecutionServiceDAO = members.values.head

  private def getMember(key: Int) = members.get(key).orElse(throw new RawlsException(s"member with key $key does not exist")).get


  // TODO: implement, potentially take different arguments
  // used for routing unsubmitted workflows to a cromwell instance.
  // this is where we implement the routing algorithm, whether that be
  // round-robin, random, etc.
  private def nextAvailableMember(workflowRecs: Seq[WorkflowRecord]) = members.values.head


}

