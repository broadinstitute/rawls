package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model.Workflow

/**
  * Created by davidan on 6/14/16.
  */
// TODO: keys for the map of members should probably be strings
class ExecutionServiceCluster (members: Map[Int,ExecutionServiceDAO]) {


  // TODO: implement
  // inspect an already-submitted workflow to determine which cromwell instance
  // it landed on, and return that instance.
  def getMember(wf: Workflow) = members.values.head
  def getMember(wr: WorkflowRecord) = members.values.head



  // TODO: implement, potentially take different arguments
  // used for routing unsubmitted workflows to a cromwell instance.
  // this is where we implement the routing algorithm, whether that be
  // round-robin, random, etc.
  def nextAvailableMember(workflowRecs: Seq[WorkflowRecord]) = members.values.head



}
