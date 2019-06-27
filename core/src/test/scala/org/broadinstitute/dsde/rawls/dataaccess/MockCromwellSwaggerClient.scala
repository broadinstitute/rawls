package org.broadinstitute.dsde.rawls.dataaccess

import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class MockCromwellSwaggerClient extends CromwellSwaggerClient(List("fake/path")) {

  val workflowDescriptions: mutable.Map[String, WorkflowDescription] =  new TrieMap()

  override def validate(userInfo: UserInfo, wdl: String): WorkflowDescription = {
    workflowDescriptions(wdl)
  }

}
