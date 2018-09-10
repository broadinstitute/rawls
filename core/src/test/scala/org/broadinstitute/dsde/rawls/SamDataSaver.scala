package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class SamDataSaver(implicit executionContext: ExecutionContext) {

  case class MockSamPolicy(resourceTypeName: String, resourceId: String, policyName: String, actions: Set[String], roles: Set[String], members: Set[RawlsUserEmail])

  private val users: TrieMap[RawlsUserEmail, RawlsUser] = TrieMap()
  private val policies: TrieMap[String, Set[RawlsUserEmail]] = TrieMap()

  def createUser(user: RawlsUser) = Future {
    users.putIfAbsent(user.userEmail, user)
  }

  def createManagedGroup() = {
    Future.successful(())
  }

  //also need roles and actions damnit
  def createPolicy(resourceTypeName: String, resourceId: String, policyName: String, members: Set[RawlsUserEmail]) = Future {
    policies.put(s"$resourceTypeName/$resourceId/$policyName", members)
  }
}
