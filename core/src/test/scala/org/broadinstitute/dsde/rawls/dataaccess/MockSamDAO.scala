package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{SubsystemStatus, UserInfo, UserStatus}

import scala.concurrent.Future

/**
  * Created by mbemis on 11/9/17.
  */
class MockSamDAO extends SamDAO {
  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = Future.successful(None)
  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Boolean] = Future.successful(true)
  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = Future.successful(true)
  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Boolean] = Future.successful(true)
  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Boolean] = Future.successful(true)
  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Boolean] = Future.successful(true)
  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = Future.successful(Set.empty)
  override def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicy]] = Future.successful(Set.empty)

  override def getStatus(): Future[SubsystemStatus] = Future.successful(SubsystemStatus(true, None))

}
