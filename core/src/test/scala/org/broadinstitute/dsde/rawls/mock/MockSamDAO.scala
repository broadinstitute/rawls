package org.broadinstitute.dsde.rawls.mock

import akka.http.scaladsl.model.DateTime
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.{ManagedGroupAccessResponse, ManagedRoles, RawlsUserEmail, SubsystemStatus, SyncReportItem, UserIdInfo, UserInfo, UserStatus}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MockSamDAO extends SamDAO {

  case class MockSamPolicy(resourceTypeName: String, resourceId: String, policyName: String, actions: Set[String], roles: Set[String], members: Set[String])

  private val resources: TrieMap[String, Set[String]] = TrieMap()
  //key: policy key string (resourceTypeName/resourceId/policyName), value: policy
  private val policies: TrieMap[String, MockSamPolicy] = TrieMap()



  private def resourceKey(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String) = s"${resourceTypeName.value}/$resourceId"
  private def policyKey(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String) = s"${resourceTypeName.value}/$resourceId/$policyName"


  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = ???

  override def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]] = ???

  override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[Option[UserIdInfo]] = ???

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = ???

  override def createResource(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    resources.put(resourceKey(resourceTypeName, resourceId), Set(policyKey(resourceTypeName, resourceId, "owner")))
    policies.put(policyKey(resourceTypeName, resourceId, "owner"), MockSamPolicy(resourceTypeName.value, resourceId, "owner", Set.empty, Set("owner"), Set(userInfo.userEmail.value)))
    Future.successful(())
  }

  override def createResourceFull(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policiez: Map[String, SamPolicy], authDomain: Set[String], userInfo: UserInfo): Future[Unit] = {
    resources.put(resourceKey(resourceTypeName, resourceId), policiez.map(p => policyKey(resourceTypeName, resourceId, p._1)).toSet)
    policiez.map{case (policyName, policy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, policy.actions, policy.roles, policy.memberEmails))}
    Future.successful(())
  }

  override def deleteResource(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def userHasAction(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, action: SamResourceActions.SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
    Future.successful(true)
  }

  override def getPolicy(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicy] = {
    val policy = policies(policyKey(resourceTypeName, resourceId, policyName))
    Future.successful(SamPolicy(policy.members, policy.actions, policy.roles))
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Unit] = {
    println(policies)
    policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
      case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, policy.memberEmails))
      case None => policies.putIfAbsent(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, policy.actions, policy.roles, policy.memberEmails))
    }
    Future.successful(())
  }

  override def overwritePolicyMembership(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String, memberList: Set[WorkbenchEmail], userInfo: UserInfo): Future[Unit] = {
    policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
      case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, memberList.map(_.value)))
      case None => throw new Exception(s"policy $policyName does not exist")
    }
    Future.successful(())
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
      case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, existingPolicy.members ++ Set(memberEmail)))
      case None => throw new Exception(s"policy $policyName does not exist")
    }
    Future.successful(())
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
      case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, existingPolicy.members -- Set(memberEmail)))
      case None => throw new Exception(s"policy $policyName does not exist")
    }
    Future.successful(())
  }

  override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] = {
    Future.successful(Map(WorkbenchEmail(policyName) -> Seq.empty))
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    Future.successful(Set.empty)
  }

  override def getResourcePolicies(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    Future.successful(Set.empty)
  }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
    val policyNames = resources.getOrElse(resourceKey(resourceTypeName, resourceId), Set.empty)
    val loadedPolicies = policyNames.map(policies.get).map(_.get).map(x => SamPolicyWithNameAndEmail(x.policyName, SamPolicy(x.members, x.actions, x.roles), x.policyName))

    Future.successful(loadedPolicies)
  }

  override def listUserPoliciesForResource(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    Future.successful(Set.empty)
  }

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[String]] = {
    Future.successful(Set.empty)
  }

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicySyncStatus] = {
    Future.successful(SamPolicySyncStatus(DateTime.now.toString(), policyName))
  }

  override def createGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def deleteGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def listGroupPolicyEmails(groupName: WorkbenchGroupName, policyName: ManagedRoles.ManagedRole, userInfo: UserInfo): Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty)
  }

  override def getGroupEmail(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[WorkbenchEmail] = ???

  override def listManagedGroups(userInfo: UserInfo): Future[List[ManagedGroupAccessResponse]] = {
    Future.successful(List.empty)
  }

  override def addUserToManagedGroup(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmail: WorkbenchEmail, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def removeUserFromManagedGroup(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmail: WorkbenchEmail, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def overwriteManagedGroupMembership(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmails: Seq[WorkbenchEmail], userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def requestAccessToManagedGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  /**
    * @return a json blob
    */
  override def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String] = ???

  override def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String] = ???

  override def getStatus(): Future[SubsystemStatus] = ???
}