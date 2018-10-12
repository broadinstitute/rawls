package org.broadinstitute.dsde.rawls.mock

import akka.http.scaladsl.model.{DateTime, StatusCodes}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class MockSamDAO extends SamDAO {

  case class MockSamPolicy(resourceTypeName: String, resourceId: String, policyName: SamResourcePolicyName, actions: Set[String], roles: Set[String], members: Set[String])

  private val resources: TrieMap[String, Set[String]] = TrieMap()
  //key: policy key string (resourceTypeName/resourceId/policyName), value: policy
  private val policies: TrieMap[String, MockSamPolicy] = TrieMap()

  //key: email address, value: user id info
  private val users: TrieMap[String, Option[UserIdInfo]] = TrieMap()

  private val groups: TrieMap[String, Set[String]] = TrieMap()

  private def generateId(): String = java.util.UUID.randomUUID().toString

  val roleActions = Map(
    "billing-project/owner" -> Set("create_workspace", "alter_policies", "read_policies", "launch_batch_compute", "list_notebook_cluster", "launch_notebook_cluster", "sync_notebook_cluster", "delete_notebook_cluster", "alter_google_role"),
    "billing-project/workspace-creator" -> Set("create_workspace", "share_policy::can-compute-user", "read_policy::can-compute-user"),
    "workspace/owner" -> Set("delete", "read_policies", "share_policy::owner", "share_policy::writer", "share_policy::reader", "own", "write", "read", "compute", "share_policy::can-compute"),
    "workspace/writer" -> Set("read_policy::owner", "write", "read"),
    "workspace/reader" -> Set("read_policy::owner", "read")
  )

  private def resourceKey(resourceTypeName: SamResourceTypeName, resourceId: String) = s"${resourceTypeName.value}/$resourceId"
  private def policyKey(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName) = s"${resourceTypeName.value}/$resourceId/${policyName.value}"


  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val userSubjectId = generateId()
    users.putIfAbsent(userInfo.userEmail.value, Option(UserIdInfo(userSubjectId, userInfo.userEmail.value, Some(userInfo.userSubjectId.value))))

    Future.successful(Some(UserStatus(RawlsUser(userInfo), Map.empty)))
  }

  override def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]] = {
    Future.successful(Some(UserStatus(RawlsUser(userInfo), Map.empty)))
  }

  override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[Either[Unit, Option[UserIdInfo]]] = {
    val result = if(users.contains(userEmail)) Right(users.get(userEmail).get)
    else if(groups.contains(userEmail)) Right(None)
    else Left(())

    Future.successful(result)
  }

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = {
    Future.successful(WorkbenchEmail(s"PROXY_${userInfo.userSubjectId}@firecloud.biz"))
  }

  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    resources.put(resourceKey(resourceTypeName, resourceId), Set(policyKey(resourceTypeName, resourceId, SamResourcePolicyName("owner"))))
    policies.put(policyKey(resourceTypeName, resourceId, SamResourcePolicyName("owner")), MockSamPolicy(resourceTypeName.value, resourceId, SamWorkspacePolicyNames.owner, Set.empty, Set("owner"), Set(userInfo.userEmail.value)))
    Future.successful(())
  }

  override def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policiez: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], userInfo: UserInfo): Future[Unit] = {
    resources.put(resourceKey(resourceTypeName, resourceId), policiez.map(p => policyKey(resourceTypeName, resourceId, p._1)).toSet)
    policiez.map{case (policyName, policy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, policy.actions, policy.roles, policy.memberEmails))}
    Future.successful(())
  }

  override def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    Future.successful(())
  }

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
    if(resources.getOrElse(resourceKey(resourceTypeName, resourceId), Set.empty).isEmpty) {
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "resource not found")))
    }
    else {
      val policiesForResource = resources.get(resourceKey(resourceTypeName, resourceId))
      val policiesForUser = policiesForResource.get.filter(x => policies(x).members.contains(userInfo.userEmail.value))
      val actionsForUser = policiesForUser.flatMap(x => policies(x).actions)
      val rolesForUser = policiesForUser.flatMap(x => policies(x).roles)
      val actionsForRoles = rolesForUser.flatMap(x => roleActions.getOrElse(s"${resourceTypeName.value}/$x", Set.empty))

      Future.successful(actionsForUser.contains(action.value) || actionsForRoles.contains(action.value))
    }
  }

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicy] = {
    val policy = policies(policyKey(resourceTypeName, resourceId, policyName))
    Future.successful(SamPolicy(policy.members, policy.actions, policy.roles))
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, userInfo: UserInfo): Future[Unit] = {
    policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
      case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, policy.memberEmails))
      case None => policies.putIfAbsent(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, policy.actions, policy.roles, policy.memberEmails))
    }
    Future.successful(())
  }

  override def overwritePolicyMembership(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberList: Set[WorkbenchEmail], userInfo: UserInfo): Future[Unit] = {
    policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
      case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, memberList.map(_.value)))
      case None => throw new Exception(s"policy $policyName does not exist")
    }
    Future.successful(())
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    if(resources(resourceKey(resourceTypeName, resourceId)).isEmpty) {
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "resource not found")))
    }
    else {
      userHasAction(resourceTypeName, resourceId, SamResourceAction(s"alter_policies"), userInfo).map { alterPolicies =>
        userHasAction(resourceTypeName, resourceId, SamResourceAction(s"share_policy::$policyName"), userInfo).map { sharePolicy =>
          if (alterPolicies || sharePolicy) {
            if (!users.keys.toSeq.contains(memberEmail) && !groups.keys.toSeq.contains(memberEmail)) {
              Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"user $memberEmail is not registered")))
            }
            else {
              policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
                case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, existingPolicy.members ++ Set(memberEmail)))
                case None => throw new Exception(s"policy $policyName does not exist")
              }
              Future.successful(())
            }
          }
          else Future.failed(new Exception("You lack permission"))
        }
      }
    }
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    if (resources(resourceKey(resourceTypeName, resourceId)).isEmpty) {
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "resource not found")))
    }
    else {
      userHasAction(resourceTypeName, resourceId, SamResourceAction(s"alter_policies"), userInfo).map { alterPolicies =>
        userHasAction(resourceTypeName, resourceId, SamResourceAction(s"share_policy::$policyName"), userInfo).map { sharePolicy =>
          if (alterPolicies || sharePolicy) {
            if (!users.keys.toSeq.contains(memberEmail) && !groups.keys.toSeq.contains(memberEmail)) {
              Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"user $memberEmail is not registered")))
            }
            else {
              policies.get(policyKey(resourceTypeName, resourceId, policyName)) match {
                case Some(existingPolicy) => policies.put(policyKey(resourceTypeName, resourceId, policyName), MockSamPolicy(resourceTypeName.value, resourceId, policyName, existingPolicy.actions, existingPolicy.roles, existingPolicy.members -- Set(memberEmail)))
                case None => throw new Exception(s"policy $policyName does not exist")
              }
              Future.successful(())
            }
          }
          else Future.failed(new Exception("You lack permission"))
        }
      }
    }
  }

  override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = {
    val userSubjectId = generateId()
    users.putIfAbsent(userEmail, Option(UserIdInfo(userSubjectId, userInfo.userEmail.value, None)))

    Future.successful(())
  }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] = {
    Future.successful(Map(WorkbenchEmail(policyName.value) -> Seq.empty))
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    Future.successful(Set.empty)
  }

  override def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    val policyNames = resources.getOrElse(resourceKey(resourceTypeName, resourceId), throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
    val loadedPolicies = policyNames.map(policies.get).map(_.get).map(x => SamPolicyWithName(x.policyName, SamPolicy(x.members, x.actions, x.roles)))

    Future.successful(loadedPolicies)
  }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
    val policyNames = resources.getOrElse(resourceKey(resourceTypeName, resourceId), throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
    val loadedPolicies = policyNames.map(policies.get).map(_.get).map(x => SamPolicyWithNameAndEmail(x.policyName, SamPolicy(x.members, x.actions, x.roles), x.policyName.value))

    Future.successful(loadedPolicies)
  }

  override def listUserPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    Future.successful(Set.empty)
  }

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[String]] = {
    val policiesForResource = resources.get(resourceKey(resourceTypeName, resourceId))
    val policiesForUser = policiesForResource.get.filter(x => policies(x).members.contains(userInfo.userEmail.value))

    val rolesForUser = policiesForUser.flatMap(x => policies(x).roles)

    Future.successful(rolesForUser)
  }

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicySyncStatus] = {
    Future.successful(SamPolicySyncStatus(DateTime.now.toString(), policyName.value))
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