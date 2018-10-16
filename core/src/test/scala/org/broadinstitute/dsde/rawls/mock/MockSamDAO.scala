package org.broadinstitute.dsde.rawls.mock

import java.util.concurrent.ConcurrentLinkedDeque

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MockSamDAO(dataSource: SlickDataSource)(implicit executionContext: ExecutionContext) extends SamDAO {
  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = ???

  override def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]] = ???

  override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[Either[Unit, Option[UserIdInfo]]] = Future.successful(Right(Option(UserIdInfo(userInfo.userSubjectId.value, userEmail, Option(userInfo.userSubjectId.value)))))

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = ???

  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = Future.successful(())

  override def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], userInfo: UserInfo): Future[Unit] = ???

  override def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = Future.successful(())

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = Future.successful(true)

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicy] = Future.successful(SamPolicy(Set.empty, Set.empty, Set.empty))

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, userInfo: UserInfo): Future[Unit] = Future.successful(())

  override def overwritePolicyMembership(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberList: Set[WorkbenchEmail], userInfo: UserInfo): Future[Unit] = ???

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = Future.successful(())

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = Future.successful(())

  override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = ???

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] = Future.successful(Map(WorkbenchEmail("foo@bar.com") -> Seq.empty))

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    resourceTypeName match {
      case workspace =>
        dataSource.inTransaction { dataaccess =>
          dataaccess.workspaceQuery.listAll()
        }.map(_.map(workspace => SamResourceIdWithPolicyName(workspace.workspaceId, SamWorkspacePolicyNames.owner, Set.empty, Set.empty, None)).toSet)

      case billingProject =>
        dataSource.inTransaction { dataaccess =>
          dataaccess.rawlsBillingProjectQuery.listProjectsWithCreationStatus(CreationStatuses.Ready)
        }.map(_.map(project => SamResourceIdWithPolicyName(project.projectName.value, SamBillingProjectPolicyNames.owner, Set.empty, Set.empty, None)).toSet)

      case _ => Future.successful(Set.empty)
    }
  }

  override def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = Future.successful(Set(SamPolicyWithName(SamWorkspacePolicyNames.owner, SamPolicy(Set.empty, Set.empty, Set.empty))))

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = Future.successful(
    Set(SamWorkspacePolicyNames.projectOwner,
      SamWorkspacePolicyNames.owner,
      SamWorkspacePolicyNames.shareReader,
      SamWorkspacePolicyNames.shareWriter,
      SamWorkspacePolicyNames.canCatalog,
      SamWorkspacePolicyNames.canCompute,
      SamWorkspacePolicyNames.reader,
      SamWorkspacePolicyNames.writer).map( policyName =>

      SamPolicyWithNameAndEmail(policyName, SamPolicy(Set.empty, Set.empty, Set.empty), WorkbenchEmail(policyName.value + "@example.com"))
    ))

  override def listUserPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = ???

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[String]] = ???

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicySyncStatus] = ???

  override def getResourceAuthDomain(resourceTypeName: SamResourceTypeName, resourceId: String): Future[Seq[String]] = ???

  override def requestAccessToManagedGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = ???

  override def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String] = Future.successful("""{"client_email": "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com"}""")

  override def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String] = Future.successful("""{"client_email": "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com"}""")

  override def getStatus(): Future[SubsystemStatus] = Future.successful(SubsystemStatus(true, None))
}

class CustomizableMockSamDAO(dataSource: SlickDataSource)(implicit executionContext: ExecutionContext) extends MockSamDAO(dataSource) {
  val userEmails = new TrieMap[String, String]()
  val invitedUsers = new TrieMap[String, String]()
  val policies = new TrieMap[(SamResourceTypeName, String), TrieMap[SamResourcePolicyName, SamPolicyWithNameAndEmail]]()

  val callsToAddToPolicy = new ConcurrentLinkedDeque[(SamResourceTypeName, String, SamResourcePolicyName, String)]()
  val callsToRemoveFromPolicy = new ConcurrentLinkedDeque[(SamResourceTypeName, String, SamResourcePolicyName, String)]()

  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    userEmails.put(userInfo.userEmail.value, userInfo.userSubjectId.value)
    Future.successful(Option(UserStatus(RawlsUser(userInfo.userSubjectId, userInfo.userEmail), Map.empty)))
  }

  override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[Either[Unit, Option[UserIdInfo]]] = {
    val result = userEmails.get(userEmail).map { id => UserIdInfo(id, userEmail, Option(id)) }
    Future.successful(result match {
      case Some(_) => Right(result)
      case None => Left(())
    })
  }

  override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = {
    Future.successful(invitedUsers.put(userEmail, userEmail))
  }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
    policies.get((resourceTypeName, resourceId)) match {
      case Some(foundPolicies) => Future.successful(foundPolicies.values.toSet)
      case None => super.listPoliciesForResource(resourceTypeName, resourceId, userInfo)
    }
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, userInfo: UserInfo): Future[Unit] = {
    val newMap = new TrieMap[SamResourcePolicyName, SamPolicyWithNameAndEmail]()
    val mapToUpdate = policies.putIfAbsent((resourceTypeName, resourceId), newMap) match {
      case Some(oldMap) => oldMap
      case None => newMap
    }
    mapToUpdate.put(policyName, SamPolicyWithNameAndEmail(policyName, policy, WorkbenchEmail("")))
    Future.successful(())
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    callsToAddToPolicy.add((resourceTypeName, resourceId, policyName, memberEmail))
    Future.successful(())
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    callsToRemoveFromPolicy.add((resourceTypeName, resourceId, policyName, memberEmail))
    Future.successful(())
  }
}

