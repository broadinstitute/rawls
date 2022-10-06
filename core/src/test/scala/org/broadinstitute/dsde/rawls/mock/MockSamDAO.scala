package org.broadinstitute.dsde.rawls.mock

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName, WorkbenchUserId}

import java.util.concurrent.ConcurrentLinkedDeque
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class MockSamDAO(dataSource: SlickDataSource)(implicit executionContext: ExecutionContext) extends SamDAO {
  import dataSource.dataAccess.{rawlsBillingProjectQuery, workspaceQuery, RawlsBillingProjectExtensions}

  override def registerUser(ctx: RawlsRequestContext): Future[Option[RawlsUser]] = ???

  override def getUserStatus(ctx: RawlsRequestContext): Future[Option[SamUserStatusResponse]] =
    Future.successful(
      Option(SamUserStatusResponse(ctx.userInfo.userSubjectId.value, ctx.userInfo.userEmail.value, enabled = true))
    )

  override def getUserIdInfo(userEmail: String, ctx: RawlsRequestContext): Future[SamDAO.GetUserIdInfoResult] =
    Future.successful(
      SamDAO.User(UserIdInfo(ctx.userInfo.userSubjectId.value, userEmail, Option(ctx.userInfo.userSubjectId.value)))
    )

  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Unit] = Future.successful(())

  override def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], ctx: RawlsRequestContext, parent: Option[SamFullyQualifiedResourceId]): Future[SamCreateResourceResponse] =
    Future.successful(
      SamCreateResourceResponse(
        resourceTypeName.value,
        resourceId,
        authDomain,
        policies.keys
          .map(policyName =>
            SamCreateResourcePolicyResponse(
              SamCreateResourceAccessPolicyIdResponse(
                policyName.value,
                SamFullyQualifiedResourceId(resourceId, resourceTypeName.value)
              ),
              "fake-email@testing.org"
            )
          )
          .toSet
      )
    )

  override def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Unit] = Future.successful(())

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, cts: RawlsRequestContext): Future[Boolean] = Future.successful(true)

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, ctx: RawlsRequestContext): Future[SamPolicy] = Future.successful(SamPolicy(Set.empty, Set.empty, Set.empty))

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, ctx: RawlsRequestContext): Future[Unit] = Future.successful(())

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, ctx: RawlsRequestContext): Future[Unit] = Future.successful(())

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, ctx: RawlsRequestContext): Future[Unit] = Future.successful(())

  override def inviteUser(userEmail: String, ctx: RawlsRequestContext): Future[Unit] = ???

  override def getUserIdInfoForEmail(userEmail: WorkbenchEmail): Future[UserIdInfo] =
    Future.successful(UserIdInfo("111111111111111", "user@email.example", None))

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName,
                                  resourceId: String,
                                  policyName: SamResourcePolicyName
  ): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] =
    Future.successful(Map(WorkbenchEmail("foo@bar.com") -> Seq.empty))

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName,
                                  userInfo: UserInfo
  ): Future[Set[SamResourceIdWithPolicyName]] =
    resourceTypeName match {
      case SamResourceTypeNames.workspace =>
        dataSource
          .inTransaction(_ => workspaceQuery.listAll())
          .map(
            _.map(workspace =>
              SamResourceIdWithPolicyName(workspace.workspaceId,
                                          SamWorkspacePolicyNames.owner,
                                          Set.empty,
                                          Set.empty,
                                          false
              )
            ).toSet
          )

      case SamResourceTypeNames.billingProject =>
        dataSource
          .inTransaction(_ => rawlsBillingProjectQuery.read)
          .map(
            _.map(project =>
              SamResourceIdWithPolicyName(project.projectName.value,
                                          SamBillingProjectPolicyNames.owner,
                                          Set.empty,
                                          Set.empty,
                                          false
              )
            ).toSet
          )

      case _ => Future.successful(Set.empty)
    }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Set[SamPolicyWithNameAndEmail]] = Future.successful(resourceTypeName match {
    case SamResourceTypeNames.workspace =>
      Set(
        SamWorkspacePolicyNames.projectOwner,
        SamWorkspacePolicyNames.owner,
        SamWorkspacePolicyNames.shareReader,
        SamWorkspacePolicyNames.shareWriter,
        SamWorkspacePolicyNames.canCatalog,
        SamWorkspacePolicyNames.canCompute,
        SamWorkspacePolicyNames.reader,
        SamWorkspacePolicyNames.writer
      ).map(policyName =>
        SamPolicyWithNameAndEmail(policyName,
                                  SamPolicy(Set.empty, Set.empty, Set.empty),
                                  WorkbenchEmail(policyName.value + "@example.com")
        )
      )

    case SamResourceTypeNames.billingProject =>
      Set(SamBillingProjectPolicyNames.canComputeUser,
          SamBillingProjectPolicyNames.owner,
          SamBillingProjectPolicyNames.workspaceCreator
      ).map(policyName =>
        SamPolicyWithNameAndEmail(policyName,
                                  SamPolicy(Set.empty, Set.empty, Set.empty),
                                  WorkbenchEmail(policyName.value + "@example.com")
        )
      )

    case _ => Set.empty
  })

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Set[SamResourceRole]] = Future.successful(Set(SamWorkspaceRoles.owner))

  override def listUserActionsForResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Set[SamResourceAction]] = Future.successful(Set(SamBillingProjectActions.readSpendReport))

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, ctx: RawlsRequestContext): Future[SamPolicySyncStatus] = Future.successful(SamPolicySyncStatus("", WorkbenchEmail("foo@bar.com")))

  override def getResourceAuthDomain(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Seq[String]] = Future.successful(Seq.empty)

  override def getPetServiceAccountKeyForUser(googleProject: GoogleProjectId,
                                              userEmail: RawlsUserEmail
  ): Future[String] = Future.successful(
    """{"client_email": "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com", "client_id": "104493171545941951815"}"""
  )

  override def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String] = Future.successful(
    """{"client_email": "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com", "client_id": "104493171545941951815"}"""
  )

  override def deleteUserPetServiceAccount(googleProject: GoogleProjectId, userInfo: UserInfo): Future[Unit] =
    Future.unit

  override def getStatus(): Future[SubsystemStatus] = Future.successful(SubsystemStatus(true, None))

  override def listAllResourceMemberIds(resourceTypeName: SamResourceTypeName,
                                        resourceId: String,
                                        userInfo: UserInfo
  ): Future[Set[UserIdInfo]] = Future.successful(Set.empty)

  override def getAccessInstructions(groupName: WorkbenchGroupName, ctx: RawlsRequestContext): Future[Option[String]] = ???

  override def listResourceChildren(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Seq[SamFullyQualifiedResourceId]] = Future.successful(Seq.empty)

  override def listUserResources(resourceTypeName: SamResourceTypeName, ctx: RawlsRequestContext): Future[Seq[SamUserResource]] = ???

  override def admin: SamAdminDAO = new MockSamAdminDAO()

  class MockSamAdminDAO extends SamAdminDAO {
    override def listPolicies(resourceType: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Set[SamPolicyWithNameAndEmail]] =
      MockSamDAO.this.listPoliciesForResource(resourceType, resourceId, ctx)

    override def addUserToPolicy(resourceTypeName: SamResourceTypeName,
                                 resourceId: String,
                                 policyName: SamResourcePolicyName,
                                 memberEmail: String,
                                 ctx: RawlsRequestContext
    ): Future[Unit] =
      MockSamDAO.this.addUserToPolicy(resourceTypeName, resourceId, policyName, memberEmail, ctx)

    override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName,
                                      resourceId: String,
                                      policyName: SamResourcePolicyName,
                                      memberEmail: String,
                                      ctx: RawlsRequestContext
    ): Future[Unit] =
      MockSamDAO.this.removeUserFromPolicy(resourceTypeName, resourceId, policyName, memberEmail, ctx)
  }
}

class CustomizableMockSamDAO(dataSource: SlickDataSource)(implicit executionContext: ExecutionContext)
    extends MockSamDAO(dataSource) {
  val userEmails = new TrieMap[String, Option[String]]()
  val invitedUsers = new TrieMap[String, String]()
  val policies = new TrieMap[(SamResourceTypeName, String), TrieMap[SamResourcePolicyName, SamPolicyWithNameAndEmail]]()

  val callsToAddToPolicy = new ConcurrentLinkedDeque[(SamResourceTypeName, String, SamResourcePolicyName, String)]()
  val callsToRemoveFromPolicy =
    new ConcurrentLinkedDeque[(SamResourceTypeName, String, SamResourcePolicyName, String)]()

  override def registerUser(ctx: RawlsRequestContext): Future[Option[RawlsUser]] = {
    userEmails.put(ctx.userInfo.userEmail.value, Option(ctx.userInfo.userSubjectId.value))
    Future.successful(Option(RawlsUser(ctx.userInfo.userSubjectId, ctx.userInfo.userEmail)))
  }

  override def getUserIdInfo(userEmail: String, ctx: RawlsRequestContext): Future[SamDAO.GetUserIdInfoResult] = {
    val result = userEmails.get(userEmail).map(_.map(id => UserIdInfo(id, userEmail, Option(id))))
    Future.successful(result match {
      case Some(Some(userOrGroup)) => SamDAO.User(userOrGroup)
      case Some(None)              => SamDAO.NotUser
      case None                    => SamDAO.NotFound
    })
  }

  override def inviteUser(userEmail: String, ctx: RawlsRequestContext): Future[Unit] =
    Future.successful(invitedUsers.put(userEmail, userEmail))

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Set[SamPolicyWithNameAndEmail]] =
    policies.get((resourceTypeName, resourceId)) match {
      case Some(foundPolicies) => Future.successful(foundPolicies.values.toSet)
      case None                => super.listPoliciesForResource(resourceTypeName, resourceId, ctx)
    }

  override def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, resourcePolicies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], ctx: RawlsRequestContext, parent: Option[SamFullyQualifiedResourceId]): Future[SamCreateResourceResponse] = {
    // save each policy
    resourcePolicies.map { case (samResourcePolicyName, samPolicy) =>
      overwritePolicy(resourceTypeName, resourceId, samResourcePolicyName, samPolicy, ctx)
    }

    super.createResourceFull(resourceTypeName, resourceId, resourcePolicies, authDomain, ctx, parent)
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, ctx: RawlsRequestContext): Future[Unit] = {
    val newMap = new TrieMap[SamResourcePolicyName, SamPolicyWithNameAndEmail]()
    val mapToUpdate = policies.putIfAbsent((resourceTypeName, resourceId), newMap) match {
      case Some(oldMap) => oldMap
      case None         => newMap
    }
    mapToUpdate.put(policyName, SamPolicyWithNameAndEmail(policyName, policy, WorkbenchEmail("")))
    Future.successful(())
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, ctx: RawlsRequestContext): Future[Unit] = {
    callsToAddToPolicy.add((resourceTypeName, resourceId, policyName, memberEmail))
    Future.successful(())
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, ctx: RawlsRequestContext): Future[Unit] = {
    callsToRemoveFromPolicy.add((resourceTypeName, resourceId, policyName, memberEmail))
    Future.successful(())
  }

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, ctx: RawlsRequestContext): Future[Boolean] = {
    val pol = policies((resourceTypeName, resourceId))
    // iterate through map and find a value that contains the action and the user
    Future.successful(
      pol.exists(p =>
        p._2.policy.actions.contains(action) &&
          p._2.policy.memberEmails.contains(WorkbenchEmail(ctx.userInfo.userEmail.value))
      )
    )
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName,
                                  userInfo: UserInfo
  ): Future[Set[SamResourceIdWithPolicyName]] = {
    val policiesForType = for {
      ((typeName, resourceId), resourcePolicies) <- policies if typeName == resourceTypeName
      (policyName, policy) <- resourcePolicies
      if policy.policy.memberEmails.contains(WorkbenchEmail(userInfo.userEmail.value))
    } yield SamResourceIdWithPolicyName(resourceId, policyName, Set.empty, Set.empty, false)
    if (policiesForType.isEmpty) {
      super.getPoliciesForType(resourceTypeName, userInfo)
    } else {
      Future.successful(policiesForType.toSet)
    }
  }
}
