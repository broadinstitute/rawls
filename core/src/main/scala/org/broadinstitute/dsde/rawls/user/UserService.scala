package org.broadinstitute.dsde.rawls.user

import _root_.slick.jdbc.TransactionIsolation
import akka.actor.{Actor, Props}
import akka.pattern._
import com.google.api.client.http.HttpResponseException
import com.google.api.client.auth.oauth2.TokenResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Notifications._
import org.broadinstitute.dsde.rawls.model.ManagedRoles.ManagedRole
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.jndi.DirectorySubjectNameSupport
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import akka.http.scaladsl.model.StatusCodes
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  val allUsersGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))
  val workspaceCreatorPolicyName = "workspace-creator"
  val canComputeUserPolicyName = "can-compute-user"
  val ownerPolicyName = "owner"

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String, notificationDAO: NotificationDAO, samDAO: SamDAO, projectOwnerGrantableRoles: Seq[String])(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, gpsDAO, gpsGroupSyncTopic, notificationDAO, samDAO, projectOwnerGrantableRoles)

  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList)
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String, notificationDAO: NotificationDAO, samDAO: SamDAO, projectOwnerGrantableRoles: Seq[String])(implicit protected val executionContext: ExecutionContext) extends RoleSupport with FutureSupport with UserWiths with LazyLogging with DirectorySubjectNameSupport {

  import dataSource.dataAccess.driver.api._
  import spray.json.DefaultJsonProtocol._

  def SetRefreshToken(token: UserRefreshToken) = setRefreshToken(token)
  def GetRefreshTokenDate = getRefreshTokenDate()

  def CreateUser = createUser()

  def ListBillingProjects = listBillingProjects
  def AdminDeleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { deleteBillingProject(projectName, ownerInfo) }
  def AdminRegisterBillingProject(xfer: RawlsBillingProjectTransfer) = asFCAdmin { registerBillingProject(xfer) }
  def AdminUnregisterBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { unregisterBillingProject(projectName, ownerInfo) }

  def AddUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamResourceActions.alterPolicies) { addUserToBillingProject(projectName, projectAccessUpdate) }
  def RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamResourceActions.alterPolicies) { removeUserFromBillingProject(projectName, projectAccessUpdate) }
  def GrantGoogleRoleToUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String) = requireProjectAction(projectName, SamResourceActions.alterGoogleRole) { grantGoogleRoleToUser(projectName, targetUserEmail, role) }
  def RemoveGoogleRoleFromUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String) = requireProjectAction(projectName, SamResourceActions.alterGoogleRole) { removeGoogleRoleFromUser(projectName, targetUserEmail, role) }
  def ListBillingAccounts = listBillingAccounts()

  def SetManagedGroupAccessInstructions(groupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions) = asFCAdmin { setManagedGroupAccessInstructions(groupRef, instructions) }

  def CreateBillingProjectFull(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName) = startBillingProjectCreation(projectName, billingAccount)
  def GetBillingProjectMembers(projectName: RawlsBillingProjectName) = requireProjectAction(projectName, SamResourceActions.readPolicies) { getBillingProjectMembers(projectName) }

  def OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) = overwriteGroupMembers(groupRef, memberList)
  def AdminSynchronizeGroupMembers(groupRef: RawlsGroupRef) = asFCAdmin { synchronizeGroupMembersApi(groupRef) }
  def InternalSynchronizeGroupMembers(groupRef: RawlsGroupRef) = synchronizeGroupMembers(groupRef)

  def AdminDeleteRefreshToken(userRef: RawlsUserRef) = asFCAdmin { deleteRefreshToken(userRef) }
  def AdminDeleteAllRefreshTokens = asFCAdmin { deleteAllRefreshTokens() }

  def IsAdmin(userEmail: RawlsUserEmail) = { isAdmin(userEmail) }
  def IsLibraryCurator(userEmail: RawlsUserEmail) = { isLibraryCurator(userEmail) }
  def AdminAddLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { addLibraryCurator(userEmail) }
  def AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { removeLibraryCurator(userEmail) }

  def requireProjectAction(projectName: RawlsBillingProjectName, action: SamResourceActions.SamResourceAction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a project owner.")))
    }
  }

  def setRefreshToken(userRefreshToken: UserRefreshToken): Future[PerRequestMessage] = {
    gcsDAO.storeToken(userInfo, userRefreshToken.refreshToken).map(_ => RequestComplete(StatusCodes.Created))
  }

  def getRefreshTokenDate(): Future[PerRequestMessage] = {
    gcsDAO.getTokenDate(RawlsUser(userInfo)).map(_ match {
      case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"no refresh token stored for ${userInfo.userEmail}"))
      case Some(date) => RequestComplete(UserRefreshTokenDate(date))
    }).recover {
      case t: TokenResponseException =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(t.getStatusCode, t))
    }
  }

  //Note: As of Sam Phase I, this function only fires off the welcome email and updates pending workspace access
  //The rest of user registration now takes place in Sam
  def createUser(): Future[Unit] = {
    val user = RawlsUser(userInfo)

    val response = handleFutures(Future.sequence(Seq(toFutureTry(turnInvitesIntoRealAccess(user)))))(_ => {
      notificationDAO.fireAndForgetNotification(ActivationNotification(user.userSubjectId))
    }, handleException("Errors creating user"))

    response.map(_ => ())
  }

  def turnInvitesIntoRealAccess(user: RawlsUser) = {
    val groupRefs = dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.findWorkspaceInvitesForUser(user.userEmail).flatMap { invites =>
        DBIO.sequence(invites.map { case (workspaceName, accessLevel) =>
          dataAccess.workspaceQuery.loadAccessGroup(workspaceName, accessLevel)
        })
      }
    }

    groupRefs.flatMap { refs =>
      Future.sequence(refs.map { ref =>
        updateGroupMembers(ref, RawlsGroupMemberList(userSubjectIds = Option(Seq(user.userSubjectId.value))), RawlsGroupMemberList())
      })
    } flatMap { _ =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.deleteWorkspaceInvitesForUser(user.userEmail)
      }
    }
  }

  def getUserGroup(rawlsGroupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.loadGroupIfMember(rawlsGroupRef, RawlsUser(userInfo)) map {
        case None => RequestComplete(ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found or member not in group"))
        case Some(group) => RequestComplete(group.toRawlsGroupShort)
      }
    }
  }

  private def loadUser(userRef: RawlsUserRef): Future[RawlsUser] = dataSource.inTransaction { dataAccess => withUser(userRef, dataAccess)(DBIO.successful) }

  def listGroupsForUser(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withUser(userEmail, dataAccess) { user =>
        for {
          groups <- dataAccess.rawlsGroupQuery.listGroupsForUser(user)
        } yield {
          groups map {ref => ref.groupName.value}
        }
      }
    } map(RequestComplete(_))
  }

  private def verifyNoSubmissions(userRef: RawlsUserRef, dataAccess: DataAccess): ReadAction[Unit] = {
    dataAccess.submissionQuery.findBySubmitter(userRef.userSubjectId.value).exists.result flatMap {
      case false => DBIO.successful(())
      case _ => DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Cannot delete a user with submissions")))
    }
  }

  def isAdmin(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(tryIsFCAdmin(userEmail)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(b) => b match {
        case true => RequestComplete(StatusCodes.OK)
        case false => RequestComplete(StatusCodes.NotFound)
      }
    }
  }

  def isLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.isLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(b) => b match {
        case true => RequestComplete(StatusCodes.OK)
        case false => RequestComplete(StatusCodes.NotFound)
      }
    }
  }

  def addLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.addLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(_) => RequestComplete(StatusCodes.OK)
    }
  }

  def removeLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.removeLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(_) => RequestComplete(StatusCodes.OK)
    }
  }

  import spray.json.DefaultJsonProtocol._

  def listBillingAccounts(): Future[PerRequestMessage] =
    gcsDAO.listBillingAccounts(userInfo) map(RequestComplete(_))

  def listBillingProjects(): Future[PerRequestMessage] = {
    val membershipsFuture = for {
      resourceIdsWithPolicyNames <- samDAO.getPoliciesForType(SamResourceTypeNames.billingProject, userInfo)
      projectDetailsByName <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.getBillingProjectDetails(resourceIdsWithPolicyNames.map(idWithPolicyName => RawlsBillingProjectName(idWithPolicyName.resourceId))) }
    } yield {
      resourceIdsWithPolicyNames.collect {
        case SamResourceIdWithPolicyName(resourceId, "owner") => (resourceId, ProjectRoles.Owner)
        case SamResourceIdWithPolicyName(resourceId, "workspace-creator") => (resourceId, ProjectRoles.User)
      }.flatMap { case (resourceId, role) =>
        projectDetailsByName.get(resourceId).map { case (projectStatus, message) =>
          RawlsBillingProjectMembership(RawlsBillingProjectName(resourceId), role, projectStatus, message)
        }
      }
    }

    membershipsFuture.map(RequestComplete(_))
  }

  def getBillingProjectMembers(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    samDAO.getResourcePolicies(SamResourceTypeNames.billingProject, projectName.value, userInfo).map { policies =>
      for {
        (role, policy) <- policies.collect {
          case SamPolicyWithName("owner", policy) => (ProjectRoles.Owner, policy)
          case SamPolicyWithName("workspace-creator", policy) => (ProjectRoles.User, policy)
        }
        email <- policy.memberEmails
      } yield RawlsBillingProjectMember(RawlsUserEmail(email), role)
    }.map(RequestComplete(_))
  }

  def unregisterBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[PerRequestMessage] = {
    val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfo("newOwnerEmail")), OAuth2BearerToken(ownerInfo("newOwnerToken")), 3600, RawlsUserSubjectId("0"))
    for {
      _ <- samDAO.deleteResource(SamResourceTypeNames.billingProject, projectName.value, ownerUserInfo)
      _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(projectName) }
    } yield {
      RequestComplete(StatusCodes.NoContent)
    }
  }

  def deleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[PerRequestMessage] = {
    // unregister then delete actual project in google
    for {
      _ <- unregisterBillingProject(projectName, ownerInfo)
      _ <- gcsDAO.deleteProject(projectName)
    } yield RequestComplete(StatusCodes.NoContent)
  }

  //very sad: have to pass the new owner's token in the POST body (oh no!)
  //we could instead exploit the fact that Sam will let you create pets in projects you're not in (!!!),
  //but that seems extremely shady
  def registerBillingProject(xfer: RawlsBillingProjectTransfer): Future[PerRequestMessage] = {
    val billingProjectName = RawlsBillingProjectName(xfer.project)
    val project = RawlsBillingProject(billingProjectName, s"gs://${xfer.bucket}", CreationStatuses.Ready, None, None)
    val ownerUserInfo = UserInfo(RawlsUserEmail(xfer.newOwnerEmail), OAuth2BearerToken(xfer.newOwnerToken), 3600, RawlsUserSubjectId("0"))


    for {
      _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.create(project) }

      _ <- samDAO.createResource(SamResourceTypeNames.billingProject, billingProjectName.value, ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, workspaceCreatorPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.workspaceCreator)), ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, canComputeUserPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser)), ownerUserInfo)
      ownerGroupEmail <- samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, SamProjectRoles.owner).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
      computeUserGroupEmail <- samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, UserService.canComputeUserPolicyName).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))

      policiesToAdd = Map(
        "roles/viewer" -> List(s"group:${ownerGroupEmail.value}"),
        "roles/billing.projectManager" -> List(s"group:${ownerGroupEmail.value}"),
        "roles/genomics.pipelinesRunner" -> List(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}"))

      _ <- gcsDAO.addPolicyBindings(billingProjectName, policiesToAdd)
      _ <- gcsDAO.grantReadAccess(billingProjectName, xfer.bucket, Set(ownerGroupEmail, computeUserGroupEmail))
    } yield {
      RequestComplete(StatusCodes.Created)
    }
  }

  def addUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    val policies = projectAccessUpdate.role match {
      case ProjectRoles.Owner => Seq(ownerPolicyName)
      case ProjectRoles.User => Seq(workspaceCreatorPolicyName, canComputeUserPolicyName)
    }
    for {
      _ <- Future.traverse(policies) { policy =>
        samDAO.addUserToPolicy(SamResourceTypeNames.billingProject, projectName.value, policy, projectAccessUpdate.email, userInfo)}

      (project, addUsers, addSubGroups) <- loadMembersAndProject(projectName, projectAccessUpdate)

      _ <- Future.traverse(policies) { policy =>
        updateGroupMembership(RawlsGroupRef(RawlsGroupName(policyGroupName(SamResourceTypeNames.billingProject.value, project.projectName.value, policy))), addUsers = addUsers, addSubGroups = addSubGroups)
      }
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    val policy = projectAccessUpdate.role match {
      case ProjectRoles.Owner => ownerPolicyName
      case ProjectRoles.User => workspaceCreatorPolicyName
    }
    for {
      _ <- samDAO.removeUserFromPolicy(SamResourceTypeNames.billingProject, projectName.value, policy, projectAccessUpdate.email, userInfo).recover {
        case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode.contains(StatusCodes.BadRequest) => throw new RawlsExceptionWithErrorReport(e.errorReport.copy(statusCode = Some(StatusCodes.NotFound)))}

      (project, removeUsers, removeSubGroups) <- loadMembersAndProject(projectName, projectAccessUpdate)

      _ <- updateGroupMembership(RawlsGroupRef(RawlsGroupName(policyGroupName(SamResourceTypeNames.billingProject.value, project.projectName.value, policy))), removeUsers = removeUsers, removeSubGroups = removeSubGroups)
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  private def googleRoleWhitelistCheck(role: String): Future[Unit] = {
    if (projectOwnerGrantableRoles.contains(role))
      Future.successful(())
    else
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"Cannot alter Google role $role: not in list [${projectOwnerGrantableRoles mkString ", "}]")))
  }

  def grantGoogleRoleToUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String): Future[PerRequestMessage] = {
    for {
      _ <- googleRoleWhitelistCheck(role)
      proxyGroupEmail <- samDAO.getProxyGroup(userInfo, targetUserEmail)
      _ <- gcsDAO.addRoleToGroup(projectName, proxyGroupEmail, role)
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def removeGoogleRoleFromUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String): Future[PerRequestMessage] = {
    for {
      _ <- googleRoleWhitelistCheck(role)
      proxyGroupEmail <- samDAO.getProxyGroup(userInfo, targetUserEmail)
      _ <- gcsDAO.removeRoleFromGroup(projectName, proxyGroupEmail, role)
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def loadMembersAndProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[(RawlsBillingProject, Set[RawlsUserRef], Set[RawlsGroupRef])] = {
    dataSource.inTransaction { dataAccess =>
      for {
        (addUsers, addSubGroups) <- dataAccess.rawlsGroupQuery.loadFromEmail(projectAccessUpdate.email).map {
          case Some(Left(user)) => (Set[RawlsUserRef](user), Set.empty[RawlsGroupRef])
          case Some(Right(group)) => (Set.empty[RawlsUserRef], Set[RawlsGroupRef](group))
          case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Member ${projectAccessUpdate.email} not found"))
        }
        projectOption <- dataAccess.rawlsBillingProjectQuery.load(projectName)
      } yield {
        (projectOption.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Project ${projectName.value} not found"))), addUsers, addSubGroups)
      }
    }
  }

  def overwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList): Future[PerRequestMessage] = {
    for {
      (users, groups) <- dataSource.inTransaction({ dataAccess =>
        withGroup(groupRef, dataAccess) { group =>
          loadMemberUsersAndGroups(memberList, dataAccess)
        }
      }, TransactionIsolation.ReadCommitted) // read committed required to reduce db locks and allow concurrency
     _ <- overwriteGroupMembership(groupRef, users.map(RawlsUser.toRef), groups.map(RawlsGroup.toRef))
    } yield RequestComplete(StatusCodes.NoContent)
  }

  /** completely overwrites all group members */
  def overwriteGroupMembership(groupRef: RawlsGroupRef, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): Future[RawlsGroup] = {
    updateGroupMembershipInternal(groupRef) { group =>
      group.copy(users = users, subGroups = subGroups)
    }
  }

  def updateGroupMembership(groupRef: RawlsGroupRef, addUsers: Set[RawlsUserRef] = Set.empty, removeUsers: Set[RawlsUserRef] = Set.empty, addSubGroups: Set[RawlsGroupRef] = Set.empty, removeSubGroups: Set[RawlsGroupRef] = Set.empty): Future[RawlsGroup] = {

    val groupOrPolicy = groupRef.groupName.value match {
      case policyGroupNamePattern(_*) => updateGroupMembershipInternalPolicy(_)
      case _ => updateGroupMembershipInternal(_)
    }
    groupOrPolicy(groupRef) { group =>
      group.copy(
        users = group.users ++ addUsers -- removeUsers,
        subGroups = group.subGroups ++ addSubGroups -- removeSubGroups
      )
    }
  }

  def requestAccessToManagedGroup(groupRef: ManagedGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      val query = for {
        group <- dataAccess.managedGroupQuery.load(groupRef)
        accessInstructions <- dataAccess.managedGroupQuery.getManagedGroupAccessInstructions(Set(groupRef))
      } yield (group, accessInstructions)

      query.flatMap { case (group, accessInstructions) =>
        group match {
          case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"The group [${groupRef.membersGroupName.value}] was not found"))
          case Some(managedGroup) =>
            if (accessInstructions.nonEmpty) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "You may not request access to this group"))
            else {
              dataAccess.rawlsGroupQuery.flattenGroupMembership(managedGroup.adminsGroup).map { users =>
                users.foreach { user =>
                  notificationDAO.fireAndForgetNotification(GroupAccessRequestNotification(user.userSubjectId, groupRef.membersGroupName.value, users.map(_.userSubjectId) + userInfo.userSubjectId, userInfo.userSubjectId))
                }
                RequestComplete(StatusCodes.NoContent)
              }
            }
        }
      }
    }
  }

  def setManagedGroupAccessInstructions(managedGroupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.managedGroupQuery.setManagedGroupAccessInstructions(managedGroupRef, instructions).map {
        case 0 => RequestComplete(StatusCodes.InternalServerError, "We were unable to update the access instructions")
        case _ => RequestComplete(StatusCodes.NoContent)
      }
    }
  }

  /**
   * Internal function to update a group
   *
   * @param groupRef group to update
   * @param update function that takes the existing group as input and should produce an updated version to be saved
   * @return
   */
  private def updateGroupMembershipInternal(groupRef: RawlsGroupRef)(update: RawlsGroup => RawlsGroup): Future[RawlsGroup] = {
    for {
      (savedGroup, intersectionGroups) <- dataSource.inTransaction ({ dataAccess =>
        for {
          groupOption <- dataAccess.rawlsGroupQuery.load(groupRef)
          group = groupOption.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"group ${groupRef.groupName.value} not found")))
          updatedGroup = update(group)
          savedGroup <- dataAccess.rawlsGroupQuery.save(updatedGroup)

          // update intersection groups associated with groupRef
          groupsToIntersects <- dataAccess.workspaceQuery.findAssociatedGroupsToIntersect(savedGroup)
          intersectionGroups <- updateIntersectionGroupMembers(groupsToIntersects.toSet, dataAccess)
        } yield (savedGroup, intersectionGroups)
      }, TransactionIsolation.ReadCommitted)

      messages = (intersectionGroups.toSeq :+ RawlsGroup.toRef(savedGroup)).map(_.toJson.compactPrint)

      _ <- gpsDAO.publishMessages(gpsGroupSyncTopic, messages)
    } yield savedGroup
  }

  private def updateGroupMembershipInternalPolicy(groupRef: RawlsGroupRef)(update: RawlsGroup => RawlsGroup): Future[RawlsGroup] = {
    // note that this does not actually update the policy, sam does that. this just figures our intersections
    // updates the intersection groups and publishes messages for those updates
    for {
      (savedGroup, intersectionGroups) <- dataSource.inTransaction ({ dataAccess =>
        for {
          groupOption <- dataAccess.rawlsGroupQuery.load(groupRef)
          group = groupOption.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"group ${groupRef.groupName.value} not found")))
          updatedGroup = update(group)

          // update intersection groups associated with groupRef
          groupsToIntersects <- dataAccess.workspaceQuery.findAssociatedGroupsToIntersect(updatedGroup)
          intersectionGroups <- updateIntersectionGroupMembers(groupsToIntersects.toSet, dataAccess)
        } yield (updatedGroup, intersectionGroups)
      }, TransactionIsolation.ReadCommitted)

      messages = intersectionGroups.toSeq.map(_.toJson.compactPrint)

      _ <- gpsDAO.publishMessages(gpsGroupSyncTopic, messages)
    } yield savedGroup
  }

  private def loadMemberUsersAndGroups(memberList: RawlsGroupMemberList, dataAccess: DataAccess): ReadWriteAction[(Set[RawlsUser], Set[RawlsGroup])] = {
    val userQueriesByEmail = for {
      email <- memberList.userEmails.getOrElse(Seq.empty)
    } yield dataAccess.rawlsUserQuery.loadUserByEmail(RawlsUserEmail(email)).map((email, _))

    val userQueriesBySub = for {
      sub <- memberList.userSubjectIds.getOrElse(Seq.empty)
    } yield dataAccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(sub))).map((sub, _))

    val userQueries = DBIO.sequence(userQueriesByEmail ++ userQueriesBySub)

    val groupQueriesByEmail = for {
      email <- memberList.subGroupEmails.getOrElse(Seq.empty)
    } yield dataAccess.rawlsGroupQuery.loadGroupByEmail(RawlsGroupEmail(email)).map((email, _))

    val groupQueriesByName = for {
      name <- memberList.subGroupNames.getOrElse(Seq.empty)
    } yield dataAccess.rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(name))).map((name, _))

    val subGroupQueries = DBIO.sequence(groupQueriesByEmail ++ groupQueriesByName)

    for {
      users <- userQueries
      subGroups <- subGroupQueries
    } yield {
      (users.collect { case (email, None) => email }, subGroups.collect { case (email, None) => email }) match {
        // success case, all users and groups found
        case (Seq(), Seq()) => (users.map(_._2.get).toSet, subGroups.map(_._2.get).toSet)

        // failure cases, some users and/or groups not found
        case (Seq(), missingGroups) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some groups not found: ${missingGroups.mkString(", ")}"))
        case (missingUsers, Seq()) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}"))
        case (missingUsers, missingGroups) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}. Some groups not found: ${missingGroups.mkString(", ")}"))
      }
    }
  }

  def updateGroupMembers(groupRef: RawlsGroupRef, addMemberList: RawlsGroupMemberList = RawlsGroupMemberList(), removeMemberList: RawlsGroupMemberList = RawlsGroupMemberList()): Future[PerRequestMessage] = {
    for {
      (addUsers, addSubGroups, removeUsers, removeSubGroups) <- dataSource.inTransaction { dataAccess =>
        for {
          (addUsers, addSubGroups) <- loadMemberUsersAndGroups(addMemberList, dataAccess)
          (removeUsers, removeSubGroups) <- loadMemberUsersAndGroups(removeMemberList, dataAccess)
        } yield (addUsers, addSubGroups, removeUsers, removeSubGroups)
      }
      _ <- updateGroupMembership(groupRef,
        addUsers = addUsers.map(RawlsUser.toRef),
        addSubGroups = addSubGroups.map(RawlsGroup.toRef),
        removeUsers = removeUsers.map(RawlsUser.toRef),
        removeSubGroups = removeSubGroups.map(RawlsGroup.toRef)
      )
      
    } yield RequestComplete(StatusCodes.NoContent)
  }

  def synchronizeGroupMembersApi(groupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    synchronizeGroupMembers(groupRef) map { syncReport =>
      val statusCode = if (syncReport.items.exists(_.errorReport.isDefined)) {
        StatusCodes.BadGateway // status 500 is used for all other errors, 502 seems like the best otherwise
      } else {
        StatusCodes.OK
      }
      RequestComplete(statusCode, syncReport)
    }
  }

  def synchronizeGroupMembers(groupRef: RawlsGroupRef): Future[SyncReport] = {
    dataSource.inTransaction { dataAccess =>
      withGroup(groupRef, dataAccess) { group =>
        synchronizeGroupMembersInternal(group, dataAccess)
      }
    }
  }

  def synchronizeGroupMembersInternal(group: RawlsGroup, dataAccess: DataAccess): ReadWriteAction[SyncReport] = {
    def loadRefs(refs: Set[Either[RawlsUserRef, RawlsGroupRef]]) = {
      DBIO.sequence(refs.map {
        case Left(userRef) => dataAccess.rawlsUserQuery.load(userRef).map(userOption => Left(userOption.getOrElse(throw new RawlsException(s"user $userRef not found"))))
        case Right(groupRef) => dataAccess.rawlsGroupQuery.load(groupRef).map(groupOption => Right(groupOption.getOrElse(throw new RawlsException(s"group $groupRef not found"))))
      }.toSeq)
    }

    def toSyncReportItem(operation: String, email: String, result: Try[Unit]) = {
      SyncReportItem(
        operation,
        email,
        result match {
          case Success(_) => None
          case Failure(t) => Option(ErrorReport(t))
        }
      )
    }

    DBIO.from(gcsDAO.listGroupMembers(group)) flatMap {
      case None => DBIO.from(gcsDAO.createGoogleGroup(group) map (_ => Map.empty[String, Option[Either[RawlsUserRef, RawlsGroupRef]]]))
      case Some(members) => DBIO.successful(members)
    } flatMap { membersByEmail =>

      val knownEmailsByMember = membersByEmail.collect { case (email, Some(member)) => (member, email) }
      val unknownEmails = membersByEmail.collect { case (email, None) => email }

      val toRemove = knownEmailsByMember.keySet -- group.users.map(Left(_)) -- group.subGroups.map(Right(_))
      val emailsToRemove = unknownEmails ++ toRemove.map(knownEmailsByMember)
      val removeFutures = DBIO.sequence(emailsToRemove map { removeMember =>
        DBIO.from(toFutureTry(gcsDAO.removeEmailFromGoogleGroup(group.groupEmail.value, removeMember)).map(toSyncReportItem("removed", removeMember, _)))
      })


      val realMembers: Set[Either[RawlsUserRef, RawlsGroupRef]] = group.users.map(Left(_)) ++ group.subGroups.map(Right(_))
      val toAdd = realMembers -- knownEmailsByMember.keySet
      val addFutures = loadRefs(toAdd) flatMap { addMembers =>
        DBIO.sequence(addMembers map { addMember =>
          val memberEmail = addMember match {
            case Left(user) => user.userEmail.value
            case Right(subGroup) => subGroup.groupEmail.value
          }
          DBIO.from(toFutureTry(gcsDAO.addMemberToGoogleGroup(group, addMember)).map(toSyncReportItem("added", memberEmail, _)))
        })
      }

      for {
        syncReportItems <- DBIO.sequence(Seq(removeFutures, addFutures))
        _ <- dataAccess.rawlsGroupQuery.updateSynchronizedDate(group)
      } yield {
        SyncReport(group.groupEmail, syncReportItems.flatten)
      }
    }
  }

  def updateIntersectionGroupMembers(groupsToIntersect: Set[GroupsToIntersect], dataAccess:DataAccess): ReadWriteAction[Iterable[RawlsGroupRef]] = {

    val allGroupRefs = groupsToIntersect.flatMap(_.groups)

    dataAccess.rawlsGroupQuery.loadGroupsRecursive(allGroupRefs).flatMap { allGroups =>
      // load all the groups first because this is fast and most are likely to be empty or with no subgroups
      // only with subgroups do we need to go back to rawlsGroupQuery to do the intersection (which is expensive)
      val groupsByName = allGroups.map(g => g.groupName -> g).toMap

      // this set makes sure to query once per set of groups
      val intersectionsToMake = Set() ++ groupsToIntersect.map(_.groups)
      val intersections = DBIO.sequence(intersectionsToMake.toSeq.map { groups =>
        // this is the right thing to call when we know how to make it perform well
        // dataAccess.rawlsGroupQuery.intersectGroupMembership(groups).map(members => groups -> members)
        DBIO.successful(groups -> groups.map(g => dataAccess.rawlsGroupQuery.flattenGroup(groupsByName(g.groupName), groupsByName)).reduce(_ intersect _))
      })

      val intersectionMemberships = intersections.map { sourceGroupsWithMembers =>
        val membersBySourceGroups = sourceGroupsWithMembers.toMap
        groupsToIntersect.map { gti =>
          gti.target -> membersBySourceGroups(gti.groups)
        }.toSeq
      }

      intersectionMemberships.flatMap(dataAccess.rawlsGroupQuery.overwriteGroupUsers).map(_ => groupsToIntersect.map(_.target))
    }
  }

  def deleteRefreshToken(rawlsUserRef: RawlsUserRef): Future[PerRequestMessage] = {
    deleteRefreshTokenInternal(rawlsUserRef).map(_ => RequestComplete(StatusCodes.OK))

  }

  def deleteAllRefreshTokens(): Future[PerRequestMessage] = {
    for {
      users <- dataSource.inTransaction { _.rawlsUserQuery.loadAllUsers() }
      tries <- Future.traverse(users) { user => toFutureTry(deleteRefreshTokenInternal(user)) }
    } yield {
      val errors = tries.collect {
        case Failure(t) => ErrorReport(t)
      }
      if (errors.isEmpty) {
        RequestComplete(StatusCodes.OK)
      } else {
        RequestComplete(ErrorReport("exceptions revoking/deleting some tokens", errors))
      }
    }
  }

  def startBillingProjectCreation(projectName: RawlsBillingProjectName, billingAccountName: RawlsBillingAccountName): Future[PerRequestMessage] = {
    def createForbiddenErrorMessage(who: String, billingAccountName: RawlsBillingAccountName) = {
      s"""${who} must have the permission "Billing Account User" on ${billingAccountName.value} to create a project with it."""
    }
    gcsDAO.listBillingAccounts(userInfo) flatMap { billingAccountNames =>
      billingAccountNames.find(_.accountName == billingAccountName) match {
        case Some(billingAccount) if billingAccount.firecloudHasAccess =>
          for {
            _ <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
                case None =>
                  for {
                    _ <- DBIO.from(samDAO.createResource(SamResourceTypeNames.billingProject, projectName.value, userInfo))
                    _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, workspaceCreatorPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.workspaceCreator)), userInfo))
                    _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, canComputeUserPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser)), userInfo))
                    project <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, "gs://" + gcsDAO.getCromwellAuthBucketName(projectName), CreationStatuses.Creating, Option(billingAccountName), None))
                  } yield project

                case Some(_) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists"))
              }
            }

            createProjectOperation <- gcsDAO.createProject(projectName, billingAccount).recoverWith {
              case t: Throwable =>
                // failed to create project in google land, rollback inserts above
                dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(projectName) } map(_ => throw t)
            }

            _ <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.insertOperations(Seq(createProjectOperation))
            }
          } yield {
            RequestComplete(StatusCodes.Created)
          }
        case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, createForbiddenErrorMessage("You", billingAccountName))))
        case Some(billingAccount) if !billingAccount.firecloudHasAccess => Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, createForbiddenErrorMessage(gcsDAO.billingEmail, billingAccountName))))
      }
    }
  }

  private def deleteRefreshTokenInternal(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    for {
      _ <- gcsDAO.revokeToken(rawlsUserRef)
      _ <- gcsDAO.deleteToken(rawlsUserRef).recover { case e: HttpResponseException if e.getStatusCode == 404 => Unit }
    } yield { Unit }
  }

  private def reduceErrorReports(errorReportOptions: Iterable[Option[ErrorReport]]): Option[ErrorReport] = {
    val errorReports = errorReportOptions.collect {
      case Some(errorReport) => errorReport
    }.toSeq

    errorReports match {
      case Seq() => None
      case Seq(single) => Option(single)
      case many => Option(ErrorReport("multiple errors", errorReports))
    }
  }

  /**
   * handles a Future [ Seq [ Try [ T ] ] ], calling success with the successful result of the tries or failure with any exceptions
   *
   * @param futures
   * @param success
   * @param failure
   * @tparam T
   * @return
   */
  private def handleFutures[T, R](futures: Future[Seq[Try[T]]])(success: Seq[T] => R, failure: Seq[Throwable] => R): Future[R] = {
    futures map { tries =>
      val exceptions = tries.collect { case Failure(t) => t }
      if (exceptions.isEmpty) {
        success(tries.map(_.get))
      } else {
        failure(exceptions)
      }
    }
  }

  private def handleException(message: String)(exceptions: Seq[Throwable]): PerRequestMessage = {
    throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, message, exceptions.map(ErrorReport(_))))
  }
}

