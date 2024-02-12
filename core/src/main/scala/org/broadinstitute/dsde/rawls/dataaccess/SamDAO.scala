package org.broadinstitute.dsde.rawls.dataaccess

import cats.effect.Async
import cats.effect.kernel.Resource
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUser,
  RawlsUserEmail,
  SamCreateResourceResponse,
  SamFullyQualifiedResourceId,
  SamPolicy,
  SamPolicySyncStatus,
  SamPolicyWithNameAndEmail,
  SamResourceAction,
  SamResourceIdWithPolicyName,
  SamResourcePolicyName,
  SamResourceRole,
  SamResourceTypeName,
  SamUserResource,
  SamUserStatusResponse,
  SubsystemStatus,
  SyncReportItem,
  UserIdInfo,
  UserInfo
}
import org.broadinstitute.dsde.workbench.model._

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
trait SamDAO {
  val errorReportSource = ErrorReportSource("sam")

  def registerUser(ctx: RawlsRequestContext): Future[Option[RawlsUser]]

  def getUserStatus(ctx: RawlsRequestContext): Future[Option[SamUserStatusResponse]]

  def getUserIdInfo(userEmail: String, ctx: RawlsRequestContext): Future[SamDAO.GetUserIdInfoResult]

  def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Unit]

  def createResourceFull(resourceTypeName: SamResourceTypeName,
                         resourceId: String,
                         policies: Map[SamResourcePolicyName, SamPolicy],
                         authDomain: Set[String],
                         ctx: RawlsRequestContext,
                         parent: Option[SamFullyQualifiedResourceId]
  ): Future[SamCreateResourceResponse]

  def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, ctx: RawlsRequestContext): Future[Unit]

  def userHasAction(resourceTypeName: SamResourceTypeName,
                    resourceId: String,
                    action: SamResourceAction,
                    cts: RawlsRequestContext
  ): Future[Boolean]

  def getActionServiceAccount(googleProject: GoogleProjectId,
                              resourceTypeName: SamResourceTypeName,
                              resourceId: String,
                              action: SamResourceAction,
                              ctx: RawlsRequestContext
  ): Future[WorkbenchEmail]

  def getPolicy(resourceTypeName: SamResourceTypeName,
                resourceId: String,
                policyName: SamResourcePolicyName,
                ctx: RawlsRequestContext
  ): Future[SamPolicy]

  def overwritePolicy(resourceTypeName: SamResourceTypeName,
                      resourceId: String,
                      policyName: SamResourcePolicyName,
                      policy: SamPolicy,
                      ctx: RawlsRequestContext
  ): Future[Unit]

  def addUserToPolicy(resourceTypeName: SamResourceTypeName,
                      resourceId: String,
                      policyName: SamResourcePolicyName,
                      memberEmail: String,
                      ctx: RawlsRequestContext
  ): Future[Unit]

  def removeUserFromPolicy(resourceTypeName: SamResourceTypeName,
                           resourceId: String,
                           policyName: SamResourcePolicyName,
                           memberEmail: String,
                           ctx: RawlsRequestContext
  ): Future[Unit]

  def inviteUser(userEmail: String, ctx: RawlsRequestContext): Future[Unit]

  def getUserIdInfoForEmail(userEmail: WorkbenchEmail): Future[UserIdInfo]

  def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName,
                         resourceId: String,
                         policyName: SamResourcePolicyName
  ): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]]

  def listUserResources(resourceTypeName: SamResourceTypeName, ctx: RawlsRequestContext): Future[Seq[SamUserResource]]

  def listPoliciesForResource(resourceTypeName: SamResourceTypeName,
                              resourceId: String,
                              ctx: RawlsRequestContext
  ): Future[Set[SamPolicyWithNameAndEmail]]

  def listUserRolesForResource(resourceTypeName: SamResourceTypeName,
                               resourceId: String,
                               ctx: RawlsRequestContext
  ): Future[Set[SamResourceRole]]

  def listUserActionsForResource(resourceTypeName: SamResourceTypeName,
                                 resourceId: String,
                                 ctx: RawlsRequestContext
  ): Future[Set[SamResourceAction]]

  def getPolicySyncStatus(resourceTypeName: SamResourceTypeName,
                          resourceId: String,
                          policyName: SamResourcePolicyName,
                          ctx: RawlsRequestContext
  ): Future[SamPolicySyncStatus]

  def getResourceAuthDomain(resourceTypeName: SamResourceTypeName,
                            resourceId: String,
                            ctx: RawlsRequestContext
  ): Future[Seq[String]]

  def getAccessInstructions(groupName: WorkbenchGroupName, ctx: RawlsRequestContext): Future[Option[String]]

  def listAllResourceMemberIds(resourceTypeName: SamResourceTypeName,
                               resourceId: String,
                               ctx: RawlsRequestContext
  ): Future[Set[UserIdInfo]]

  /**
    * @return a json blob
    */
  def getPetServiceAccountKeyForUser(googleProject: GoogleProjectId, userEmail: RawlsUserEmail): Future[String]

  def getDefaultPetServiceAccountKeyForUser(ctx: RawlsRequestContext): Future[String]

  def getUserArbitraryPetServiceAccountKey(userEmail: String): Future[String]

  def getUserPetServiceAccount(ctx: RawlsRequestContext, googleProjectId: GoogleProjectId): Future[WorkbenchEmail]

  def deleteUserPetServiceAccount(googleProject: GoogleProjectId, ctx: RawlsRequestContext): Future[Unit]

  def getStatus(): Future[SubsystemStatus]

  def listResourceChildren(resourceTypeName: SamResourceTypeName,
                           resourceId: String,
                           ctx: RawlsRequestContext
  ): Future[Seq[SamFullyQualifiedResourceId]]

  def admin: SamAdminDAO
}

trait SamAdminDAO {
  def listPolicies(resourceType: SamResourceTypeName,
                   resourceId: String,
                   ctx: RawlsRequestContext
  ): Future[Set[SamPolicyWithNameAndEmail]]
  def addUserToPolicy(resourceTypeName: SamResourceTypeName,
                      resourceId: String,
                      policyName: SamResourcePolicyName,
                      memberEmail: String,
                      ctx: RawlsRequestContext
  ): Future[Unit]
  def removeUserFromPolicy(resourceTypeName: SamResourceTypeName,
                           resourceId: String,
                           policyName: SamResourcePolicyName,
                           memberEmail: String,
                           ctx: RawlsRequestContext
  ): Future[Unit]
}

object SamDAO {
  sealed trait GetUserIdInfoResult extends Product with Serializable
  case object NotFound extends GetUserIdInfoResult
  case object NotUser extends GetUserIdInfoResult
  final case class User(userIdInfo: UserIdInfo) extends GetUserIdInfoResult
  val defaultScopes = Set(
    com.google.api.services.oauth2.Oauth2Scopes.USERINFO_EMAIL,
    com.google.api.services.oauth2.Oauth2Scopes.USERINFO_PROFILE
  )

  implicit class SamExtensions(samDAO: SamDAO) {
    def asResourceAdmin[A, F[_]](resourceTypeName: SamResourceTypeName,
                                 resourceId: String,
                                 policyName: SamResourcePolicyName,
                                 ctx: RawlsRequestContext
    )(runAsAdmin: => F[A])(implicit F: Async[F]): F[A] = {
      def invoke(f: (SamResourceTypeName, String, SamResourcePolicyName, String, RawlsRequestContext) => Future[Unit]) =
        F.fromFuture(F.delay {
          f(resourceTypeName, resourceId, policyName, ctx.userInfo.userEmail.value, ctx)
        })

      Resource
        .make(invoke(samDAO.admin.addUserToPolicy))(_ => invoke(samDAO.admin.removeUserFromPolicy))
        .use(_ => runAsAdmin)
    }
  }
}
