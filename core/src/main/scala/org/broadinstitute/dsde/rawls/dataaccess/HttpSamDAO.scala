package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.common.tracing.OkHttpClientTracingInterceptor
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.{Span, Tracing}
import okhttp3.{Interceptor, Response}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, Retry}
import org.broadinstitute.dsde.workbench.client.sam
import org.broadinstitute.dsde.workbench.client.sam.api._
import org.broadinstitute.dsde.workbench.client.sam.{ApiCallback, ApiClient, ApiException}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}

import java.util
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String, serviceAccountCreds: Credential)(implicit
  val system: ActorSystem,
  val executionContext: ExecutionContext
) extends SamDAO
    with Retry
    with LazyLogging
    with FutureSupport {

  private val samServiceURL = baseSamServiceURL

  private val okHttpClient = new ApiClient().getHttpClient

  protected def getApiClient(ctx: RawlsRequestContext): ApiClient = {

    val okHttpClientWithTracingBuilder = okHttpClient.newBuilder
    ctx.tracingSpan.foreach(span =>
      okHttpClientWithTracingBuilder
        .addInterceptor(new SpanSettingInterceptor(span))
        .addInterceptor(new OkHttpClientTracingInterceptor(Tracing.getTracer))
    )

    val samApiClient = new ApiClient(okHttpClientWithTracingBuilder.build())
    samApiClient.setBasePath(samServiceURL)
    samApiClient.setAccessToken(ctx.userInfo.accessToken.token)

    samApiClient
  }

  protected def googleApi(ctx: RawlsRequestContext) = new GoogleApi(getApiClient(ctx))

  protected def resourcesApi(ctx: RawlsRequestContext) = new ResourcesApi(getApiClient(ctx))

  protected def usersApi(ctx: RawlsRequestContext) = new UsersApi(getApiClient(ctx))

  protected def groupApi(ctx: RawlsRequestContext) = new GroupApi(getApiClient(ctx))

  protected def adminApi(ctx: RawlsRequestContext) = new AdminApi(getApiClient(ctx))

  private def rawlsSAContext = RawlsRequestContext(
    UserInfo(RawlsUserEmail(""), OAuth2BearerToken(getServiceAccountAccessToken), 0, RawlsUserSubjectId(""))
  )

  protected def when401or5xx: Predicate[Throwable] = anyOf(DsdeHttpDAO.when5xx, DsdeHttpDAO.whenUnauthorized)

  private class SamApiCallback[T](functionName: String) extends ApiCallback[T] {
    private val promise = Promise[T]()

    override def onFailure(e: ApiException,
                           statusCode: Int,
                           responseHeaders: util.Map[String, util.List[String]]
    ): Unit = {
      val response = e.getResponseBody

      // attempt to propagate an ErrorReport from Sam. If we can't understand Sam's response as an ErrorReport,
      // create our own error message.
      import WorkspaceJsonSupport.ErrorReportFormat
      import spray.json._
      val errorReport = Try(response.parseJson.convertTo[ErrorReport]).recover { case _: Throwable =>
        ErrorReport(StatusCode.int2StatusCode(statusCode), s"Sam call to $functionName failed with error '$response'")
      }.get

      val rawlsExceptionWithErrorReport = new RawlsExceptionWithErrorReport(errorReport)
      logger.info(s"Sam call to $functionName failed", rawlsExceptionWithErrorReport)
      promise.failure(rawlsExceptionWithErrorReport)
    }

    override def onSuccess(result: T, statusCode: Int, responseHeaders: util.Map[String, util.List[String]]): Unit =
      promise.success(result)

    override def onUploadProgress(bytesWritten: Long, contentLength: Long, done: Boolean): Unit = ()

    override def onDownloadProgress(bytesRead: Long, contentLength: Long, done: Boolean): Unit = ()

    def future: Future[T] = promise.future
  }

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeName,
                                   resourceId: String,
                                   policyName: SamResourcePolicyName,
                                   ctx: RawlsRequestContext
  ): Future[SamPolicySyncStatus] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.SyncStatus]("syncStatus")

      googleApi(ctx).syncStatusAsync(resourceTypeName.value, resourceId, policyName.value, callback)

      callback.future.map { syncStatus =>
        SamPolicySyncStatus(syncStatus.getLastSyncDate, WorkbenchEmail(syncStatus.getEmail))
      }
    }

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeName,
                                        resourceId: String,
                                        ctx: RawlsRequestContext
  ): Future[Set[SamResourceRole]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[String]]("resourceRolesV2")

      resourcesApi(ctx).resourceRolesV2Async(resourceTypeName.value, resourceId, callback)

      callback.future.map { roles =>
        roles.asScala.map(SamResourceRole).toSet
      }
    }

  override def listUserActionsForResource(resourceTypeName: SamResourceTypeName,
                                          resourceId: String,
                                          ctx: RawlsRequestContext
  ): Future[Set[SamResourceAction]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[String]]("resourceActionsV2")

      resourcesApi(ctx).resourceActionsV2Async(resourceTypeName.value, resourceId, callback)

      callback.future.map { roles =>
        roles.asScala.map(SamResourceAction).toSet
      }
    }

  override def createResourceFull(resourceTypeName: SamResourceTypeName,
                                  resourceId: String,
                                  policies: Map[SamResourcePolicyName, SamPolicy],
                                  authDomain: Set[String],
                                  ctx: RawlsRequestContext,
                                  parent: Option[SamFullyQualifiedResourceId]
  ): Future[SamCreateResourceResponse] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("createResourceV2")

      val createRequest = new sam.model.CreateResourceRequestV2()
        .resourceId(resourceId)
        .authDomain(authDomain.toList.asJava)
        .policies(policies.map { case (policyName, policy) =>
          policyName.value -> new sam.model.AccessPolicyMembershipV2()
            .roles(policy.roles.map(_.value).toList.asJava)
            .actions(policy.actions.map(_.value).toList.asJava)
            .memberEmails(policy.memberEmails.map(_.value).toList.asJava)
        }.asJava)
        // .returnResource(true)
        .parent(
          parent
            .map(p =>
              new sam.model.FullyQualifiedResourceId().resourceTypeName(p.resourceTypeName).resourceId(p.resourceId)
            )
            .orNull
        )

      resourcesApi(ctx).createResourceV2Async(resourceTypeName.value, createRequest, callback)

      callback.future
    }.flatMap { _ =>
      // This second api call is because the generated client does not know how to handle different response types
      // for the same api. When `returnResource(true)` the above api call returns the information fetched by
      // the api call below. When `returnResource(false)` the above api call returns no content. The generated
      // client only handles the no content case. So we go fetch. This api response variability was added
      // in Oct. 2019 https://broadworkbench.atlassian.net/browse/AS-55. This was a performance optimization
      // because sam was making too many expensive ldap calls. Sam does not use ldap anymore so those performance
      // characteristics are irrelevant. However, this kind of sucks.
      listPoliciesForResource(resourceTypeName, resourceId, ctx).map { policies =>
        SamCreateResourceResponse(
          resourceTypeName.value,
          resourceId,
          authDomain,
          policies.map(p =>
            SamCreateResourcePolicyResponse(
              SamCreateResourceAccessPolicyIdResponse(p.policyName.value,
                                                      SamFullyQualifiedResourceId(resourceId, resourceTypeName.value)
              ),
              p.email.value
            )
          )
        )
      }
    }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName,
                                       resourceId: String,
                                       ctx: RawlsRequestContext
  ): Future[Set[SamPolicyWithNameAndEmail]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[sam.model.AccessPolicyResponseEntryV2]]("listResourcePoliciesV2")

      resourcesApi(ctx).listResourcePoliciesV2Async(resourceTypeName.value, resourceId, callback)

      callback.future.map(toSamPolicyWithNameAndEmails)
    }

  private def toSamPolicyWithNameAndEmails(policies: util.List[sam.model.AccessPolicyResponseEntryV2]) =
    policies.asScala
      .map(policy =>
        SamPolicyWithNameAndEmail(
          SamResourcePolicyName(policy.getPolicyName),
          SamPolicy(
            policy.getPolicy.getMemberEmails.asScala.map(WorkbenchEmail).toSet,
            policy.getPolicy.getActions.asScala.map(SamResourceAction).toSet,
            policy.getPolicy.getRoles.asScala.map(SamResourceRole).toSet
          ),
          WorkbenchEmail(policy.getEmail)
        )
      )
      .toSet

  override def registerUser(ctx: RawlsRequestContext): Future[Option[RawlsUser]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.UserStatus]("createUserV2")

      usersApi(ctx).createUserV2Async(null, callback)

      callback.future
        .map { userStatus =>
          Option(
            RawlsUser(RawlsUserSubjectId(userStatus.getUserInfo.getUserSubjectId),
                      RawlsUserEmail(userStatus.getUserInfo.getUserEmail)
            )
          )
        }
        .recover {
          case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.Conflict) =>
            None
        }
    }

  override def getUserStatus(ctx: RawlsRequestContext): Future[Option[SamUserStatusResponse]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.UserStatusInfo]("getUserStatusInfo")

      usersApi(ctx).getUserStatusInfoAsync(callback)

      callback.future
        .map { userStatus =>
          Option(SamUserStatusResponse(userStatus.getUserSubjectId, userStatus.getUserEmail, userStatus.getEnabled))
        }
        .recover {
          case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.Conflict) =>
            None
        }
    }

  override def getUserIdInfo(userEmail: String, ctx: RawlsRequestContext): Future[SamDAO.GetUserIdInfoResult] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.UserIdInfo]("getUserIds")

      usersApi(ctx).getUserIdsAsync(userEmail, callback)

      callback.future
        .map { userIdInfo =>
          Option(userIdInfo)
            .map(i => SamDAO.User(UserIdInfo(i.getUserSubjectId, i.getUserEmail, Option(i.getUserSubjectId))))
            .getOrElse(SamDAO.NotUser)
        }
        .recover {
          case notOK: RawlsExceptionWithErrorReport
              if notOK.errorReport.statusCode.exists(
                Set[StatusCode](StatusCodes.Conflict, StatusCodes.NotFound).contains
              ) =>
            SamDAO.NotFound
        }
    }

  override def createResource(resourceTypeName: SamResourceTypeName,
                              resourceId: String,
                              ctx: RawlsRequestContext
  ): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("createResourceWithDefaultsV2")

      resourcesApi(ctx).createResourceWithDefaultsV2Async(resourceTypeName.value, resourceId, null, callback)

      callback.future.map(_ => ())
    }

  override def deleteResource(resourceTypeName: SamResourceTypeName,
                              resourceId: String,
                              ctx: RawlsRequestContext
  ): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("deleteResourceV2")

      resourcesApi(ctx).deleteResourceV2Async(resourceTypeName.value, resourceId, callback)

      callback.future.map(_ => ())
    }

  override def userHasAction(resourceTypeName: SamResourceTypeName,
                             resourceId: String,
                             action: SamResourceAction,
                             ctx: RawlsRequestContext
  ): Future[Boolean] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[java.lang.Boolean]("resourcePermissionV2")

      resourcesApi(ctx).resourcePermissionV2Async(resourceTypeName.value, resourceId, action.value, callback)

      callback.future.map(_.booleanValue())
    }

  override def getPolicy(resourceTypeName: SamResourceTypeName,
                         resourceId: String,
                         policyName: SamResourcePolicyName,
                         ctx: RawlsRequestContext
  ): Future[SamPolicy] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.AccessPolicyMembershipV2]("getPolicyV2")

      resourcesApi(ctx).getPolicyV2Async(resourceTypeName.value, resourceId, policyName.value.toLowerCase, callback)

      callback.future.map { policy =>
        SamPolicy(
          policy.getMemberEmails.asScala.toSet.map(WorkbenchEmail),
          policy.getActions.asScala.toSet.map(SamResourceAction),
          policy.getRoles.asScala.toSet.map(SamResourceRole)
        )
      }
    }

  override def listResourceChildren(resourceTypeName: SamResourceTypeName,
                                    resourceId: String,
                                    ctx: RawlsRequestContext
  ): Future[Seq[SamFullyQualifiedResourceId]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[sam.model.FullyQualifiedResourceId]]("listResourceChildren")

      resourcesApi(ctx).listResourceChildrenAsync(resourceTypeName.value, resourceId, callback)

      callback.future.map { ids =>
        ids.asScala.toSeq.map(id => SamFullyQualifiedResourceId(id.getResourceId, id.getResourceTypeName))
      }
    }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName,
                               resourceId: String,
                               policyName: SamResourcePolicyName,
                               policy: SamPolicy,
                               ctx: RawlsRequestContext
  ): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("overwritePolicyV2")

      resourcesApi(ctx).overwritePolicyV2Async(
        resourceTypeName.value,
        resourceId,
        policyName.value.toLowerCase,
        new sam.model.AccessPolicyMembershipV2()
          .memberEmails(policy.memberEmails.map(_.value).toList.asJava)
          .actions(policy.actions.map(_.value).toList.asJava)
          .roles(policy.roles.map(_.value).toList.asJava),
        callback
      )

      callback.future.map(_ => ())
    }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName,
                               resourceId: String,
                               policyName: SamResourcePolicyName,
                               memberEmail: String,
                               ctx: RawlsRequestContext
  ): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("addUserToPolicyV2")

      resourcesApi(ctx).addUserToPolicyV2Async(resourceTypeName.value,
                                               resourceId,
                                               policyName.value.toLowerCase,
                                               memberEmail,
                                               null,
                                               callback
      )

      callback.future.map(_ => ())
    }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName,
                                    resourceId: String,
                                    policyName: SamResourcePolicyName,
                                    memberEmail: String,
                                    ctx: RawlsRequestContext
  ): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("removeUserFromPolicyV2")

      resourcesApi(ctx).removeUserFromPolicyV2Async(resourceTypeName.value,
                                                    resourceId,
                                                    policyName.value.toLowerCase,
                                                    memberEmail,
                                                    callback
      )

      callback.future.map(_ => ())
    }

  override def inviteUser(userEmail: String, ctx: RawlsRequestContext): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.UserStatusDetails]("inviteUser")

      usersApi(ctx).inviteUserAsync(userEmail, null, callback)

      callback.future.map(_ => ())
    }

  override def getUserIdInfoForEmail(userEmail: WorkbenchEmail): Future[UserIdInfo] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[sam.model.UserIdInfo]("getUserIds")

      usersApi(rawlsSAContext).getUserIdsAsync(userEmail.value, callback)

      callback.future.map { userIdInfo =>
        UserIdInfo(userIdInfo.getUserSubjectId, userIdInfo.getUserEmail, Option(userIdInfo.getGoogleSubjectId))
      }
    }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName,
                                  resourceId: String,
                                  policyName: SamResourcePolicyName
  ): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.Map[String, util.List[sam.model.SyncReportEntry]]]("syncPolicy")

      googleApi(rawlsSAContext).syncPolicyAsync(resourceTypeName.value, resourceId, policyName.value, null, callback)

      callback.future.map { response =>
        response.asScala.map { case (email, entries) =>
          WorkbenchEmail(email) -> entries.asScala
            .map(entry =>
              SyncReportItem(entry.getOperation, entry.getEmail, Option(entry.getErrorReport).map(mapSamErrorReport))
            )
            .toSeq
        }.toMap
      }
    }

  private def mapSamErrorReport(samError: sam.model.ErrorReport): ErrorReport =
    ErrorReport(
      samError.getSource,
      samError.getMessage,
      Option(StatusCode.int2StatusCode(samError.getStatusCode)),
      samError.getCauses.asScala.map(mapSamErrorReport).toSeq,
      samError.getStackTrace.asScala.map { traceEle =>
        new StackTraceElement(traceEle.getClassName,
                              traceEle.getMethodName,
                              traceEle.getFileName,
                              traceEle.getLineNumber
        )
      }.toSeq,
      None
    ) // can't reliably convert a sam class name to a Class[_] in rawls

  override def listUserResources(resourceTypeName: SamResourceTypeName,
                                 ctx: RawlsRequestContext
  ): Future[Seq[SamUserResource]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[sam.model.UserResourcesResponse]]("listResourcesAndPoliciesV2")

      resourcesApi(ctx).listResourcesAndPoliciesV2Async(resourceTypeName.value, callback)

      callback.future.map { userResourcesResponse =>
        userResourcesResponse.asScala.map { userResourcesResponse =>
          SamUserResource(
            userResourcesResponse.getResourceId,
            toSamRolesAndActions(userResourcesResponse.getDirect),
            toSamRolesAndActions(userResourcesResponse.getInherited),
            toSamRolesAndActions(userResourcesResponse.getPublic),
            userResourcesResponse.getAuthDomainGroups.asScala.map(WorkbenchGroupName).toSet,
            userResourcesResponse.getMissingAuthDomainGroups.asScala.map(WorkbenchGroupName).toSet
          )
        }.toSeq
      }
    }

  private def toSamRolesAndActions(rolesAndActions: sam.model.RolesAndActions) =
    SamRolesAndActions(
      rolesAndActions.getRoles.asScala.map(SamResourceRole).toSet,
      rolesAndActions.getActions.asScala.map(SamResourceAction).toSet
    )

  override def getPetServiceAccountKeyForUser(googleProject: GoogleProjectId,
                                              userEmail: RawlsUserEmail
  ): Future[String] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[String]("getUserPetServiceAccountKey")

      googleApi(rawlsSAContext).getUserPetServiceAccountKeyAsync(googleProject.value, userEmail.value, callback)

      callback.future
    }

  override def deleteUserPetServiceAccount(googleProject: GoogleProjectId, ctx: RawlsRequestContext): Future[Unit] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[Void]("deletePetServiceAccount")

      googleApi(ctx).deletePetServiceAccountAsync(googleProject.value, callback)

      callback.future.map(_ => ())
    }

  override def getDefaultPetServiceAccountKeyForUser(ctx: RawlsRequestContext): Future[String] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[String]("getArbitraryPetServiceAccountKey")

      googleApi(ctx).getArbitraryPetServiceAccountKeyAsync(callback)

      callback.future
    }

  override def getDefaultPetServiceAccount(userEmail: String): Future[String] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[String]("getUserArbitraryPetServiceAccountKey")

      googleApi(rawlsSAContext).getUserArbitraryPetServiceAccountKeyAsync(userEmail, callback)

      callback.future
    }

  private def getServiceAccountAccessToken = {
    val expiresInSeconds = Option(serviceAccountCreds.getExpiresInSeconds).map(_.longValue()).getOrElse(0L)
    if (expiresInSeconds < 60 * 5) {
      serviceAccountCreds.refreshToken()
    }
    serviceAccountCreds.getAccessToken
  }

  override def getResourceAuthDomain(resourceTypeName: SamResourceTypeName,
                                     resourceId: String,
                                     ctx: RawlsRequestContext
  ): Future[Seq[String]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[String]]("getAuthDomainV2")

      resourcesApi(ctx).getAuthDomainV2Async(resourceTypeName.value, resourceId, callback)

      callback.future.map(_.asScala.toSeq)
    }

  override def listAllResourceMemberIds(resourceTypeName: SamResourceTypeName,
                                        resourceId: String,
                                        ctx: RawlsRequestContext
  ): Future[Set[UserIdInfo]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[util.List[sam.model.UserIdInfo]]("getAllResourceUsersV2")

      resourcesApi(ctx).getAllResourceUsersV2Async(resourceTypeName.value, resourceId, callback)

      callback.future.map(
        _.asScala
          .map(idInfo => UserIdInfo(idInfo.getUserSubjectId, idInfo.getUserEmail, Option(idInfo.getUserSubjectId)))
          .toSet
      )
    }

  override def getAccessInstructions(groupName: WorkbenchGroupName, ctx: RawlsRequestContext): Future[Option[String]] =
    retry(when401or5xx) { () =>
      val callback = new SamApiCallback[String]("getAccessInstructions")

      groupApi(ctx).getAccessInstructionsAsync(groupName.value, callback)

      callback.future.map(Option.apply).recover {
        case notFound: RawlsExceptionWithErrorReport
            if notFound.errorReport.statusCode.contains(StatusCodes.NotFound) =>
          None
      }
    }

  override def admin: SamAdminDAO = new SamAdminDAO {
    override def listPolicies(resourceType: SamResourceTypeName,
                              resourceId: String,
                              ctx: RawlsRequestContext
    ): Future[Set[SamPolicyWithNameAndEmail]] =
      retry(when401or5xx) { () =>
        val callback = new SamApiCallback[util.List[sam.model.AccessPolicyResponseEntryV2]]("adminListResourcePolicies")

        adminApi(ctx).adminListResourcePoliciesAsync(resourceType.value, resourceId, callback)

        callback.future.map(toSamPolicyWithNameAndEmails)
      }

    override def addUserToPolicy(resourceTypeName: SamResourceTypeName,
                                 resourceId: String,
                                 policyName: SamResourcePolicyName,
                                 memberEmail: String,
                                 ctx: RawlsRequestContext
    ): Future[Unit] =
      retry(when401or5xx) { () =>
        val callback = new SamApiCallback[Void]("adminAddUserToPolicy")

        adminApi(ctx).adminAddUserToPolicyAsync(resourceTypeName.value,
                                                resourceId,
                                                policyName.value,
                                                memberEmail,
                                                null,
                                                callback
        )

        callback.future.map(_ => ())
      }

    override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName,
                                      resourceId: String,
                                      policyName: SamResourcePolicyName,
                                      memberEmail: String,
                                      ctx: RawlsRequestContext
    ): Future[Unit] =
      retry(when401or5xx) { () =>
        val callback = new SamApiCallback[Void]("adminRemoveUserFromPolicy")

        adminApi(ctx).adminRemoveUserFromPolicyAsync(resourceTypeName.value,
                                                     resourceId,
                                                     policyName.value,
                                                     memberEmail,
                                                     callback
        )

        callback.future.map(_ => ())
      }
  }

  override def getStatus(): Future[SubsystemStatus] = {
    val callback = new SamApiCallback[sam.model.SystemStatus]("getSystemStatus")

    val statusApi = new StatusApi(new ApiClient(okHttpClient))
    statusApi.getApiClient.setBasePath(samServiceURL)
    statusApi.getSystemStatusAsync(callback)

    callback.future.map { samSystemStatus =>
      SubsystemStatus(
        samSystemStatus.getOk,
        Option(samSystemStatus.getSystems).map { subSystemStatuses =>
          val messages = for {
            (subSystem, subSystemStatus) <- subSystemStatuses.asScala
            message <- Option(subSystemStatus.getMessages).map(_.asScala).getOrElse(Seq("none"))
          } yield s"$subSystem: (ok: ${subSystemStatus.getOk}, message: $message)"
          messages.toList
        }
      )
    }
  }
}

class SpanSettingInterceptor(span: Span) extends Interceptor {
  override def intercept(chain: Interceptor.Chain): Response =
    Using(Tracing.getTracer.withSpan(span)) { _ =>
      chain.proceed(chain.request())
    }.get
}
