package org.broadinstitute.dsde.rawls.fastpass

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import org.broadinstitute.dsde.rawls.config.FastPassConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockGoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUser,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceRoles,
  UserIdInfo,
  UserInfo
}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.{doAnswer, doReturn, spy}

import scala.concurrent.{ExecutionContext, Future}

object MockFastPassService {

  def setupUsers(user: RawlsUser, testUsers: Seq[RawlsUser], samDAO: SamDAO, gcsDAO: GoogleServicesDAO): Unit =
    (testUsers ++ Seq(user)).foreach { testUser =>
      val userIdInfo = UserIdInfo(
        testUser.userSubjectId.value,
        testUser.userEmail.value,
        Some(testUser.userSubjectId.value + "-google")
      )

      val userStatusResponse = Some(
        SamUserStatusResponse(testUser.userSubjectId.value, testUser.userEmail.value, enabled = true)
      )

      doReturn(
        Future.successful(
          SamDAO.User(userIdInfo)
        )
      ).when(samDAO)
        .getUserIdInfo(
          ArgumentMatchers.eq(testUser.userEmail.value),
          ArgumentMatchers.any[RawlsRequestContext]
        )

      doReturn(Future.successful(userIdInfo))
        .when(samDAO)
        .getUserIdInfoForEmail(ArgumentMatchers.eq(WorkbenchEmail(user.userEmail.value)))

      doReturn(Future.successful(userStatusResponse))
        .when(samDAO)
        .getUserStatus(
          ArgumentMatchers.argThat((ctx: RawlsRequestContext) => ctx.userInfo.userEmail.equals(testUser.userEmail))
        )

      val petKey = s"${testUser.userEmail.value}-pet-key"
      val petUserSubjectId = RawlsUserSubjectId(s"${testUser.userSubjectId.value}-pet")
      val petEmail = s"${testUser.userEmail.value}-pet@bar.com"

      doReturn(Future.successful(userStatusResponse))
        .when(samDAO)
        .getUserStatus(
          ArgumentMatchers.argThat((ctx: RawlsRequestContext) => ctx.userInfo.userEmail.value.startsWith(petEmail))
        )

      doAnswer { invocation =>
        val googleProjectId = invocation.getArgument[GoogleProjectId](0)
        Future.successful(petKey + googleProjectId.value)
      }
        .when(samDAO)
        .getPetServiceAccountKeyForUser(
          ArgumentMatchers.any[GoogleProjectId],
          ArgumentMatchers.eq(testUser.userEmail)
        )

      doAnswer { invocation =>
        val googleProjectId = invocation.getArgument[GoogleProjectId](1)
        Future.successful(WorkbenchEmail(petEmail + googleProjectId.value))
      }
        .when(samDAO)
        .getUserPetServiceAccount(
          ArgumentMatchers.argThat((ctx: RawlsRequestContext) => ctx.userInfo.userEmail.equals(testUser.userEmail)),
          ArgumentMatchers.any[GoogleProjectId]
        )

      doAnswer { invocation =>
        val key = invocation.getArgument[String](0)
        val googleProjectId = key.replace(petKey, "")
        Future.successful(
          UserInfo(RawlsUserEmail(petEmail + googleProjectId), OAuth2BearerToken("test_token"), 0, petUserSubjectId)
        )

      }.when(gcsDAO).getUserInfoUsingJson(ArgumentMatchers.argThat((key: String) => key.startsWith(petKey)))

      doReturn(Future.successful(testUser match {
        case testUser if testUser.userEmail.value.equals("writer-access") =>
          Set(SamWorkspaceRoles.writer, SamWorkspaceRoles.canCompute)
        case testUser if testUser.userEmail.value.equals("reader-access") => Set(SamWorkspaceRoles.shareReader)
        case testUser if testUser.userEmail.value.equals("owner-access") || testUser.userEmail.equals(user.userEmail) =>
          Set(SamWorkspaceRoles.owner)
        case _ => Set()
      }))
        .when(samDAO)
        .listUserRolesForResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.any[String],
          ArgumentMatchers.argThat((arg: RawlsRequestContext) => arg.userInfo.userSubjectId.equals(petUserSubjectId))
        )
    }

  def constructor(user: RawlsUser,
                  testUsers: Seq[RawlsUser],
                  config: FastPassConfig,
                  googleIamDAO: GoogleIamDAO,
                  googleStorageDAO: GoogleStorageDAO,
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  terraBucketReaderRole: String,
                  terraBucketWriterRole: String
  )(ctx: RawlsRequestContext, dataSource: SlickDataSource)(implicit
    executionContext: ExecutionContext,
    openTelemetry: OpenTelemetryMetrics[IO]
  ): (FastPassService, GoogleServicesDAO, SamDAO) = {
    val googleAccessContextManagerDAO = Mockito.spy(new MockGoogleAccessContextManagerDAO())

    val mockGcsDAO = spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val mockSamDAO = spy(new MockSamDAO(dataSource))

    setupUsers(user, testUsers, mockSamDAO, mockGcsDAO)
    (spy(
       new FastPassService(
         ctx,
         dataSource,
         config,
         googleIamDAO,
         googleStorageDAO,
         mockGcsDAO,
         mockSamDAO,
         terraBillingProjectOwnerRole,
         terraWorkspaceCanComputeRole,
         terraWorkspaceNextflowRole,
         terraBucketReaderRole,
         terraBucketWriterRole
       )
     ),
     mockGcsDAO,
     mockSamDAO
    )
  }
}
