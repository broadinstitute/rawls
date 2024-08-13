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
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.{doAnswer, doReturn, spy}

import scala.concurrent.{ExecutionContext, Future}

object MockFastPassService {

  /**
    * Sets up a FastPassService with mocked calls to Sam and Google Services.
    * @return A tuple of (MockFastPassService, MockSamDAO, MockGoogleServicesDAO)
    */
  def setup(user: RawlsUser,
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
    executionContext: ExecutionContext
  ): (FastPassServiceImpl, GoogleServicesDAO, SamDAO) = {
    val googleAccessContextManagerDAO = Mockito.spy(new MockGoogleAccessContextManagerDAO())

    val mockGcsDAO = spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val mockSamDAO = spy(new MockSamDAO(dataSource))

    setupUsers(user, testUsers, mockSamDAO, mockGcsDAO)
    (spy(
       new FastPassServiceImpl(
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

  private def setupUsers(user: RawlsUser, testUsers: Seq[RawlsUser], samDAO: SamDAO, gcsDAO: GoogleServicesDAO): Unit =
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

      val petUserSubjectId = RawlsUserSubjectId(s"${testUser.userSubjectId.value}-pet")

      def projectPetInfo(googleProject: String): (WorkbenchEmail, String) = {
        val petEmail = WorkbenchEmail(s"${testUser.userEmail.value}-pet@$googleProject.iam.gserviceaccount.com")
        val petKey =
          s"""{"private_key_id": "${testUser.userEmail.value}-$googleProject-pet-key", "client_id": "${petUserSubjectId.value}", "client_email": "${petEmail.value}" }"""
        (petEmail, petKey)
      }

      doReturn(Future.successful(userStatusResponse))
        .when(samDAO)
        .getUserStatus(
          ArgumentMatchers.argThat((ctx: RawlsRequestContext) =>
            ctx.userInfo.userEmail.value.startsWith(s"${testUser.userEmail.value}-pet")
          )
        )

      doAnswer { invocation =>
        val googleProjectId = invocation.getArgument[GoogleProjectId](0)
        Future.successful(projectPetInfo(googleProjectId.value)._2)
      }
        .when(samDAO)
        .getPetServiceAccountKeyForUser(
          ArgumentMatchers.any[GoogleProjectId],
          ArgumentMatchers.eq(testUser.userEmail)
        )

      doAnswer { invocation =>
        val googleProjectId = invocation.getArgument[GoogleProjectId](1)
        Future.successful(projectPetInfo(googleProjectId.value)._1)
      }
        .when(samDAO)
        .getUserPetServiceAccount(
          ArgumentMatchers.argThat((ctx: RawlsRequestContext) => ctx.userInfo.userEmail.equals(testUser.userEmail)),
          ArgumentMatchers.any[GoogleProjectId]
        )

      doReturn(
        Future.successful(
          UserInfo(RawlsUserEmail(""), OAuth2BearerToken("test_token"), 0, petUserSubjectId)
        )
      ).when(gcsDAO)
        .getUserInfoUsingJson(
          ArgumentMatchers.argThat((key: String) => key.contains(s"${testUser.userEmail.value}-pet"))
        )

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
}
