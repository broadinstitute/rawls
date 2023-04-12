package org.broadinstitute.dsde.rawls.fastpass

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import org.broadinstitute.dsde.rawls.config.FastPassConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUser,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceTypeNames,
  SamWorkspaceRoles,
  UserInfo
}
import org.broadinstitute.dsde.workbench.client.sam.model.{Enabled, UserInfo => SamClientUserInfo, UserStatus}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, doReturn, spy, when}

import scala.concurrent.{ExecutionContext, Future}

object MockFastPassService {

  def setupUsers(user: RawlsUser, testUsers: Seq[RawlsUser], samDAO: SamDAO, gcsDAO: GoogleServicesDAO): Unit = {
    val mockSamAdminDAO = spy(samDAO.admin)
    when(samDAO.admin).thenReturn(mockSamAdminDAO)
    (testUsers ++ Seq(user)).foreach { testUser =>
      doReturn(
        Future.successful(
          Some(
            new UserStatus()
              .userInfo(
                new SamClientUserInfo()
                  .userEmail(testUser.userEmail.value)
                  .userSubjectId(testUser.userSubjectId.value)
              )
              .enabled(new Enabled().google(true).ldap(true).allUsersGroup(true))
          )
        )
      ).when(mockSamAdminDAO)
        .getUserByEmail(
          ArgumentMatchers.eq(testUser.userEmail.value),
          ArgumentMatchers.any[RawlsRequestContext]
        )

      val petKey = s"${testUser.userEmail.value}-pet-key"
      val petUserSubjectId = RawlsUserSubjectId(s"${testUser.userSubjectId.value}-pet")
      val petEmail = s"${testUser.userEmail.value}-pet@bar.com"

      doReturn(Future.successful(petKey))
        .when(samDAO)
        .getPetServiceAccountKeyForUser(
          ArgumentMatchers.any[GoogleProjectId],
          ArgumentMatchers.eq(testUser.userEmail)
        )

      doReturn(Future.successful(WorkbenchEmail(petEmail)))
        .when(samDAO)
        .getUserPetServiceAccount(
          ArgumentMatchers.argThat((ctx: RawlsRequestContext) => ctx.userInfo.userEmail.equals(testUser.userEmail)),
          ArgumentMatchers.any[GoogleProjectId]
        )

      doReturn(
        Future.successful(
          UserInfo(RawlsUserEmail(petEmail), OAuth2BearerToken("test_token"), 0, petUserSubjectId)
        )
      ).when(gcsDAO).getUserInfoUsingJson(ArgumentMatchers.eq(petKey))

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

  def constructor(user: RawlsUser,
                  testUsers: Seq[RawlsUser],
                  config: FastPassConfig,
                  googleIamDAO: GoogleIamDAO,
                  googleStorageDAO: GoogleStorageDAO,
                  googleServicesDAO: GoogleServicesDAO,
                  samDAO: SamDAO,
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  terraBucketReaderRole: String,
                  terraBucketWriterRole: String
  )(ctx: RawlsRequestContext, dataAccess: DataAccess)(implicit
    executionContext: ExecutionContext,
    openTelemetry: OpenTelemetryMetrics[IO]
  ): FastPassService = {
    setupUsers(user, testUsers, samDAO, googleServicesDAO)
    new FastPassService(
      ctx,
      dataAccess,
      config,
      googleIamDAO,
      googleStorageDAO,
      googleServicesDAO,
      samDAO,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole
    )
  }
}
