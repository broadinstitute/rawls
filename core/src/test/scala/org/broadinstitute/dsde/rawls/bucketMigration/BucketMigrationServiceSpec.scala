package org.broadinstitute.dsde.rawls.bucketMigration

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamUserStatusResponse,
  UserInfo
}
import org.mockito.Mockito.{when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.not
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class BucketMigrationServiceSpec extends AnyFlatSpec with TestDriverComponent {

  private def getRequestContext(userEmail: String): RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken("fake_token"), 0L, RawlsUserSubjectId("user_id"))
    )

  def mockAdminService(mockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                       mockGcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
  ): BucketMigrationService = {
    val adminUser = "admin@example.com"
    val adminCtx = getRequestContext(adminUser)
    when(mockGcsDAO.isAdmin(adminUser)).thenReturn(Future.successful(true))
    when(mockSamDAO.getUserStatus(adminCtx))
      .thenReturn(Future.successful(Some(SamUserStatusResponse("userId", adminUser, true))))

    BucketMigrationService.constructor(slickDataSource, mockSamDAO, mockGcsDAO)(adminCtx)
  }

  def mockNonAdminService(mockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                          mockGcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
  ): BucketMigrationService = {
    val nonAdminUser = "user@example.com"
    val nonAdminCtx = getRequestContext(nonAdminUser)
    when(mockGcsDAO.isAdmin(nonAdminUser)).thenReturn(Future.successful(false))
    BucketMigrationService.constructor(slickDataSource, mockSamDAO, mockGcsDAO)(nonAdminCtx)
  }

  def mockAdminEnforcementTest(): (BucketMigrationService, BucketMigrationService) = {
    val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

    (mockAdminService(mockSamDAO = mockSamDAO, mockGcsDAO = mockGcsDAO),
     mockNonAdminService(mockSamDAO = mockSamDAO, mockGcsDAO = mockGcsDAO)
    )
  }

  it should "schedule a single workspace and return past migration attempts" in withMinimalTestDatabase { _ =>
    val adminService = mockAdminService()

    Await.result(
      for {
        preMigrationAttempts <- adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName)
        _ <- adminService.migrateWorkspaceBucket(minimalTestData.wsName)
        postMigrationAttempts <- adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName)
      } yield {
        preMigrationAttempts shouldBe List.empty
        postMigrationAttempts should not be empty
      },
      Duration.Inf
    )
  }

  it should "schedule all workspaces in a billing project and return the migration attempts" in withMinimalTestDatabase {
    _ =>
      val adminService = mockAdminService()

      Await.result(
        for {
          preMigrationAttempts <- adminService.getBucketMigrationAttemptsForBillingProject(
            minimalTestData.billingProject.projectName
          )
          _ <- adminService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName)
          postMigrationAttempts <- adminService.getBucketMigrationAttemptsForBillingProject(
            minimalTestData.billingProject.projectName
          )
        } yield {
          preMigrationAttempts.foreach { case (_, attempts) =>
            attempts shouldBe List.empty
          }
          postMigrationAttempts.foreach { case (_, attempts) =>
            attempts should not be empty
          }
        },
        Duration.Inf
      )
  }

  it should "schedule any eligible workspaces in a billing project for migration and return any failures" in withMinimalTestDatabase {
    _ =>
      val adminService = mockAdminService()

      val lockedWorkspace =
        minimalTestData.workspace.copy(name = "lockedWorkspace",
                                       isLocked = true,
                                       workspaceId = UUID.randomUUID.toString
        )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(adminService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                     Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      Await
        .result(adminService.getBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
                Duration.Inf
        )
        .foreach {
          case (lockedWorkspace.name, attempts) => attempts shouldBe List.empty
          case (_, attempts)                    => attempts should not be empty
        }
  }

  it should "schedule all workspaces and return any errors that occur" in withMinimalTestDatabase { _ =>
    val adminService = mockAdminService()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) shouldBe List.empty
    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName2),
                 Duration.Inf
    ) shouldBe List.empty
    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(lockedWorkspace.toWorkspaceName),
                 Duration.Inf
    ) shouldBe List.empty

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(adminService.migrateAllWorkspaceBuckets(
                     List(minimalTestData.wsName, minimalTestData.wsName2, lockedWorkspace.toWorkspaceName)
                   ),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) should not be empty
    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName2),
                 Duration.Inf
    ) should not be empty
    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(lockedWorkspace.toWorkspaceName),
                 Duration.Inf
    ) shouldBe List.empty
  }

  behavior of "getBucketMigrationAttemptsForWorkspace"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonAdminService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "getBucketMigrationAttemptsForBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.getBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.getBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "migrateWorkspaceBucket"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.migrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.migrateWorkspaceBucket(minimalTestData.wsName),
        Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "fail when a workspace is locked" in withMinimalTestDatabase { _ =>
    val adminService = mockAdminService()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(adminService.migrateWorkspaceBucket(lockedWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  behavior of "migrateAllWorkspaceBuckets"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.migrateAllWorkspaceBuckets(List(minimalTestData.wsName, minimalTestData.wsName2)),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.migrateAllWorkspaceBuckets(List(minimalTestData.wsName, minimalTestData.wsName2)),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "migrateWorkspaceBucketsInBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }
}
