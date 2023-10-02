package org.broadinstitute.dsde.rawls.bucketMigration

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.implicits.catsSyntaxOptionId
import com.google.api.services.storage.model.Bucket
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceRole,
  SamResourceTypeNames,
  SamRolesAndActions,
  SamUserResource,
  SamUserStatusResponse,
  SamWorkspaceActions,
  SamWorkspaceRoles,
  UserInfo,
  Workspace,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.{
  MultiregionalBucketMigrationProgress,
  MultiregionalBucketMigrationStep,
  MultiregionalStorageTransferJobs,
  STSJobProgress
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.not
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class BucketMigrationServiceSpec extends AnyFlatSpec with TestDriverComponent {

  private def getRequestContext(userEmail: String): RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail(userEmail),
               OAuth2BearerToken("fake_token"),
               0L,
               RawlsUserSubjectId(UUID.randomUUID().toString)
      )
    )

  def mockBucketMigrationServiceForOwner(mockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                         mockGcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
  ): BucketMigrationService = {
    val ownerRequest = getRequestContext("owner@example.com")
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                               any(),
                               ArgumentMatchers.eq(SamWorkspaceActions.own),
                               ArgumentMatchers.eq(ownerRequest)
      )
    )
      .thenReturn(Future.successful(true))
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                               any(),
                               ArgumentMatchers.eq(SamBillingProjectActions.own),
                               ArgumentMatchers.eq(ownerRequest)
      )
    )
      .thenReturn(Future.successful(true))
    when(mockSamDAO.getUserStatus(ownerRequest)).thenReturn(
      Future.successful(
        Option(
          SamUserStatusResponse(ownerRequest.userInfo.userSubjectId.value,
                                ownerRequest.userInfo.userEmail.value,
                                enabled = true
          )
        )
      )
    )

    val bucket = mock[Bucket]
    when(bucket.getLocation).thenReturn("US")
    when(mockGcsDAO.getBucket(any(), any())(any())).thenReturn(Future.successful(Right(bucket)))

    BucketMigrationService.constructor(slickDataSource, mockSamDAO, mockGcsDAO)(ownerRequest)
  }

  def mockBucketMigrationServiceForNonOwner(mockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                            mockGcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
  ): BucketMigrationService = {
    val userRequest = getRequestContext("user@example.com")
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                               any(),
                               ArgumentMatchers.eq(SamWorkspaceActions.own),
                               ArgumentMatchers.eq(userRequest)
      )
    )
      .thenReturn(Future.successful(false))
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                               any(),
                               ArgumentMatchers.eq(SamWorkspaceActions.read),
                               ArgumentMatchers.eq(userRequest)
      )
    )
      .thenReturn(Future.successful(true))
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                               any(),
                               ArgumentMatchers.eq(SamBillingProjectActions.own),
                               ArgumentMatchers.eq(userRequest)
      )
    )
      .thenReturn(Future.successful(false))
    when(mockSamDAO.getUserStatus(userRequest)).thenReturn(
      Future.successful(
        Option(
          SamUserStatusResponse(userRequest.userInfo.userSubjectId.value,
                                userRequest.userInfo.userEmail.value,
                                enabled = true
          )
        )
      )
    )

    BucketMigrationService.constructor(slickDataSource, mockSamDAO, mockGcsDAO)(userRequest)
  }

  def mockOwnerEnforcementTest(): (BucketMigrationService, BucketMigrationService) = {
    val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

    (mockBucketMigrationServiceForOwner(mockSamDAO = mockSamDAO, mockGcsDAO = mockGcsDAO),
     mockBucketMigrationServiceForNonOwner(mockSamDAO = mockSamDAO, mockGcsDAO = mockGcsDAO)
    )
  }

  def mockBucketMigrationServiceForAdminUser(mockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                             mockGcsDAO: GoogleServicesDAO =
                                               mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
  ): BucketMigrationService = {
    val adminUser = "admin@example.com"
    val adminCtx = getRequestContext(adminUser)
    when(mockGcsDAO.isAdmin(adminUser)).thenReturn(Future.successful(true))
    when(mockSamDAO.getUserStatus(adminCtx))
      .thenReturn(Future.successful(Some(SamUserStatusResponse("userId", adminUser, true))))

    val bucket = mock[Bucket]
    when(bucket.getLocation).thenReturn("US")
    when(mockGcsDAO.getBucket(any(), any())(any())).thenReturn(Future.successful(Right(bucket)))

    BucketMigrationService.constructor(slickDataSource, mockSamDAO, mockGcsDAO)(adminCtx)
  }

  def mockBucketMigrationServiceForNonAdminUser(mockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                                mockGcsDAO: GoogleServicesDAO =
                                                  mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
  ): BucketMigrationService = {
    val nonAdminUser = "user@example.com"
    val nonAdminCtx = getRequestContext(nonAdminUser)
    when(mockGcsDAO.isAdmin(nonAdminUser)).thenReturn(Future.successful(false))
    BucketMigrationService.constructor(slickDataSource, mockSamDAO, mockGcsDAO)(nonAdminCtx)
  }

  def mockAdminEnforcementTest(): (BucketMigrationService, BucketMigrationService) = {
    val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

    (mockBucketMigrationServiceForAdminUser(mockSamDAO = mockSamDAO, mockGcsDAO = mockGcsDAO),
     mockBucketMigrationServiceForNonAdminUser(mockSamDAO = mockSamDAO, mockGcsDAO = mockGcsDAO)
    )
  }

  it should "allow admins to schedule a single workspace and return past migration attempts" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      Await.result(
        for {
          preMigrationAttempts <- adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName)
          _ <- adminService.adminMigrateWorkspaceBucket(minimalTestData.wsName)
          postMigrationAttempts <- adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName)
        } yield {
          preMigrationAttempts shouldBe List.empty
          postMigrationAttempts should not be empty
        },
        Duration.Inf
      )
  }

  it should "allow admins to schedule all workspaces in a billing project and return the migration attempts" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      Await.result(
        for {
          preMigrationAttempts <- adminService.adminGetBucketMigrationAttemptsForBillingProject(
            minimalTestData.billingProject.projectName
          )
          _ <- adminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName)
          postMigrationAttempts <- adminService.adminGetBucketMigrationAttemptsForBillingProject(
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

  it should "allow admins to schedule any eligible workspaces in a billing project for migration and return any failures" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      val lockedWorkspace =
        minimalTestData.workspace.copy(name = "lockedWorkspace",
                                       isLocked = true,
                                       workspaceId = UUID.randomUUID.toString
        )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          adminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
          Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      Await
        .result(
          adminService.adminGetBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
          Duration.Inf
        )
        .foreach {
          case (name, attempts) if name.equals(lockedWorkspace.toWorkspaceName.toString) => attempts shouldBe List.empty
          case (_, attempts)                                                             => attempts should not be empty
        }
  }

  it should "allow admins to schedule all workspaces and return any errors that occur" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) shouldBe List.empty
    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName2),
                 Duration.Inf
    ) shouldBe List.empty
    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(lockedWorkspace.toWorkspaceName),
                 Duration.Inf
    ) shouldBe List.empty

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(adminService.adminMigrateAllWorkspaceBuckets(
                     List(minimalTestData.wsName, minimalTestData.wsName2, lockedWorkspace.toWorkspaceName)
                   ),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) should not be empty
    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName2),
                 Duration.Inf
    ) should not be empty
    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(lockedWorkspace.toWorkspaceName),
                 Duration.Inf
    ) shouldBe List.empty
  }

  it should "allow owners to schedule a single workspace and return past migration attempts" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()

      Await.result(
        for {
          preMigrationAttempts <- ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName)
          _ <- ownerService.migrateWorkspaceBucket(minimalTestData.wsName)
          postMigrationAttempts <- ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName)
        } yield {
          preMigrationAttempts shouldBe List.empty
          postMigrationAttempts should not be empty
        },
        Duration.Inf
      )
  }

  it should "allow owners to schedule all workspaces in a billing project and return the migration attempts" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()

      Await.result(
        for {
          preMigrationAttempts <- ownerService.getBucketMigrationAttemptsForBillingProject(
            minimalTestData.billingProject.projectName
          )
          _ <- ownerService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName)
          postMigrationAttempts <- ownerService.getBucketMigrationAttemptsForBillingProject(
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

  it should "allow owners to schedule any eligible workspaces in a billing project for migration and return any failures" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()

      val lockedWorkspace =
        minimalTestData.workspace.copy(name = "lockedWorkspace",
                                       isLocked = true,
                                       workspaceId = UUID.randomUUID.toString
        )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(ownerService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                     Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      Await
        .result(ownerService.getBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
                Duration.Inf
        )
        .foreach {
          case (name, attempts) if name.equals(lockedWorkspace.toWorkspaceName.toString) => attempts shouldBe List.empty
          case (_, attempts)                                                             => attempts should not be empty
        }
  }

  it should "allow owners to schedule all workspaces and return any errors that occur" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) shouldBe List.empty
    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName2),
                 Duration.Inf
    ) shouldBe List.empty
    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(lockedWorkspace.toWorkspaceName),
                 Duration.Inf
    ) shouldBe List.empty

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(ownerService.migrateAllWorkspaceBuckets(
                     List(minimalTestData.wsName, minimalTestData.wsName2, lockedWorkspace.toWorkspaceName)
                   ),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) should not be empty
    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName2),
                 Duration.Inf
    ) should not be empty
    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(lockedWorkspace.toWorkspaceName),
                 Duration.Inf
    ) shouldBe List.empty
  }

  behavior of "adminGetBucketMigrationAttemptsForWorkspace"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonAdminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "getBucketMigrationAttemptsForWorkspace"

  it should "be limited to workspace owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonOwnerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "adminGetBucketMigrationAttemptsForBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(
      adminService.adminGetBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
      Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminGetBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "getBucketMigrationAttemptsForBillingProject"

  it should "be limited to billing project owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.getBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.getBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "adminMigrateWorkspaceBucket"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.adminMigrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminMigrateWorkspaceBucket(minimalTestData.wsName),
        Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "fail when a workspace is locked" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(adminService.adminMigrateWorkspaceBucket(lockedWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail for McWorkspaces" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()

    val mcWorkspace = minimalTestData.workspace.copy(name = "mcWorkspace",
                                                     workspaceType = WorkspaceType.McWorkspace,
                                                     workspaceId = UUID.randomUUID.toString
    )
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(mcWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(adminService.adminMigrateWorkspaceBucket(mcWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail for non-US multiregion buckets" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()

    val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                        workspaceId = UUID.randomUUID.toString,
                                                        bucketName = "nonUsBucket"
    )
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace), Duration.Inf)

    val usBucket = mock[Bucket]
    when(usBucket.getLocation).thenReturn("US")
    when(
      adminService.gcsDAO.getBucket(ArgumentMatchers.eq(minimalTestData.workspace.bucketName),
                                    ArgumentMatchers.eq(minimalTestData.workspace.googleProjectId.some)
      )(any())
    ).thenReturn(Future.successful(Right(usBucket)))

    val nonUsBucket = mock[Bucket]
    when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
    when(
      adminService.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                    ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
      )(any())
    ).thenReturn(Future.successful(Right(nonUsBucket)))

    Await.result(adminService.adminMigrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) should have size 1

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(adminService.adminMigrateWorkspaceBucket(nonUsWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  behavior of "migrateWorkspaceBucket"

  it should "be limited to workspace owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.migrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.migrateWorkspaceBucket(minimalTestData.wsName),
        Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "fail when a workspace is locked" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(ownerService.migrateWorkspaceBucket(lockedWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail for McWorkspaces" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()

    val mcWorkspace = minimalTestData.workspace.copy(name = "mcWorkspace",
                                                     workspaceType = WorkspaceType.McWorkspace,
                                                     workspaceId = UUID.randomUUID.toString
    )
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(mcWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(ownerService.migrateWorkspaceBucket(mcWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail for non-US multiregion buckets" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()

    val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                        workspaceId = UUID.randomUUID.toString,
                                                        bucketName = "nonUsBucket"
    )
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace), Duration.Inf)

    val usBucket = mock[Bucket]
    when(usBucket.getLocation).thenReturn("US")
    when(
      ownerService.gcsDAO.getBucket(ArgumentMatchers.eq(minimalTestData.workspace.bucketName),
                                    ArgumentMatchers.eq(minimalTestData.workspace.googleProjectId.some)
      )(any())
    ).thenReturn(Future.successful(Right(usBucket)))

    val nonUsBucket = mock[Bucket]
    when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
    when(
      ownerService.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                    ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
      )(any())
    ).thenReturn(Future.successful(Right(nonUsBucket)))

    Await.result(ownerService.migrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                 Duration.Inf
    ) should have size 1

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(ownerService.migrateWorkspaceBucket(nonUsWorkspace.toWorkspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  behavior of "adminMigrateAllWorkspaceBuckets"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.adminMigrateAllWorkspaceBuckets(List(minimalTestData.wsName, minimalTestData.wsName2)),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminMigrateAllWorkspaceBuckets(List(minimalTestData.wsName, minimalTestData.wsName2)),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if a non-US bucket is scheduled, but still schedule any eligible workspaces" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                          workspaceId = UUID.randomUUID.toString,
                                                          bucketName = "nonUsBucket"
      )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace), Duration.Inf)

      val usBucket = mock[Bucket]
      when(usBucket.getLocation).thenReturn("US")
      when(
        adminService.gcsDAO.getBucket(ArgumentMatchers.eq(minimalTestData.workspace.bucketName),
                                      ArgumentMatchers.eq(minimalTestData.workspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(usBucket)))

      val nonUsBucket = mock[Bucket]
      when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
      when(
        adminService.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                      ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(nonUsBucket)))

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          adminService.adminMigrateAllWorkspaceBuckets(List(minimalTestData.wsName, nonUsWorkspace.toWorkspaceName)),
          Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

      Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                   Duration.Inf
      ) should have size 1
      Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(nonUsWorkspace.toWorkspaceName),
                   Duration.Inf
      ) should have size 0
  }

  behavior of "migrateAllWorkspaceBuckets"

  it should "be limited to workspace owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.migrateAllWorkspaceBuckets(List(minimalTestData.wsName, minimalTestData.wsName2)),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.migrateAllWorkspaceBuckets(List(minimalTestData.wsName, minimalTestData.wsName2)),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if a non-US bucket is scheduled, but still schedule any eligible workspaces" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()

      val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                          workspaceId = UUID.randomUUID.toString,
                                                          bucketName = "nonUsBucket"
      )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace), Duration.Inf)

      val usBucket = mock[Bucket]
      when(usBucket.getLocation).thenReturn("US")
      when(
        ownerService.gcsDAO.getBucket(ArgumentMatchers.eq(minimalTestData.workspace.bucketName),
                                      ArgumentMatchers.eq(minimalTestData.workspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(usBucket)))

      val nonUsBucket = mock[Bucket]
      when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
      when(
        ownerService.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                      ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(nonUsBucket)))

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          ownerService.migrateAllWorkspaceBuckets(List(minimalTestData.wsName, nonUsWorkspace.toWorkspaceName)),
          Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

      Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(minimalTestData.wsName),
                   Duration.Inf
      ) should have size 1
      Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(nonUsWorkspace.toWorkspaceName),
                   Duration.Inf
      ) should have size 0
  }

  behavior of "adminMigrateWorkspaceBucketsInBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if a non-US bucket is scheduled, but still schedule any eligible workspaces" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()
      val newProjectName = "new-project"
      val newProject = minimalTestData.billingProject.copy(projectName = RawlsBillingProjectName(newProjectName))
      val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                          namespace = newProjectName,
                                                          workspaceId = UUID.randomUUID.toString,
                                                          bucketName = "nonUsBucket"
      )
      val usWorkspace = minimalTestData.workspace.copy(name = "usWorkspace",
                                                       namespace = newProjectName,
                                                       workspaceId = UUID.randomUUID.toString,
                                                       bucketName = "usBucket"
      )

      runAndWait(
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.create(newProject)
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace)
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(usWorkspace)
        } yield (),
        Duration.Inf
      )

      val usBucket = mock[Bucket]
      when(usBucket.getLocation).thenReturn("US")
      when(
        adminService.gcsDAO.getBucket(ArgumentMatchers.eq(usWorkspace.bucketName),
                                      ArgumentMatchers.eq(usWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(usBucket)))

      val nonUsBucket = mock[Bucket]
      when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
      when(
        adminService.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                      ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(nonUsBucket)))

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(adminService.adminMigrateWorkspaceBucketsInBillingProject(newProject.projectName), Duration.Inf)
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

      Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(usWorkspace.toWorkspaceName),
                   Duration.Inf
      ) should have size 1
      Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(nonUsWorkspace.toWorkspaceName),
                   Duration.Inf
      ) should have size 0
  }

  behavior of "migrateWorkspaceBucketsInBillingProject"

  it should "be limited to billing project owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if a non-US bucket is scheduled, but still schedule any eligible workspaces" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()
      val newProjectName = "new-project"
      val newProject = minimalTestData.billingProject.copy(projectName = RawlsBillingProjectName(newProjectName))
      val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                          namespace = newProjectName,
                                                          workspaceId = UUID.randomUUID.toString,
                                                          bucketName = "nonUsBucket"
      )
      val usWorkspace = minimalTestData.workspace.copy(name = "usWorkspace",
                                                       namespace = newProjectName,
                                                       workspaceId = UUID.randomUUID.toString,
                                                       bucketName = "usBucket"
      )

      runAndWait(
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.create(newProject)
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace)
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(usWorkspace)
        } yield (),
        Duration.Inf
      )

      val usBucket = mock[Bucket]
      when(usBucket.getLocation).thenReturn("US")
      when(
        ownerService.gcsDAO.getBucket(ArgumentMatchers.eq(usWorkspace.bucketName),
                                      ArgumentMatchers.eq(usWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(usBucket)))

      val nonUsBucket = mock[Bucket]
      when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
      when(
        ownerService.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                      ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(nonUsBucket)))

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(ownerService.migrateWorkspaceBucketsInBillingProject(newProject.projectName), Duration.Inf)
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

      Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(usWorkspace.toWorkspaceName),
                   Duration.Inf
      ) should have size 1
      Await.result(ownerService.getBucketMigrationAttemptsForWorkspace(nonUsWorkspace.toWorkspaceName),
                   Duration.Inf
      ) should have size 0
  }

  behavior of "adminGetBucketMigrationProgressForWorkspace"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()
    Await.result(adminService.adminMigrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    Await.result(adminService.adminGetBucketMigrationProgressForWorkspace(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminGetBucketMigrationProgressForWorkspace(minimalTestData.wsName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "return the progress of a bucket migration" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()

    Await.result(
      for {
        _ <- adminService.adminMigrateWorkspaceBucket(minimalTestData.workspace.toWorkspaceName)
        _ <- insertSTSJobs(minimalTestData.workspace)
        progressOpt <- adminService.adminGetBucketMigrationProgressForWorkspace(
          minimalTestData.workspace.toWorkspaceName
        )
      } yield verifyBucketMigrationProgress(progressOpt),
      Duration.Inf
    )
  }

  behavior of "getBucketMigrationProgressForWorkspace"

  it should "be limited to workspace owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()
    Await.result(ownerService.migrateWorkspaceBucket(minimalTestData.wsName), Duration.Inf)
    Await.result(ownerService.getBucketMigrationProgressForWorkspace(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.getBucketMigrationProgressForWorkspace(minimalTestData.wsName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "return the progress of a bucket migration" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()

    Await.result(
      for {
        _ <- ownerService.migrateWorkspaceBucket(minimalTestData.workspace.toWorkspaceName)
        _ <- insertSTSJobs(minimalTestData.workspace)
        progressOpt <- ownerService.getBucketMigrationProgressForWorkspace(minimalTestData.workspace.toWorkspaceName)
      } yield verifyBucketMigrationProgress(progressOpt),
      Duration.Inf
    )
  }

  behavior of "adminGetBucketMigrationProgressForBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()
    Await.result(adminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    Await.result(
      adminService.adminGetBucketMigrationProgressForBillingProject(minimalTestData.billingProject.projectName),
      Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminGetBucketMigrationProgressForBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "return the progress of a bucket migration for workspaces that have been migrated" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      Await.result(
        for {
          _ <- adminService.adminMigrateWorkspaceBucket(minimalTestData.workspace.toWorkspaceName)
          _ <- insertSTSJobs(minimalTestData.workspace)
          progressMap <- adminService.adminGetBucketMigrationProgressForBillingProject(
            minimalTestData.billingProject.projectName
          )
        } yield progressMap.map {
          case (wsName, progressOpt) if wsName.equals(minimalTestData.workspace.toWorkspaceName.toString) =>
            verifyBucketMigrationProgress(progressOpt)
          case (unmigratedWsName, Some(progress)) =>
            fail(s"Found unexpected bucket migration progress $progress for unmigrated workspace $unmigratedWsName")
          case (_, _) =>
        },
        Duration.Inf
      )
  }

  behavior of "getBucketMigrationProgressForBillingProject"

  it should "be limited to billing project owners" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()
    Await.result(ownerService.migrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    Await.result(ownerService.getBucketMigrationProgressForBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.getBucketMigrationProgressForBillingProject(minimalTestData.billingProject.projectName),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "return the progress of a bucket migration for workspaces that have been migrated" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()

      Await.result(
        for {
          _ <- ownerService.migrateWorkspaceBucket(minimalTestData.workspace.toWorkspaceName)
          _ <- insertSTSJobs(minimalTestData.workspace)
          progressMap <- ownerService.getBucketMigrationProgressForBillingProject(
            minimalTestData.billingProject.projectName
          )
        } yield progressMap.map {
          case (wsName, progressOpt) if wsName.equals(minimalTestData.workspace.toWorkspaceName.toString) =>
            verifyBucketMigrationProgress(progressOpt)
          case (unmigratedWsName, Some(progress)) =>
            fail(s"Found unexpected bucket migration progress $progress for unmigrated workspace $unmigratedWsName")
          case (_, _) =>
        },
        Duration.Inf
      )
  }

  behavior of "getEligibleOrMigratingWorkspaces"

  it should "return owned workspaces either with US multi-region buckets or that are currently or recently migrated" in withMinimalTestDatabase {
    _ =>
      def makeSamUserResponse(workspaceId: String, role: SamResourceRole): SamUserResource = SamUserResource(
        workspaceId,
        SamRolesAndActions(Set(role), Set.empty),
        SamRolesAndActions(Set.empty, Set.empty),
        SamRolesAndActions(Set.empty, Set.empty),
        Set.empty,
        Set.empty
      )

      def makeWorkspace(name: String, creator: String): Workspace = Workspace(
        minimalTestData.billingProject.projectName.value,
        name,
        UUID.randomUUID().toString,
        UUID.randomUUID().toString,
        None,
        DateTime.now(),
        DateTime.now(),
        creator,
        Map.empty
      )

      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      val gcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val userCtx = getRequestContext("user@example.com")
      val bucketMigrationService = BucketMigrationService.constructor(slickDataSource, samDAO, gcsDAO)(userCtx)

      val nonOwnedWorkspace =
        makeWorkspace("nonOwnedWorkspace", "notThisUser@example.com")
      val usWorkspace = makeWorkspace("usWorkspace", userCtx.userInfo.userEmail.value)
      val nonUSWorkspace = makeWorkspace("nonUSWorkspace", userCtx.userInfo.userEmail.value)
      val migratingWorkspace = makeWorkspace("migratingWorkspace", userCtx.userInfo.userEmail.value)
      val recentlyMigratedWorkspace = makeWorkspace("recentlyMigratedWorkspace", userCtx.userInfo.userEmail.value)
      val oldSuccessfullyMigratedWorkspace =
        makeWorkspace("oldSuccessfullyMigratedWorkspace", userCtx.userInfo.userEmail.value)
      val oldUnsuccessfullyMigratedWorkspace =
        makeWorkspace("oldUnsuccessfullyMigratedWorkspace", userCtx.userInfo.userEmail.value)

      val samResources = Seq(
        makeSamUserResponse(nonOwnedWorkspace.workspaceId, SamWorkspaceRoles.writer),
        makeSamUserResponse(usWorkspace.workspaceId, SamWorkspaceRoles.owner),
        makeSamUserResponse(nonUSWorkspace.workspaceId, SamWorkspaceRoles.owner),
        makeSamUserResponse(migratingWorkspace.workspaceId, SamWorkspaceRoles.owner),
        makeSamUserResponse(recentlyMigratedWorkspace.workspaceId, SamWorkspaceRoles.owner),
        makeSamUserResponse(oldSuccessfullyMigratedWorkspace.workspaceId, SamWorkspaceRoles.owner),
        makeSamUserResponse(oldUnsuccessfullyMigratedWorkspace.workspaceId, SamWorkspaceRoles.owner)
      )
      when(samDAO.listUserResources(SamResourceTypeNames.workspace, userCtx))
        .thenReturn(Future.successful(samResources))

      val usBucket = mock[Bucket]
      when(usBucket.getLocation).thenReturn("US")
      val nonUSBucket = mock[Bucket]
      when(nonUSBucket.getLocation).thenReturn("us-central1")
      when(gcsDAO.getBucket(any(), any())(any())).thenReturn(Future.successful(Right(usBucket)))
      when(gcsDAO.getBucket(ArgumentMatchers.eq(nonUSWorkspace.bucketName), any())(any()))
        .thenReturn(Future.successful(Right(nonUSBucket)))

      Await.result(
        slickDataSource.inTransaction { dataAccess =>
          for {
            _ <- dataAccess.workspaceQuery.createOrUpdate(nonOwnedWorkspace)
            _ <- dataAccess.workspaceQuery.createOrUpdate(usWorkspace)
            _ <- dataAccess.workspaceQuery.createOrUpdate(nonUSWorkspace)
            _ <- dataAccess.workspaceQuery.createOrUpdate(migratingWorkspace)
            _ <- dataAccess.workspaceQuery.createOrUpdate(recentlyMigratedWorkspace)
            _ <- dataAccess.workspaceQuery.createOrUpdate(oldSuccessfullyMigratedWorkspace)
            _ <- dataAccess.workspaceQuery.createOrUpdate(oldUnsuccessfullyMigratedWorkspace)

            _ <- dataAccess.multiregionalBucketMigrationQuery.schedule(migratingWorkspace, "US".some)
            _ <- dataAccess.multiregionalBucketMigrationQuery.schedule(recentlyMigratedWorkspace, "US".some)
            _ <- dataAccess.multiregionalBucketMigrationQuery.schedule(oldSuccessfullyMigratedWorkspace, "US".some)
            _ <- dataAccess.multiregionalBucketMigrationQuery.schedule(oldUnsuccessfullyMigratedWorkspace, "US".some)

            recentlyMigratedAttempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(recentlyMigratedWorkspace.workspaceIdAsUUID)
              .value
            oldSuccessfullyMigratedAttempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(oldSuccessfullyMigratedWorkspace.workspaceIdAsUUID)
              .value
            oldUnsuccessfullyMigratedAttempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(oldUnsuccessfullyMigratedWorkspace.workspaceIdAsUUID)
              .value

            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              recentlyMigratedAttempt.getOrElse(fail()).id,
              multiregionalBucketMigrationQuery.finishedCol,
              Timestamp.from(Instant.now().minusSeconds(86400)).some,
              multiregionalBucketMigrationQuery.outcomeCol,
              "Success".some
            )
            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              oldSuccessfullyMigratedAttempt.getOrElse(fail()).id,
              multiregionalBucketMigrationQuery.finishedCol,
              Timestamp.from(Instant.now().minusSeconds(86400 * 10)).some,
              multiregionalBucketMigrationQuery.outcomeCol,
              "Success".some
            )
            _ <- dataAccess.multiregionalBucketMigrationQuery.update3(
              oldUnsuccessfullyMigratedAttempt.getOrElse(fail()).id,
              multiregionalBucketMigrationQuery.finishedCol,
              Timestamp.from(Instant.now().minusSeconds(86400 * 10)).some,
              multiregionalBucketMigrationQuery.outcomeCol,
              "Failure".some,
              multiregionalBucketMigrationQuery.messageCol,
              "error".some
            )
          } yield ()
        },
        Duration.Inf
      )

      val expectedWorkspaces = List(
        usWorkspace.toWorkspaceName.toString,
        migratingWorkspace.toWorkspaceName.toString,
        recentlyMigratedWorkspace.toWorkspaceName.toString,
        oldUnsuccessfullyMigratedWorkspace.toWorkspaceName.toString
      )

      val returnedWorkspaces = Await.result(bucketMigrationService.getEligibleOrMigratingWorkspaces, Duration.Inf)

      returnedWorkspaces.keys should contain theSameElementsAs expectedWorkspaces
      returnedWorkspaces(usWorkspace.toWorkspaceName.toString) shouldBe None
      returnedWorkspaces(migratingWorkspace.toWorkspaceName.toString).get.outcome shouldBe None
      returnedWorkspaces(recentlyMigratedWorkspace.toWorkspaceName.toString).get.outcome shouldBe Some(Outcome.Success)
      returnedWorkspaces(oldUnsuccessfullyMigratedWorkspace.toWorkspaceName.toString).get.outcome shouldBe Some(
        Outcome.Failure("error")
      )
  }

  behavior of "adminGetBucketMigrationProgressForWorkspaces"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()
    Await.result(adminService.adminMigrateAllWorkspaceBuckets(Seq(minimalTestData.wsName, minimalTestData.wsName2)),
      Duration.Inf
    )
    Await.result(
      adminService.adminGetBucketMigrationProgressForWorkspaces(Seq(minimalTestData.wsName, minimalTestData.wsName2)),
      Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonAdminService.adminGetBucketMigrationProgressForWorkspaces(Seq(minimalTestData.wsName, minimalTestData.wsName2)),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "return the progress of a bucket migration for workspaces that have been migrated" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      Await.result(
        for {
          _ <- adminService.adminMigrateWorkspaceBucket(minimalTestData.workspace.toWorkspaceName)
          _ <- insertSTSJobs(minimalTestData.workspace)
          progressMap <- adminService.adminGetBucketMigrationProgressForWorkspaces(
            Seq(minimalTestData.workspace.toWorkspaceName, minimalTestData.workspace2.toWorkspaceName)
          )
        } yield {
          progressMap.keys should contain theSameElementsAs Seq(minimalTestData.workspace.toWorkspaceName.toString, minimalTestData.workspace2.toWorkspaceName.toString)
          verifyBucketMigrationProgress(progressMap(minimalTestData.workspace.toWorkspaceName.toString))
          progressMap(minimalTestData.workspace2.toWorkspaceName.toString) shouldBe None
        },
        Duration.Inf
      )
  }

  behavior of "getBucketMigrationProgressForWorkspaces"

  it should "be limited to users that own all requested workspaces" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()
    Await.result(ownerService.migrateAllWorkspaceBuckets(Seq(minimalTestData.wsName, minimalTestData.wsName2)),
      Duration.Inf
    )
    Await.result(
      ownerService.getBucketMigrationProgressForWorkspaces(Seq(minimalTestData.wsName, minimalTestData.wsName2)),
      Duration.Inf
    )

    when(nonOwnerService.samDAO.userHasAction(SamResourceTypeNames.workspace, minimalTestData.workspace.workspaceId, SamWorkspaceActions.own, nonOwnerService.ctx)).thenReturn(Future.successful(true))
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        nonOwnerService.getBucketMigrationProgressForWorkspaces(Seq(minimalTestData.wsName, minimalTestData.wsName2)),
        Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)

  }

  it should "return the progress of a bucket migration for workspaces that have been migrated" in withMinimalTestDatabase {
    _ =>
      val ownerService = mockBucketMigrationServiceForOwner()

      Await.result(
        for {
          _ <- ownerService.migrateWorkspaceBucket(minimalTestData.workspace.toWorkspaceName)
          _ <- insertSTSJobs(minimalTestData.workspace)
          progressMap <- ownerService.getBucketMigrationProgressForWorkspaces(
            Seq(minimalTestData.workspace.toWorkspaceName, minimalTestData.workspace2.toWorkspaceName)
          )
        } yield {
          progressMap.keys should contain theSameElementsAs Seq(minimalTestData.workspace.toWorkspaceName.toString, minimalTestData.workspace2.toWorkspaceName.toString)
          verifyBucketMigrationProgress(progressMap(minimalTestData.workspace.toWorkspaceName.toString))
          progressMap(minimalTestData.workspace2.toWorkspaceName.toString) shouldBe None
        },
        Duration.Inf
      )
  }

  private def insertSTSJobs(workspace: Workspace): Future[Unit] =
    slickDataSource.inTransaction { dataAccess =>
      import dataAccess.driver.api._
      for {
        attemptOpt <- dataAccess.multiregionalBucketMigrationQuery
          .getAttempt(workspace.workspaceIdAsUUID)
          .value
        attempt = attemptOpt.getOrElse(fail("migration not found"))
        _ <- MultiregionalStorageTransferJobs.storageTransferJobs.map(job =>
          (job.jobName,
           job.migrationId,
           job.destBucket,
           job.sourceBucket,
           job.totalBytesToTransfer,
           job.bytesTransferred,
           job.totalObjectsToTransfer,
           job.objectsTransferred
          )
        ) forceInsertAll List(
          ("jobName", attempt.id, workspace.bucketName, "tempBucketName", 100L.some, 50L.some, 4L.some, 2L.some),
          ("jobName", attempt.id, "tempBucketName", workspace.bucketName, 100L.some, 100L.some, 4L.some, 4L.some)
        )
      } yield ()
    }

  private def verifyBucketMigrationProgress(progressOpt: Option[MultiregionalBucketMigrationProgress]) = {
    val progress = progressOpt.getOrElse(fail("unable to find bucket migration progress"))
    progress.migrationStep shouldBe MultiregionalBucketMigrationStep.ScheduledForMigration
    progress.outcome shouldBe None
    progress.tempBucketTransferProgress shouldBe STSJobProgress(100L, 100L, 4L, 4L).some
    progress.finalBucketTransferProgress shouldBe STSJobProgress(100L, 50L, 4L, 2L).some
  }
}
