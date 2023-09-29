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
  SamUserStatusResponse,
  UserInfo,
  Workspace,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.monitor.migration.{
  MultiregionalBucketMigrationProgress,
  MultiregionalBucketMigrationStep,
  MultiregionalStorageTransferJobs,
  STSJobProgress
}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
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

  it should "schedule a single workspace and return past migration attempts" in withMinimalTestDatabase { _ =>
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

  it should "schedule all workspaces in a billing project and return the migration attempts" in withMinimalTestDatabase {
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

  it should "schedule any eligible workspaces in a billing project for migration and return any failures" in withMinimalTestDatabase {
    _ =>
      val adminService = mockBucketMigrationServiceForAdminUser()

      val lockedWorkspace =
        minimalTestData.workspace.copy(name = "lockedWorkspace",
                                       isLocked = true,
                                       workspaceId = UUID.randomUUID.toString
        )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(adminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                     Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      Await
        .result(adminService.adminGetBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
                Duration.Inf
        )
        .foreach {
          case (name, attempts) if name.equals(lockedWorkspace.toWorkspaceName.toString) => attempts shouldBe List.empty
          case (_, attempts)                                                             => attempts should not be empty
        }
  }

  it should "schedule all workspaces and return any errors that occur" in withMinimalTestDatabase { _ =>
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

  behavior of "getBucketMigrationAttemptsForWorkspace"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonAdminService.adminGetBucketMigrationAttemptsForWorkspace(minimalTestData.wsName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  behavior of "getBucketMigrationAttemptsForBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.adminGetBucketMigrationAttemptsForBillingProject(minimalTestData.billingProject.projectName),
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

  behavior of "migrateWorkspaceBucket"

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

  behavior of "migrateAllWorkspaceBuckets"

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

  behavior of "migrateWorkspaceBucketsInBillingProject"

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

  behavior of "getBucketMigrationProgressForWorkspace"

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
        progressOpt <- adminService.adminGetBucketMigrationProgressForWorkspace(minimalTestData.workspace.toWorkspaceName)
      } yield verifyBucketMigrationProgress(progressOpt),
      Duration.Inf
    )
  }

  behavior of "getBucketMigrationProgressForBillingProject"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()
    Await.result(adminService.adminMigrateWorkspaceBucketsInBillingProject(minimalTestData.billingProject.projectName),
                 Duration.Inf
    )
    Await.result(adminService.adminGetBucketMigrationProgressForBillingProject(minimalTestData.billingProject.projectName),
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
