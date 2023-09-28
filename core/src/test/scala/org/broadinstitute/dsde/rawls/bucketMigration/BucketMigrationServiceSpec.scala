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
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
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

  it should "schedule a single workspace and return past migration attempts" in withMinimalTestDatabase { _ =>
    val service = mockBucketMigrationServiceForAdminUser()

    Await.result(
      for {
        preMigrationAttempts <- service.getBucketMigrationAttemptsForWorkspaceInternal(minimalTestData.workspace)
        _ <- service.migrateWorkspaceBucketInternal(minimalTestData.workspace)
        postMigrationAttempts <- service.getBucketMigrationAttemptsForWorkspaceInternal(minimalTestData.workspace)
      } yield {
        preMigrationAttempts shouldBe List.empty
        postMigrationAttempts should not be empty
      },
      Duration.Inf
    )
  }

  it should "schedule multiple workspaces and return the migration attempts" in withMinimalTestDatabase { _ =>
    val service = mockBucketMigrationServiceForAdminUser()

    Await.result(
      for {
        preMigrationAttempts <- service.getBucketMigrationAttemptsForWorkspacesInternal(
          Seq(minimalTestData.workspace, minimalTestData.workspace2)
        )
        _ <- service.migrateWorkspaceBuckets(Seq(minimalTestData.workspace, minimalTestData.workspace2))
        postMigrationAttempts <- service.getBucketMigrationAttemptsForWorkspacesInternal(
          Seq(minimalTestData.workspace, minimalTestData.workspace2)
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
      val service = mockBucketMigrationServiceForAdminUser()

      val lockedWorkspace =
        minimalTestData.workspace.copy(name = "lockedWorkspace",
                                       isLocked = true,
                                       workspaceId = UUID.randomUUID.toString
        )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          service.migrateWorkspaceBuckets(Seq(minimalTestData.workspace, lockedWorkspace)),
          Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      Await
        .result(
          service.getBucketMigrationAttemptsForWorkspacesInternal(Seq(minimalTestData.workspace, lockedWorkspace)),
          Duration.Inf
        )
        .foreach {
          case (name, attempts) if name.equals(lockedWorkspace.toWorkspaceName.toString) => attempts shouldBe List.empty
          case (_, attempts)                                                             => attempts should not be empty
        }
  }

  behavior of "asFCAdminWithWorkspace"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.asFCAdminWithWorkspace(minimalTestData.wsName)(_ => Future.successful(())), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonAdminService.asFCAdminWithWorkspace(minimalTestData.wsName)(_ => Future.successful(())),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "load the correct workspace" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()
    Await.result(
      adminService.asFCAdminWithWorkspace(minimalTestData.wsName) { workspace =>
        workspace.toWorkspaceName shouldBe minimalTestData.wsName
        Future.successful(())
      },
      Duration.Inf
    )
  }

  behavior of "asFCAdminWithBillingProjectWorkspaces"

  it should "be limited to fc-admins" in withMinimalTestDatabase { _ =>
    val (adminService, nonAdminService) = mockAdminEnforcementTest()

    Await.result(adminService.asFCAdminWithBillingProjectWorkspaces(minimalTestData.billingProject.projectName)(_ =>
                   Future.successful(())
                 ),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonAdminService.asFCAdminWithBillingProjectWorkspaces(minimalTestData.billingProject.projectName)(
                     _ => Future.successful(())
                   ),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "load the correct workspace" in withMinimalTestDatabase { _ =>
    val adminService = mockBucketMigrationServiceForAdminUser()
    val billingProject =
      minimalTestData.billingProject.copy(projectName = RawlsBillingProjectName("billingProjectName"))
    val workspace1 = minimalTestData.workspace.copy(namespace = billingProject.projectName.value,
                                                    name = UUID.randomUUID().toString,
                                                    workspaceId = UUID.randomUUID().toString
    )
    val workspace2 = minimalTestData.workspace.copy(namespace = billingProject.projectName.value,
                                                    name = UUID.randomUUID().toString,
                                                    workspaceId = UUID.randomUUID().toString
    )

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.rawlsBillingProjectQuery.create(billingProject)
          _ <- dataAccess.workspaceQuery.createOrUpdate(workspace1)
          _ <- dataAccess.workspaceQuery.createOrUpdate(workspace2)
        } yield ()
      },
      Duration.Inf
    )
    Await.result(
      adminService.asFCAdminWithBillingProjectWorkspaces(billingProject.projectName) { workspaces =>
        workspaces.map(_.toWorkspaceName) should contain theSameElementsAs List(workspace1.toWorkspaceName,
                                                                                workspace2.toWorkspaceName
        )
        Future.successful(())
      },
      Duration.Inf
    )
  }

  behavior of "asOwnerWithWorkspace"

  it should "require owner access to the workspace" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.asOwnerWithWorkspace(minimalTestData.wsName)(_ => Future.successful(())), Duration.Inf)
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonOwnerService.asOwnerWithWorkspace(minimalTestData.wsName)(_ => Future.successful(())),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "load the correct workspace" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()
    Await.result(
      ownerService.asOwnerWithWorkspace(minimalTestData.wsName) { workspace =>
        workspace.toWorkspaceName shouldBe minimalTestData.wsName
        Future.successful(())
      },
      Duration.Inf
    )
  }

  behavior of "asOwnerWithBillingProjectWorkspaces"

  it should "require owner access to the workspace" in withMinimalTestDatabase { _ =>
    val (ownerService, nonOwnerService) = mockOwnerEnforcementTest()

    Await.result(ownerService.asOwnerWithBillingProjectWorkspaces(minimalTestData.billingProject.projectName)(_ =>
                   Future.successful(())
                 ),
                 Duration.Inf
    )
    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(nonOwnerService.asOwnerWithBillingProjectWorkspaces(minimalTestData.billingProject.projectName)(_ =>
                     Future.successful(())
                   ),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "load the correct workspace" in withMinimalTestDatabase { _ =>
    val ownerService = mockBucketMigrationServiceForOwner()
    val billingProject =
      minimalTestData.billingProject.copy(projectName = RawlsBillingProjectName("billingProjectName"))
    val workspace1 = minimalTestData.workspace.copy(namespace = billingProject.projectName.value,
                                                    name = UUID.randomUUID().toString,
                                                    workspaceId = UUID.randomUUID().toString
    )
    val workspace2 = minimalTestData.workspace.copy(namespace = billingProject.projectName.value,
                                                    name = UUID.randomUUID().toString,
                                                    workspaceId = UUID.randomUUID().toString
    )

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.rawlsBillingProjectQuery.create(billingProject)
          _ <- dataAccess.workspaceQuery.createOrUpdate(workspace1)
          _ <- dataAccess.workspaceQuery.createOrUpdate(workspace2)
        } yield ()
      },
      Duration.Inf
    )
    Await.result(
      ownerService.asOwnerWithBillingProjectWorkspaces(billingProject.projectName) { workspaces =>
        workspaces.map(_.toWorkspaceName) should contain theSameElementsAs List(workspace1.toWorkspaceName,
                                                                                workspace2.toWorkspaceName
        )
        Future.successful(())
      },
      Duration.Inf
    )
  }

  behavior of "migrateWorkspaceBucketInternal"

  it should "fail when a workspace is locked" in withMinimalTestDatabase { _ =>
    val service = mockBucketMigrationServiceForAdminUser()

    val lockedWorkspace =
      minimalTestData.workspace.copy(name = "lockedWorkspace", isLocked = true, workspaceId = UUID.randomUUID.toString)
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(lockedWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.migrateWorkspaceBucketInternal(lockedWorkspace), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail for McWorkspaces" in withMinimalTestDatabase { _ =>
    val service = mockBucketMigrationServiceForAdminUser()

    val mcWorkspace = minimalTestData.workspace.copy(name = "mcWorkspace",
                                                     workspaceType = WorkspaceType.McWorkspace,
                                                     workspaceId = UUID.randomUUID.toString
    )
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(mcWorkspace), Duration.Inf)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.migrateWorkspaceBucketInternal(mcWorkspace), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail for non-US multiregion buckets" in withMinimalTestDatabase { _ =>
    val service = mockBucketMigrationServiceForAdminUser()

    val usWorkspace = minimalTestData.workspace
    val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                        workspaceId = UUID.randomUUID.toString,
                                                        bucketName = "nonUsBucket"
    )
    runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace), Duration.Inf)

    val usBucket = mock[Bucket]
    when(usBucket.getLocation).thenReturn("US")
    when(
      service.gcsDAO.getBucket(ArgumentMatchers.eq(usWorkspace.bucketName),
                               ArgumentMatchers.eq(usWorkspace.googleProjectId.some)
      )(any())
    ).thenReturn(Future.successful(Right(usBucket)))

    val nonUsBucket = mock[Bucket]
    when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
    when(
      service.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                               ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
      )(any())
    ).thenReturn(Future.successful(Right(nonUsBucket)))

    Await.result(service.migrateWorkspaceBucketInternal(usWorkspace), Duration.Inf)
    Await.result(service.getBucketMigrationAttemptsForWorkspaceInternal(usWorkspace), Duration.Inf) should have size 1

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.migrateWorkspaceBucketInternal(nonUsWorkspace), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  behavior of "migrateWorkspaceBuckets"

  it should "throw an exception if a non-US bucket is scheduled, but still schedule any eligible workspaces" in withMinimalTestDatabase {
    _ =>
      val service = mockBucketMigrationServiceForAdminUser()

      val usWorkspace = minimalTestData.workspace
      val nonUsWorkspace = minimalTestData.workspace.copy(name = "nonUsWorkspace",
                                                          workspaceId = UUID.randomUUID.toString,
                                                          bucketName = "nonUsBucket"
      )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(nonUsWorkspace), Duration.Inf)

      val usBucket = mock[Bucket]
      when(usBucket.getLocation).thenReturn("US")
      when(
        service.gcsDAO.getBucket(ArgumentMatchers.eq(usWorkspace.bucketName),
                                 ArgumentMatchers.eq(usWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(usBucket)))

      val nonUsBucket = mock[Bucket]
      when(nonUsBucket.getLocation).thenReturn("northamerica-northeast1")
      when(
        service.gcsDAO.getBucket(ArgumentMatchers.eq(nonUsWorkspace.bucketName),
                                 ArgumentMatchers.eq(nonUsWorkspace.googleProjectId.some)
        )(any())
      ).thenReturn(Future.successful(Right(nonUsBucket)))

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          service.migrateWorkspaceBuckets(Seq(usWorkspace, nonUsWorkspace)),
          Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

      Await.result(service.getBucketMigrationAttemptsForWorkspaceInternal(usWorkspace), Duration.Inf) should have size 1
      Await.result(service.getBucketMigrationAttemptsForWorkspaceInternal(nonUsWorkspace),
                   Duration.Inf
      ) should have size 0
  }

  behavior of "getBucketMigrationProgressForWorkspaceInternal"

  it should "return the progress of a bucket migration" in withMinimalTestDatabase { _ =>
    val service = mockBucketMigrationServiceForAdminUser()

    Await.result(
      for {
        _ <- service.migrateWorkspaceBucketInternal(minimalTestData.workspace)
        _ <- insertSTSJobs(minimalTestData.workspace)
        progressOpt <- service.getBucketMigrationProgressForWorkspaceInternal(
          minimalTestData.workspace
        )
      } yield verifyBucketMigrationProgress(progressOpt),
      Duration.Inf
    )
  }

  behavior of "getBucketMigrationProgressForWorkspacesInternal"

  it should "return the progress of a bucket migration for workspaces that have been migrated" in withMinimalTestDatabase {
    _ =>
      val service = mockBucketMigrationServiceForAdminUser()

      Await.result(
        for {
          _ <- service.migrateWorkspaceBucketInternal(minimalTestData.workspace)
          _ <- insertSTSJobs(minimalTestData.workspace)
          progressMap <- service.getBucketMigrationProgressForWorkspacesInternal(
            Seq(minimalTestData.workspace, minimalTestData.workspace2)
          )
        } yield {
          progressMap.keys should contain theSameElementsAs List(minimalTestData.wsName.toString,
                                                                 minimalTestData.wsName2.toString
          )
          verifyBucketMigrationProgress(progressMap(minimalTestData.wsName.toString))
          progressMap(minimalTestData.wsName2.toString) shouldBe None
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
