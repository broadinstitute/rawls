package org.broadinstitute.dsde.rawls.monitor

import akka.http.scaladsl.model.StatusCodes
import cats.data.{NonEmptyList, OptionT}
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.api.services.cloudresourcemanager.model.{Binding, Project}
import com.google.cloud.Identity
import com.google.cloud.storage.{Acl, BucketInfo, Storage}
import com.google.common.collect.ImmutableList
import com.google.rpc.Code
import com.google.storagetransfer.v1.proto.TransferTypes.{ErrorLogEntry, ErrorSummary, TransferJob, TransferOperation}
import io.grpc.{Status, StatusRuntimeException}
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.MockGoogleServicesDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationActor._
import org.broadinstitute.dsde.rawls.monitor.migration.{FailureModes, PpwStorageTransferJob}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceServiceSpec
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobName, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.google2.mock.{BaseFakeGoogleStorage, MockGoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageTransferService, StorageRole}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.scalactic.source
import org.scalatest.Inspectors.forAll
import org.scalatest.concurrent.Eventually
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues, Succeeded}
import slick.jdbc.MySQLProfile.api._
import spray.json.{JsObject, JsString}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArraySet}
import scala.annotation.nowarn
import scala.concurrent.Future
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.language.postfixOps
import scala.util.Random

class WorkspaceMigrationActorSpec extends AnyFlatSpecLike with Matchers with Eventually with OptionValues {

  implicit val logger = new ConsoleLogger("unit_test", LogLevel(false, false, true, true))
  implicit val ec = IORuntime.global.compute
  implicit val timestampOrdering = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
  implicit class FailureMessageOps(outcome: Outcome)(implicit pos: source.Position) {
    def failureMessage: String = outcome match {
      case Failure(message) => message
      case _ =>
        throw new TestFailedException(_ => Some(s"""Expected "Failure", instead got "${outcome}"."""), None, pos)
    }
  }

  // This is a horrible hack to avoid refactoring the tangled mess in the WorkspaceServiceSpec.
  val spec = new WorkspaceServiceSpec()

  object testData {
    val billingProject = spec.testData.billingProject.copy(
      projectName = RawlsBillingProjectName("test-billing-project")
    )

    val billingProject2 = billingProject.copy(
      projectName = RawlsBillingProjectName("another-test-billing-project")
    )

    val v1Workspace = spec.testData.v1Workspace.copy(
      namespace = billingProject.projectName.value,
      workspaceId = UUID.randomUUID().toString,
      googleProjectId = billingProject.googleProjectId
    )

    val v1Workspace2 = v1Workspace.copy(
      name = UUID.randomUUID().toString,
      workspaceId = UUID.randomUUID().toString
    )

    val v1Workspace3 = v1Workspace.copy(
      namespace = billingProject2.projectName.value,
      workspaceId = UUID.randomUUID().toString
    )

    val communityWorkbenchFolderId = GoogleFolderId("folders/123456789")
  }

  def runMigrationTest(test: MigrateAction[Assertion]): Assertion =
    spec.withEmptyTestDatabase { dataSource =>
      spec.withServices(dataSource, spec.testData.userOwner) { services =>
        services.gcsDAO
          .getServiceAccountUserInfo()
          .io
          .flatMap { userInfo =>
            (populateDb *> test)
              .run(
                MigrationDeps(
                  services.slickDataSource,
                  GoogleProject("fake-google-project"),
                  testData.communityWorkbenchFolderId,
                  maxConcurrentAttempts = 0,
                  maxRetries = 1,
                  services.workspaceServiceConstructor(RawlsRequestContext(userInfo)),
                  MockStorageService(),
                  MockStorageTransferService(),
                  userInfo,
                  services.gcsDAO,
                  new MockGoogleIamDAO,
                  services.samDAO
                )
              )
              .value
          }
          .unsafeRunSync
          .getOrElse(throw new AssertionError("The test exited prematurely."))
      }
    }

  def allowOne[A]: MigrateAction[A] => MigrateAction[A] =
    MigrateAction.local(_.copy(maxConcurrentAttempts = 1))

  case class MockStorageTransferService() extends MockGoogleStorageTransferService[IO] {
    override def getStsServiceAccount(project: GoogleProject): IO[ServiceAccount] =
      IO.pure {
        ServiceAccount(
          ServiceAccountSubjectId("fake-storage-transfer-service"),
          WorkbenchEmail("project-1234@storage-transfer-service.iam.gserviceaccount.com"),
          ServiceAccountDisplayName("Fake Google Storage Transfer Service")
        )
      }

    override def createTransferJob(jobName: JobName,
                                   jobDescription: String,
                                   projectToBill: GoogleProject,
                                   originBucket: GcsBucketName,
                                   destinationBucket: GcsBucketName,
                                   schedule: JobTransferSchedule,
                                   options: Option[GoogleStorageTransferService.JobTransferOptions]
    ): IO[TransferJob] =
      IO.pure {
        TransferJob.newBuilder
          .setName(s"$jobName")
          .setDescription(jobDescription)
          .setProjectId(s"$projectToBill")
          .build
      }

    override def getTransferJob(jobName: JobName, project: GoogleProject): IO[TransferJob] =
      IO.delay {
        val operationName =
          s"transferOperations/${jobName.value.replace("/", "-")}-" +
            s"${Random.nextInt(Integer.MAX_VALUE)}"
        TransferJob.newBuilder
          .setName(jobName.value)
          .setProjectId(project.value)
          .setStatus(TransferJob.Status.ENABLED)
          .setLatestOperationName(operationName)
          .build
      }

    override def getTransferOperation(
      operationName: GoogleStorageTransferService.OperationName
    ): IO[TransferOperation] =
      IO.delay {
        val now = Instant.now
        val timestamp = com.google.protobuf.Timestamp.newBuilder
          .setSeconds(now.getEpochSecond)
          .setNanos(now.getNano)
          .build
        val jobName = "(?:transferOperations/)(?<name>.+)(?=-\\d+$)".r
          .findFirstMatchIn(operationName.value)
          .value
          .group("name")
          .replaceFirst("-", "/")
        TransferOperation.newBuilder
          .setName(operationName.value)
          .setTransferJobName(jobName)
          .setProjectId("fake-google-project")
          .setStatus(TransferOperation.Status.SUCCESS)
          .setEndTime(timestamp)
          .build
      }
  }

  case class MockStorageService() extends BaseFakeGoogleStorage {
    override def getBucket(googleProject: GoogleProject,
                           bucketName: GcsBucketName,
                           bucketGetOptions: List[Storage.BucketGetOption],
                           traceId: Option[TraceId]
    ): IO[Option[BucketInfo]] =
      IO.pure(BucketInfo.newBuilder(bucketName.value).setRequesterPays(true).build().some)

    override def removeIamPolicy(bucketName: GcsBucketName,
                                 rolesToRemove: Map[StorageRole, NonEmptyList[Identity]],
                                 traceId: Option[TraceId],
                                 retryConfig: RetryConfig,
                                 bucketSourceOptions: List[Storage.BucketSourceOption]
    ): fs2.Stream[IO, Unit] =
      fs2.Stream.emit()
  }

  def populateDb: MigrateAction[Unit] =
    inTransaction(_.rawlsBillingProjectQuery.create(testData.billingProject)).void

  def createAndScheduleWorkspace(workspace: Workspace): ReadWriteAction[Long] =
    spec.workspaceQuery.createOrUpdate(workspace) *>
      spec.workspaceMigrationQuery.schedule(workspace.toWorkspaceName)

  def writeBucketIamRevoked(workspaceId: UUID): ReadWriteAction[Unit] =
    spec.workspaceMigrationQuery
      .getAttempt(workspaceId)
      .value
      .flatMap(_.traverse_ { attempt =>
        spec.workspaceMigrationQuery.update(attempt.id,
                                            spec.workspaceMigrationQuery.workspaceBucketIamRemovedCol,
                                            Timestamp.from(Instant.now()).some
        )
      })

  "isMigrating" should "return false when a workspace is not being migrated" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(spec.workspaceMigrationQuery.isMigrating(spec.minimalTestData.v1Workspace)) shouldBe false
    }

  "schedule" should "error when a workspace is scheduled concurrently" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(spec.workspaceMigrationQuery.schedule(spec.minimalTestData.v1Workspace.toWorkspaceName))
      assertThrows[RawlsExceptionWithErrorReport] {
        spec.runAndWait(spec.workspaceMigrationQuery.schedule(spec.minimalTestData.v1Workspace.toWorkspaceName))
      }
    }

  it should "return normalized ids rather than real ids" in
    spec.withMinimalTestDatabase { _ =>
      import spec.minimalTestData
      import spec.workspaceMigrationQuery.{getAttempt, scheduleAndGetMetadata, setMigrationFinished}
      spec.runAndWait {
        for {
          a <- scheduleAndGetMetadata(minimalTestData.v1Workspace.toWorkspaceName)
          b <- scheduleAndGetMetadata(minimalTestData.v1Workspace2.toWorkspaceName)
          attempt <- getAttempt(minimalTestData.v1Workspace.workspaceIdAsUUID).value
          _ <- setMigrationFinished(attempt.value.id, Timestamp.from(Instant.now()), Success)
        } yield {
          a.id shouldBe 0
          b.id shouldBe 0
        }
      }
      spec.runAndWait(scheduleAndGetMetadata(minimalTestData.v1Workspace.toWorkspaceName)).id shouldBe 1
    }

  it should "fail to schedule V2 workspaces" in
    spec.withMinimalTestDatabase { _ =>
      val error = intercept[RawlsExceptionWithErrorReport] {
        spec.runAndWait {
          spec.workspaceMigrationQuery.schedule(spec.minimalTestData.workspace.toWorkspaceName)
        }
      }

      error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      error.errorReport.message should include("is not a V1 workspace")
    }

  "updated" should "automagically get bumped to the current timestamp when the record is updated" in
    runMigrationTest {
      for {
        before <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        now <- nowTimestamp
        after <- inTransactionT { dataAccess =>
          OptionT.liftF(
            dataAccess.workspaceMigrationQuery.update(before.id,
                                                      dataAccess.workspaceMigrationQuery.newGoogleProjectConfiguredCol,
                                                      now.some
            )
          ) *>
            dataAccess.workspaceMigrationQuery.getAttempt(before.id)
        }
      } yield before.updated should be < after.updated
    }

  "migrate" should "start a queued migration attempt and lock the workspace" in
    runMigrationTest {
      for {
        migrationId <- inTransaction { _ =>
          createAndScheduleWorkspace(testData.v1Workspace)
        }

        _ <- migrate
        (attempt, workspace) <- inTransactionT { dataAccess =>
          for {
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(migrationId)
            workspace <- OptionT[ReadWriteAction, Workspace] {
              dataAccess.workspaceQuery.findById(testData.v1Workspace.workspaceId)
            }
          } yield (attempt, workspace)
        }
      } yield {
        attempt.started shouldBe defined
        workspace.isLocked shouldBe true
      }
    }

  it should "not start more than the configured number of concurrent resource-limited migration attempts" in
    runMigrationTest {
      val workspaces = List(testData.v1Workspace, testData.v1Workspace2)
      @nowarn("msg=not.*?exhaustive")
      val test = for {
        _ <- inTransaction(_ => workspaces.traverse_(createAndScheduleWorkspace))
        _ <- allowOne(migrate *> migrate)
        Some(Seq(attempt1, attempt2)) <- inTransaction { dataAccess =>
          workspaces
            .traverse(w => dataAccess.workspaceMigrationQuery.getAttempt(w.workspaceIdAsUUID).value)
            .map(_.sequence)
        }
      } yield {
        attempt1.started shouldBe defined
        attempt2.started shouldBe empty
      }
      test
    }

  it should "start more than the configured number of concurrent only-child migration attempts" in
    runMigrationTest {
      for {
        _ <- inTransaction(_ => createAndScheduleWorkspace(testData.v1Workspace))
        _ <- MigrateAction.local(_.copy(maxConcurrentAttempts = 0))(migrate)
        attempt <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield attempt.started shouldBe defined
    }

  it should "not start migrating a workspace with an active submission" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataAccess =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            dataAccess.methodConfigurationQuery.create(spec.testData.v1Workspace, spec.testData.agoraMethodConfig) >>
            dataAccess.entityQuery.save(
              spec.testData.v1Workspace,
              Seq(
                spec.testData.aliquot1,
                spec.testData.sample1,
                spec.testData.sample2,
                spec.testData.sample3,
                spec.testData.sset1,
                spec.testData.indiv1
              )
            ) >>
            dataAccess.submissionQuery.create(spec.testData.v1Workspace, spec.testData.submission1)(
              _ => spec.metrics.counter("test"),
              _ => None
            )
        }

        _ <- migrate
        attempt <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield attempt.started shouldBe empty
    }

  it should "not start any new resource-limited migrations when transfer jobs are being rate-limited" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.workspaceMigrationQuery._
          for {
            _ <- dataAccess.rawlsBillingProjectQuery.create(testData.billingProject2)
            _ <- List(testData.v1Workspace, testData.v1Workspace2, testData.v1Workspace3)
              .traverse_(createAndScheduleWorkspace)
            attempt <- getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- attempt.traverse_ { a =>
              update(a.id, startedCol, Some(now)) *> setMigrationFinished(
                a.id,
                now,
                Failure(
                  "io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED: Quota exceeded for quota metric " +
                    "'Create requests' and limit 'Create requests per day' of service 'storagetransfer.googleapis.com' " +
                    "for consumer 'project_number:635957978953'."
                )
              )
            }
          } yield ()
        }

        _ <- migrate *> migrate

        (w2Attempt, w3Attempt) <- inTransactionT { dataAccess =>
          for {
            w2 <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace2.workspaceIdAsUUID)
            w3 <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace3.workspaceIdAsUUID)
          } yield (w2, w3)
        }

      } yield {
        w2Attempt.started shouldBe empty
        w3Attempt.started shouldBe defined // only-child workspaces are exempt
      }
    }

  it should "remove bucket permissions and record requester pays" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        migrationId <- inTransaction { dataAccess =>
          for {
            migrationId <- createAndScheduleWorkspace(testData.v1Workspace)
            _ <- dataAccess.workspaceMigrationQuery.update(
              migrationId,
              dataAccess.workspaceMigrationQuery.startedCol,
              now.some
            )
          } yield migrationId
        }
        _ <- migrate
        attempt <- inTransactionT(_.workspaceMigrationQuery.getAttempt(migrationId))
      } yield {
        attempt.requesterPaysEnabled shouldBe true
        attempt.workspaceBucketIamRemoved shouldBe defined
      }
    }

  it should "create a new google project when there are more than one v1 workspaces in the billing project" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataSource =>
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            _ <- dataSource.workspaceQuery.createOrUpdate(testData.v1Workspace2)
            _ <- writeBucketIamRevoked(testData.v1Workspace.workspaceIdAsUUID)
          } yield ()
        }

        _ <- migrate

        migration <- inTransactionT {
          _.workspaceMigrationQuery
            .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.newGoogleProjectId shouldBe defined
        migration.newGoogleProjectId should not be Some(testData.billingProject.googleProjectId)
        migration.newGoogleProjectNumber shouldBe defined
        migration.newGoogleProjectConfigured shouldBe defined
      }
    }

  it should
    """re-use the billing project's google project when there's one workspace in the billing project
      | move the google project into the CommunityWorkbench folder when it's not in a service perimeter and
      | short-circuit transferring the bucket""".stripMargin in runMigrationTest {

      def runTest(billingProject: RawlsBillingProject, workspace: Workspace) =
        for {
          _ <- inTransaction { _ =>
            createAndScheduleWorkspace(workspace) >> writeBucketIamRevoked(workspace.workspaceIdAsUUID)
          }

          // run the pipeline twice:
          // - once to exercise the only-child optimisation
          // - a second time to ensure all timestamps are set correctly
          _ <- migrate *> migrate

          migration <- inTransactionT {
            _.workspaceMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
          }
        } yield {
          migration.newGoogleProjectId shouldBe Some(billingProject.googleProjectId)

          // transferring the bucket should be short-circuited
          migration.tmpBucketCreated shouldBe defined
          migration.workspaceBucketTransferIamConfigured shouldBe defined
          migration.workspaceBucketTransferJobIssued shouldBe defined
          migration.workspaceBucketTransferred shouldBe defined
          migration.workspaceBucketDeleted shouldBe defined
          migration.finalBucketCreated shouldBe defined
          migration.tmpBucketTransferIamConfigured shouldBe defined
          migration.tmpBucketTransferJobIssued shouldBe defined
          migration.tmpBucketTransferred shouldBe defined
          migration.tmpBucketDeleted shouldBe defined
        }

      val projectMoves = new CopyOnWriteArraySet[(GoogleProjectId, String)]()
      val mockGcsDao = new MockGoogleServicesDAO("test") {
        override def addProjectToFolder(googleProject: GoogleProjectId, folderId: String): Future[Unit] = {
          projectMoves.add(googleProject -> folderId)
          super.addProjectToFolder(googleProject, folderId)
        }

        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           span: Span
        ): Future[ProjectBillingInfo] =
          fail("it should not update the billing account when re-using the billing project's google project")
      }

      MigrateAction.local(_.copy(gcsDao = mockGcsDao))(
        for {
          // only-child workspaces that are not in service perimeters should be moved to the
          // "CommunityWorkbench" folder
          _ <- runTest(testData.billingProject, testData.v1Workspace)
          _ = projectMoves.asScala shouldBe Set(
            testData.billingProject.googleProjectId -> testData.communityWorkbenchFolderId.value
          )

          _ = projectMoves.clear()

          billingProject2 = testData.billingProject.copy(
            projectName = RawlsBillingProjectName("super-secure-project"),
            servicePerimeter = Some(ServicePerimeterName("hush-hush"))
          )

          workspace2 = testData.v1Workspace2.copy(namespace = billingProject2.projectName.value)

          _ <- inTransaction { dataAccess =>
            dataAccess.rawlsBillingProjectQuery.create(billingProject2) *>
              dataAccess.workspaceQuery.createOrUpdate(workspace2)
          }

          // only-child workspaces that are in service perimeters should not be moved to the
          // "CommunityWorkbench" folder
          _ <- runTest(billingProject2, workspace2)
          _ = projectMoves.asScala shouldBe Set.empty
        } yield Succeeded
      )
    }

  it should "remove billing project resource groups from the google project iam policy that were created by deployment manager" in
    runMigrationTest {
      import SamBillingProjectPolicyNames._
      val bindingsRemoved = new ConcurrentHashMap[String, Set[String]]()
      val mockIamDao = new MockGoogleIamDAO {
        override def getProjectPolicy(iamProject: GoogleProject) = Future.successful(
          new com.google.api.services.cloudresourcemanager.model.Policy().setBindings(
            ImmutableList.of(
              new Binding()
                .setRole("roleA")
                .setMembers(ImmutableList.of("user:foo@gmail.com", s"group:$owner@example.com")),
              new Binding()
                .setRole("roleB")
                .setMembers(ImmutableList.of(s"group:$owner@example.com", s"group:$canComputeUser@example.com")),
              new Binding()
                .setRole("roleC")
                .setMembers(ImmutableList.of(s"group:$owner@example.com", s"group:$workspaceCreator@example.com"))
            )
          )
        )

        override def removeIamRoles(googleProject: GoogleProject,
                                    userEmail: WorkbenchEmail,
                                    memberType: GoogleIamDAO.MemberType,
                                    rolesToRemove: Set[String],
                                    retryIfGroupDoesNotExist: Boolean
        ) = {
          bindingsRemoved.put(userEmail.value, rolesToRemove)
          Future.successful(true)
        }
      }

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(testData.v1Workspace) >>
            writeBucketIamRevoked(testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- MigrateAction.local(_.copy(googleIamDAO = mockIamDao))(migrate)

      } yield {
        bindingsRemoved.size() shouldBe 2
        bindingsRemoved.get(owner + "@example.com") shouldBe Set("roleA", "roleB", "roleC")
        bindingsRemoved.get(canComputeUser + "@example.com") shouldBe Set("roleB")
      }
    }

  it should "fetch the google project number when it's not in the workspace record" in
    runMigrationTest {
      val googleProjectNumber = GoogleProjectNumber(('1' to '9').mkString)
      val mockGcsDao = new MockGoogleServicesDAO("test") {
        override def getGoogleProject(billingProjectName: GoogleProjectId): Future[Project] =
          Future.successful(
            new Project()
              .setProjectId(testData.v1Workspace.namespace)
              .setProjectNumber(googleProjectNumber.value.toLong)
          )
      }

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(testData.v1Workspace.copy(googleProjectNumber = None)) >>
            writeBucketIamRevoked(testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- MigrateAction.local(_.copy(gcsDao = mockGcsDao))(migrate)

        migration <- inTransactionT {
          _.workspaceMigrationQuery
            .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.newGoogleProjectId shouldBe Some(testData.billingProject.googleProjectId)
        migration.newGoogleProjectNumber shouldBe Some(googleProjectNumber)
      }
    }

  it should "not apply the only-child optimization to the last unmigrated workspace in a billing project if that billing project has multiple workspaces" in
    runMigrationTest {
      val workspaces = List(testData.v1Workspace, testData.v1Workspace2)
      inTransaction(access => workspaces.traverse_(access.workspaceQuery.createOrUpdate)) *> workspaces.foldMapK {
        workspace =>
          for {
            _ <- inTransaction(_.workspaceMigrationQuery.schedule(workspace.toWorkspaceName))
            getMigration = inTransactionT {
              _.workspaceMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
            }
            _ <- allowOne(runStep(refreshTransferJobs >>= updateMigrationTransferJobStatus) *> migrate).whileM_ {
              getMigration.map(_.finished.isEmpty)
            }
            m <- getMigration
          } yield {
            m.newGoogleProjectId shouldBe defined
            m.newGoogleProjectId should not be Some(testData.billingProject.googleProjectId)
          }
      }
    }

  it should "fail the migration when there's an error on the workspace billing account" in
    runMigrationTest {
      val workspace = testData.v1Workspace.copy(
        errorMessage = "oh noes :(".some,
        name = UUID.randomUUID.toString,
        workspaceId = UUID.randomUUID.toString
      )

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >>
            writeBucketIamRevoked(workspace.workspaceIdAsUUID)
        }

        _ <- migrate

        migration <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("an error exists on workspace")
      }
    }

  it should "fail the migration when there's no billing account on the workspace" in
    runMigrationTest {
      val workspace = testData.v1Workspace.copy(
        currentBillingAccountOnGoogleProject = None,
        name = UUID.randomUUID.toString,
        workspaceId = UUID.randomUUID.toString
      )

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >>
            writeBucketIamRevoked(workspace.workspaceIdAsUUID)
        }

        _ <- migrate

        migration <- inTransactionT { dataAccess =>
          dataAccess.workspaceMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("no billing account on workspace")
      }
    }

  it should "fail the migration when the billing account on the billing project is invalid" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataAccess =>
          import dataAccess.{executionContext => _, _}
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            _ <- writeBucketIamRevoked(testData.v1Workspace.workspaceIdAsUUID)
            _ <- rawlsBillingProjectQuery
              .withProjectName(RawlsBillingProjectName(testData.v1Workspace.namespace))
              .setInvalidBillingAccount(true)
          } yield ()
        }

        _ <- migrate

        migration <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("invalid billing account on billing project")
      }
    }

  it should "fail the migration when the billing account on the workspace does not match the billing account on the billing project" in
    runMigrationTest {
      val workspace = testData.v1Workspace.copy(
        currentBillingAccountOnGoogleProject =
          testData.v1Workspace.currentBillingAccountOnGoogleProject.map { billingAccount =>
            RawlsBillingAccountName(billingAccount.value ++ UUID.randomUUID.toString)
          },
        name = UUID.randomUUID.toString,
        workspaceId = UUID.randomUUID.toString
      )

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >>
            writeBucketIamRevoked(workspace.workspaceIdAsUUID)
        }

        _ <- migrate
        migration <- inTransactionT { dataAccess =>
          dataAccess.workspaceMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include(
          "billing account on workspace differs from billing account on billing project"
        )
      }
    }

  it should "restart jobs when Gcs is unavailable" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.newGoogleProjectConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectIdCol,
              GoogleProjectId("new-google-project").some
            )
          } yield ()
        }

        error = new StatusRuntimeException(
          Status.UNAVAILABLE.withDescription(
            "io.grpc.StatusRuntimeException: UNAVAILABLE: Failed to obtain the location " +
              s"of the GCS bucket ${testData.v1Workspace.bucketName} " +
              "Additional details: GCS is temporarily unavailable."
          )
        )

        mockStorageService = new MockStorageService {
          override def insertBucket(googleProject: GoogleProject,
                                    bucketName: GcsBucketName,
                                    acl: Option[NonEmptyList[Acl]],
                                    labels: Map[String, String],
                                    traceId: Option[TraceId],
                                    bucketPolicyOnlyEnabled: Boolean,
                                    logBucket: Option[GcsBucketName],
                                    retryConfig: RetryConfig,
                                    location: Option[String],
                                    bucketTargetOptions: List[Storage.BucketTargetOption]
          ): fs2.Stream[IO, Unit] =
            fs2.Stream.raiseError[IO](error)
        }

        _ <- MigrateAction.local(_.copy(storageService = mockStorageService))(migrate)
        _ <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }.map { migration =>
          migration.finished shouldBe defined
          migration.outcome shouldBe Some(Failure(error.getMessage))
        }

        _ <- allowOne(restartFailuresLike(FailureModes.gcsUnavailableFailure))
        _ <- migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.workspaceMigrationQuery
              .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
              .value
            retries <- dataAccess.migrationRetryQuery.getOrCreate(migration.id)
          } yield {
            migration.finished shouldBe empty
            migration.outcome shouldBe empty
            migration.tmpBucketCreated shouldBe defined
            retries.numRetries shouldBe 1
          }
          test
        }
      } yield succeed
    }

  it should "issue configure the workspace and tmp bucket iam policies for storage transfer" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketCreatedCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        _ <- migrate
        migration <- inTransactionT(_.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID))
      } yield migration.workspaceBucketTransferIamConfigured shouldBe defined
    }

  it should "issue a storage transfer job from the workspace bucket to the tmp bucket" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.workspaceBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }
        _ <- migrate
        (migration, transferJob) <- inTransactionT { dataAccess =>
          for {
            migration <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
            transferJob <- OptionT[ReadWriteAction, PpwStorageTransferJob] {
              storageTransferJobs
                .filter(_.migrationId === migration.id)
                .take(1)
                .result
                .headOption
            }
          } yield (migration, transferJob)
        }
      } yield {
        transferJob.originBucket.value shouldBe testData.v1Workspace.bucketName
        transferJob.destBucket.value shouldBe "tmp-bucket-name"
        migration.workspaceBucketTransferJobIssued shouldBe defined
      }
    }

  it should "delete the workspace bucket" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        migrationId <- inTransaction { dataAccess =>
          for {
            migrationId <- createAndScheduleWorkspace(testData.v1Workspace)
            _ <- dataAccess.workspaceMigrationQuery.update(
              migrationId,
              dataAccess.workspaceMigrationQuery.workspaceBucketTransferredCol,
              now.some
            )
          } yield migrationId
        }

        _ <- migrate
        migration <- inTransactionT { dataAccess =>
          dataAccess.workspaceMigrationQuery.getAttempt(migrationId)
        }
      } yield migration.workspaceBucketDeleted shouldBe defined
    }

  it should "issue configure the tmp and final workspace bucket iam policies for storage transfer" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.finalBucketCreatedCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        _ <- migrate
        migration <- inTransactionT(_.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID))
      } yield migration.tmpBucketTransferIamConfigured shouldBe defined
    }

  it should "issue a storage transfer job from the tmp bucket to the final workspace bucket" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        _ <- migrate

        (migration, transferJob) <- inTransactionT { dataAccess =>
          for {
            migration <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
            transferJob <- OptionT[ReadWriteAction, PpwStorageTransferJob] {
              storageTransferJobs
                .filter(_.migrationId === migration.id)
                .take(1)
                .result
                .headOption
            }
          } yield (migration, transferJob)
        }
      } yield {
        transferJob.originBucket.value shouldBe "tmp-bucket-name"
        transferJob.destBucket.value shouldBe testData.v1Workspace.bucketName
        migration.tmpBucketTransferJobIssued shouldBe defined
      }
    }

  it should "re-issue a storage transfer job when it receives permissions precondition failures" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        mockSts = new MockStorageTransferService {
          override def createTransferJob(jobName: JobName,
                                         jobDescription: String,
                                         projectToBill: GoogleProject,
                                         originBucket: GcsBucketName,
                                         destinationBucket: GcsBucketName,
                                         schedule: JobTransferSchedule,
                                         options: Option[GoogleStorageTransferService.JobTransferOptions]
          ) =
            getStsServiceAccount(projectToBill).flatMap { serviceAccount =>
              IO.raiseError(
                new StatusRuntimeException(
                  Status.FAILED_PRECONDITION.withDescription(
                    s"Service account ${serviceAccount.email} does not have required " +
                      "permissions {storage.objects.create, storage.objects.list} " +
                      s"for bucket $destinationBucket."
                  )
                )
              )
            }
        }

        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(migrate)
        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.workspaceMigrationQuery
              .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
              .value
            transferJobs <- storageTransferJobs.filter(_.migrationId === migration.id).result
          } yield {
            transferJobs shouldBe empty
            migration.tmpBucketTransferJobIssued shouldBe empty
            migration.outcome shouldBe defined
          }
          test
        }

        _ <- allowOne(restartFailuresLike(FailureModes.noBucketPermissionsFailure))
        _ <- migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.workspaceMigrationQuery
              .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
              .value
            Some(transferJob) <- storageTransferJobs.filter(_.migrationId === migration.id).result.headOption
          } yield {
            transferJob.originBucket.value shouldBe "tmp-bucket-name"
            transferJob.destBucket.value shouldBe testData.v1Workspace.bucketName
            migration.tmpBucketTransferJobIssued shouldBe defined
          }
          test
        }
      } yield succeed
    }

  it should "restart rate-limited transfer jobs after the configured amount of time has elapsed" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        error = new StatusRuntimeException(
          Status.RESOURCE_EXHAUSTED.withDescription(
            "Quota exceeded for quota metric 'Create requests' and limit " +
              "'Create requests per day' of service 'storagetransfer.googleapis.com' " +
              "for consumer 'project_number:000000000000'."
          )
        )

        mockSts = new MockStorageTransferService {
          override def createTransferJob(jobName: JobName,
                                         jobDescription: String,
                                         projectToBill: GoogleProject,
                                         originBucket: GcsBucketName,
                                         destinationBucket: GcsBucketName,
                                         schedule: JobTransferSchedule,
                                         options: Option[GoogleStorageTransferService.JobTransferOptions]
          ) =
            IO.raiseError(error)
        }

        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(migrate)
        _ <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }.map { migration =>
          migration.finished shouldBe defined
          migration.outcome shouldBe Some(Failure(error.getMessage))
        }

        _ <- allowOne(restartFailuresLike(FailureModes.stsRateLimitedFailure))
        _ <- migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.workspaceMigrationQuery
              .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
              .value
            retries <- dataAccess.migrationRetryQuery.getOrCreate(migration.id)
            Some(transferJob) <- storageTransferJobs.filter(_.migrationId === migration.id).result.headOption
          } yield {
            migration.finished shouldBe empty
            migration.outcome shouldBe empty
            migration.tmpBucketTransferJobIssued shouldBe defined
            retries.numRetries shouldBe 1
            transferJob.originBucket.value shouldBe "tmp-bucket-name"
            transferJob.destBucket.value shouldBe testData.v1Workspace.bucketName
          }
          test
        }
      } yield succeed
    }

  it should "not retry a failed migration when the maximum number of retries has been exceeded" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update2(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        mockSts = new MockStorageTransferService {
          override def createTransferJob(jobName: JobName,
                                         jobDescription: String,
                                         projectToBill: GoogleProject,
                                         originBucket: GcsBucketName,
                                         destinationBucket: GcsBucketName,
                                         schedule: JobTransferSchedule,
                                         options: Option[GoogleStorageTransferService.JobTransferOptions]
          ) =
            getStsServiceAccount(projectToBill).flatMap { serviceAccount =>
              IO.raiseError(
                new StatusRuntimeException(
                  Status.FAILED_PRECONDITION.withDescription(
                    s"Service account ${serviceAccount.email} does not have required " +
                      "permissions {storage.objects.create, storage.objects.list} " +
                      s"for bucket $destinationBucket."
                  )
                )
              )
            }
        }

        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(migrate)

        before <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val update = for {
            Some(migration) <- dataAccess.workspaceMigrationQuery
              .getAttempt(testData.v1Workspace.workspaceIdAsUUID)
              .value
            retry <- dataAccess.migrationRetryQuery.getOrCreate(migration.id)
            _ <- dataAccess.migrationRetryQuery.update(retry.id, dataAccess.migrationRetryQuery.retriesCol, 1L)
          } yield migration
          update
        }

        _ <- migrate
        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(after) <- dataAccess.workspaceMigrationQuery.getAttempt(before.id).value
            transferJobs <- storageTransferJobs.filter(_.migrationId === before.id).result
          } yield {
            transferJobs shouldBe empty
            after shouldBe before
          }
          test
        }
      } yield succeed
    }

  it should "delete the temporary bucket and record when it was deleted" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update3(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketTransferredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectIdCol,
              GoogleProjectId("google-project-id").some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        _ <- migrate

        migration <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield migration.tmpBucketDeleted shouldBe defined
    }

  it should "update the Workspace record after the temporary bucket has been deleted" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        googleProjectId = GoogleProjectId("whatever dude")
        googleProjectNumber = GoogleProjectNumber("abc123")
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update3(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketDeletedCol,
              now.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectIdCol,
              googleProjectId.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectNumberCol,
              googleProjectNumber.some
            )
          } yield ()
        }

        _ <- migrate

        workspace <- getWorkspace(testData.v1Workspace.workspaceIdAsUUID)
        migration <- inTransactionT {
          _.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome shouldBe Success.some
        workspace.googleProjectId shouldBe googleProjectId
        workspace.googleProjectNumber shouldBe googleProjectNumber.some
        workspace.workspaceVersion shouldBe WorkspaceVersions.V2
      }
    }

  it should "sync the workspace can-compute policy" in
    runMigrationTest {
      case class FullyQualifiedSamPolicy(resourceTypeName: SamResourceTypeName,
                                         resourceId: String,
                                         policyName: SamResourcePolicyName
      )
      for {
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            _ <- dataAccess.workspaceMigrationQuery.update3(
              attempt.get.id,
              dataAccess.workspaceMigrationQuery.tmpBucketDeletedCol,
              Timestamp.from(Instant.now).some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectIdCol,
              GoogleProjectId("google-project-id").some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectNumberCol,
              GoogleProjectNumber("abc123").some
            )
          } yield ()
        }

        syncedPolicies = new ConcurrentHashMap[FullyQualifiedSamPolicy, Unit]()
        mockSamDao <- MigrateAction
          .asks(_.dataSource)
          .map(new MockSamDAO(_) {
            override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName,
                                            resourceId: String,
                                            policyName: SamResourcePolicyName
            ): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] = {
              syncedPolicies.put(FullyQualifiedSamPolicy(resourceTypeName, resourceId, policyName), ())
              super.syncPolicyToGoogle(resourceTypeName, resourceId, policyName)
            }
          })

        _ <- MigrateAction.local(_.copy(samDao = mockSamDao))(migrate)

      } yield {
        syncedPolicies.size() shouldBe 1
        syncedPolicies.keySet().asScala should contain(
          FullyQualifiedSamPolicy(
            SamResourceTypeNames.workspace,
            testData.v1Workspace.workspaceId,
            SamWorkspacePolicyNames.canCompute
          )
        )
      }
    }

  // [PPWM-105] fk on retries table prevent migrated workspace from being deleted
  it should "not prevent a workspace from being deleted if the migration was retried" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataAccess =>
          createAndScheduleWorkspace(testData.v1Workspace) >>=
            dataAccess.migrationRetryQuery.getOrCreate
        }
        wsService <- MigrateAction.asks(_.workspaceService)
        _ <- MigrateAction.fromFuture {
          wsService.deleteWorkspace(testData.v1Workspace.toWorkspaceName)
        }
      } yield Succeeded
    }

  "issueBucketTransferJob" should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        // just need a unique migration id
        migration <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        workspaceBucketName = GcsBucketName("workspace-bucket-name")
        tmpBucketName = GcsBucketName("tmp-bucket-name")
        job <- startBucketTransferJob(migration, testData.v1Workspace, workspaceBucketName, tmpBucketName)
        transferJob <- inTransactionT { _ =>
          OptionT[ReadWriteAction, PpwStorageTransferJob] {
            storageTransferJobs
              .filter(_.jobName === job.getName)
              .take(1)
              .result
              .headOption
          }
        }
      } yield {
        transferJob.jobName.value shouldBe job.getName
        transferJob.migrationId shouldBe migration.id
        transferJob.originBucket shouldBe workspaceBucketName
        transferJob.destBucket shouldBe tmpBucketName
      }
    }

  "peekTransferJob" should "return the first active job that was updated last and touch it" in
    runMigrationTest {
      for {
        migration <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- startBucketTransferJob(migration, testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
        job <- peekTransferJob
      } yield job.updated should be > job.created
    }

  it should "ignore finished jobs" in
    runMigrationTest {
      for {
        migration <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        job <- startBucketTransferJob(migration, testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
        finished <- nowTimestamp
        _ <- inTransaction { _ =>
          storageTransferJobs
            .filter(_.jobName === job.getName)
            .map(_.finished)
            .update(finished.some)
        }

        job <- peekTransferJob.mapF(optionT => OptionT(optionT.value.map(_.some)))
      } yield job should not be defined
    }

  "refreshTransferJobs" should "update the state of storage transfer jobs" in
    runMigrationTest {
      for {
        migration <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- startBucketTransferJob(migration, testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
        transferJob <- refreshTransferJobs
      } yield {
        transferJob.migrationId shouldBe migration.id
        transferJob.finished shouldBe defined
        transferJob.outcome.value shouldBe Outcome.Success
      }
    }

  it should "update the state of jobs in order of last updated" in
    runMigrationTest {
      val storageTransferService = new MockStorageTransferService {
        // want to return no operations to make sure that the job does not complete and is
        // updated continually
        override def getTransferJob(jobName: JobName, project: GoogleProject) =
          IO.pure {
            TransferJob.newBuilder
              .setName(jobName.value)
              .setProjectId(project.value)
              .setStatus(TransferJob.Status.ENABLED)
              .build
          }
      }

      MigrateAction.local(_.copy(storageTransferService = storageTransferService)) {
        for {
          migration1 <- inTransactionT { dataAccess =>
            OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
              dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
          }

          migration2 <- inTransactionT { dataAccess =>
            OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace2)) *>
              dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace2.workspaceIdAsUUID)
          }

          _ <- startBucketTransferJob(migration1, testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
          _ <- startBucketTransferJob(migration2, testData.v1Workspace2, GcsBucketName("foo"), GcsBucketName("bar"))

          getTransferJobs = inTransaction { _ =>
            storageTransferJobs
              .sortBy(_.id.asc)
              .result
              .map(_.toList)
          }

          transferJobsBefore <- getTransferJobs

          _ <- runStep(refreshTransferJobs.void)
          transferJobsMid <- getTransferJobs

          _ <- runStep(refreshTransferJobs.void)
          transferJobsAfter <- getTransferJobs
        } yield {
          forAll(transferJobsBefore)(job => job.finished should not be defined)

          // the first job created should be updated first
          transferJobsMid(0).updated should be > transferJobsBefore(0).updated
          transferJobsMid(1).updated shouldBe transferJobsBefore(1).updated

          // the second job should be updated next as it was updated the longest time ago
          transferJobsAfter(0).updated shouldBe transferJobsMid(0).updated
          transferJobsAfter(1).updated should be > transferJobsMid(1).updated
        }
      }
    }

  it should "restart rate-limited transfer jobs after the configured amount of time has elapsed" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        migrationId <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.v1Workspace)
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID).value
            migrationId = attempt.value.id
            _ <- dataAccess.workspaceMigrationQuery.update5(
              migrationId,
              dataAccess.workspaceMigrationQuery.workspaceBucketIamRemovedCol,
              now.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectConfiguredCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCreatedCol,
              now.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some,
              dataAccess.workspaceMigrationQuery.workspaceBucketTransferIamConfiguredCol,
              now.some
            )
          } yield migrationId
        }

        errorDetails =
          "project-1234@storage-transfer-service.iam.gserviceaccount.com " +
            "does not have storage.objects.get access to the Google Cloud Storage object."

        mockSts = new MockStorageTransferService {
          override def getTransferOperation(operationName: GoogleStorageTransferService.OperationName) =
            super.getTransferOperation(operationName).map { operation =>
              val errorLogEntry = ErrorLogEntry.newBuilder
                .setUrl(s"gs://${testData.v1Workspace.bucketName}/foo.cram")
                .addErrorDetails(errorDetails)
                .build
              val errorSummary = ErrorSummary.newBuilder
                .setErrorCode(Code.PERMISSION_DENIED)
                .setErrorCount(1)
                .addErrorLogEntries(errorLogEntry)
                .build
              operation.toBuilder
                .setStatus(TransferOperation.Status.FAILED)
                .addErrorBreakdowns(errorSummary)
                .build
            }
        }

        _ <- migrate // to issue the sts job
        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(
          refreshTransferJobs >>= updateMigrationTransferJobStatus
        )

        _ <- inTransactionT(_.workspaceMigrationQuery.getAttempt(migrationId)).map { migration =>
          migration.finished shouldBe defined
          migration.outcome.value.failureMessage should include(errorDetails)
        }

        _ <- allowOne(reissueFailedStsJobs)
        _ <- migrate *> migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.workspaceMigrationQuery
              .getAttempt(migrationId)
              .value
            retries <- dataAccess.migrationRetryQuery.getOrCreate(migration.id)
            Seq(_, newJob) <- storageTransferJobs.filter(_.migrationId === migration.id).result
          } yield {
            migration.finished shouldBe empty
            migration.outcome shouldBe empty
            migration.workspaceBucketTransferIamConfigured shouldBe defined
            migration.workspaceBucketTransferJobIssued shouldBe defined
            retries.numRetries shouldBe 1
            newJob.outcome shouldBe empty
          }
          test
        }
      } yield succeed
    }

  def storageTransferJobForTesting = new PpwStorageTransferJob(
    id = -1,
    jobName = null,
    migrationId = -1,
    created = null,
    updated = null,
    destBucket = null,
    originBucket = null,
    finished = null,
    outcome = null
  )

  "updateMigrationTransferJobStatus" should "update WORKSPACE_BUCKET_TRANSFERRED on job success" in
    runMigrationTest {
      for {
        before <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- updateMigrationTransferJobStatus(
          storageTransferJobForTesting.copy(
            migrationId = before.id,
            destBucket = GcsBucketName("tmp-bucket-name"),
            originBucket = GcsBucketName("workspace-bucket"),
            outcome = Success.some
          )
        )

        after <- inTransactionT { dataAccess =>
          dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        after.workspaceBucketTransferred shouldBe defined
        after.tmpBucketTransferred should not be defined
      }
    }

  it should "update TMP_BUCKET_TRANSFERRED on job success" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        before <- inTransactionT { dataAccess =>
          for {
            _ <- OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace))
            attempt <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
            _ <- OptionT.liftF {
              dataAccess.workspaceMigrationQuery.update(
                attempt.id,
                dataAccess.workspaceMigrationQuery.workspaceBucketTransferredCol,
                now.some
              )
            }
            updated <- dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
          } yield updated
        }

        _ <- updateMigrationTransferJobStatus(
          storageTransferJobForTesting.copy(
            migrationId = before.id,
            originBucket = GcsBucketName("workspace-bucket"),
            destBucket = GcsBucketName("tmp-bucket-name"),
            outcome = Success.some
          )
        )

        after <- inTransactionT(_.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID))
      } yield {
        after.workspaceBucketTransferred shouldBe defined
        after.tmpBucketTransferred shouldBe defined
      }
    }

  it should "fail the migration on job failure" in
    runMigrationTest {
      for {
        before <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.v1Workspace)) *>
            dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }

        failure = Failure("oh noes :(")
        _ <- updateMigrationTransferJobStatus(
          storageTransferJobForTesting.copy(migrationId = before.id, outcome = failure.some)
        )

        after <- inTransactionT { dataAccess =>
          dataAccess.workspaceMigrationQuery.getAttempt(testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        after.finished shouldBe defined
        after.outcome shouldBe failure.some
      }
    }

  "Outcome" should "have json support for Success" in {
    val jsSuccess = outcomeJsonFormat.write(Success)
    jsSuccess shouldBe JsString("success")
    outcomeJsonFormat.read(jsSuccess) shouldBe Success
  }

  it should "have json support for Failure" in {
    val message = UUID.randomUUID.toString
    val jsFailure = outcomeJsonFormat.write(Failure(message))
    jsFailure shouldBe JsObject("failure" -> JsString(message))
    outcomeJsonFormat.read(jsFailure) shouldBe Failure(message)
  }

}
