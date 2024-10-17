package org.broadinstitute.dsde.rawls.monitor

import akka.http.scaladsl.model.StatusCodes
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxApplyOps, catsSyntaxOptionId, toFoldableOps}
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import io.opencensus.trace.{Span => OpenCensusSpan}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.Inspectors.forAll
import org.scalatest.OptionValues
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{postfixOps, reflectiveCalls}

class BillingAccountChangeSynchronizerSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with MockitoSugar
    with OptionValues {

  val defaultExecutionContext: ExecutionContext = executionContext

  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("test-ba")

  val mockGcsDAO = spy(new MockGoogleServicesDAO("test"))
  val mockSamDAO = new MockSamDAO(_: SlickDataSource) {
    override def listResourceChildren(resourceTypeName: SamResourceTypeName,
                                      resourceId: String,
                                      ctx: RawlsRequestContext
    ): Future[Seq[SamFullyQualifiedResourceId]] =
      Future.successful(Seq(SamFullyQualifiedResourceId(resourceId, SamResourceTypeNames.googleProject.value)))
  }

  import driver.api._

  "BillingAccountChangeSynchronizer" should "update the billing account on workspaces in a billing project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(billingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )

      val v2Workspace = Workspace(
        billingProject.projectName.value,
        "v2",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("differentId"),
        Option(GoogleProjectNumber("43")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val workspaceWithoutBillingAccount = v2Workspace.copy(
        name = "noBillingAccount",
        workspaceId = UUID.randomUUID().toString,
        googleProjectId = GoogleProjectId("anotherId"),
        googleProjectNumber = Option(GoogleProjectNumber("44")),
        currentBillingAccountOnGoogleProject = None
      )

      val allWorkspaceGoogleProjects =
        List(v2Workspace.googleProjectId, workspaceWithoutBillingAccount.googleProjectId)

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- workspaceQuery.createOrUpdate(workspaceWithoutBillingAccount)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(newBillingAccount),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val spiedGcsDao = spy(new MockGoogleServicesDAO("test"))
      BillingAccountChangeSynchronizer(dataSource,
                                       spiedGcsDao,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      every(
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.currentBillingAccountOnGoogleProject)
      ) shouldBe Some(newBillingAccount)

      allWorkspaceGoogleProjects.map { googleProject =>
        verify(spiedGcsDao, times(1)).setBillingAccount(ArgumentMatchers.eq(googleProject),
                                                        ArgumentMatchers.eq(newBillingAccount.some),
                                                        any()
        )
      }
    }

  it should "not endlessly retry when it fails to get billing info for google projects" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               originalBillingAccount,
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val workspace = Workspace(
        billingProject.projectName.value,
        "whatever",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("any old project"),
        Option(GoogleProjectNumber("44")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(RawlsBillingAccountName("new-ba")),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val timesCalled = new ConcurrentHashMap[GoogleProjectId, Int]()
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit
          executionContext: ExecutionContext
        ): Future[ProjectBillingInfo] = {
          timesCalled.merge(googleProjectId, 1, _ + _)
          Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(exceptionMessage)))
        }
      }

      BillingAccountChangeSynchronizer(dataSource,
                                       gcsDAO = failingGcsDao,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      runAndWait(workspaceQuery.findByIdOrFail(workspace.workspaceId)).errorMessage.value should include(
        exceptionMessage
      )

      timesCalled.size() shouldBe 2
      timesCalled.get(workspace.googleProjectId) shouldBe 1
      timesCalled.get(billingProject.googleProjectId) shouldBe 1
    }

  it should "not endlessly retry when it fails to set billing info for the google project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               originalBillingAccount,
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val workspace = Workspace(
        billingProject.projectName.value,
        "whatever",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("any old project"),
        Option(GoogleProjectNumber("44")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(RawlsBillingAccountName("new-ba")),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val timesCalled = new ConcurrentHashMap[GoogleProjectId, Int]()
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           rawlsTracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] = {
          timesCalled.merge(googleProjectId, 1, _ + _)
          Future.failed(new RawlsException(exceptionMessage))
        }
      }

      BillingAccountChangeSynchronizer(dataSource,
                                       failingGcsDao,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
        .getOrElse(fail("workspace not found"))
        .errorMessage
        .value should include(exceptionMessage)

      timesCalled.size() shouldBe 2
      timesCalled.get(workspace.googleProjectId) shouldBe 1
      timesCalled.get(billingProject.googleProjectId) shouldBe 1
    }

  it should "not try to update the billing account if the new value is the same as the old value" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Some(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               originalBillingAccount,
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val workspace = Workspace(
        billingProject.projectName.value,
        "whatever",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("any old project"),
        Option(GoogleProjectNumber("44")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace)
          // database rejects updating billing project with same billing account
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             None,
                                                             testData.userOwner.userSubjectId
          )
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             originalBillingAccount,
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val mockGcsDAO = spy(new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit
          executionContext: ExecutionContext
        ): Future[ProjectBillingInfo] =
          Future.successful(
            new ProjectBillingInfo().setBillingAccountName(originalBillingAccount.value.value).setBillingEnabled(true)
          )
      })

      BillingAccountChangeSynchronizer(dataSource,
                                       gcsDAO = mockGcsDAO,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
        .getOrElse(fail("workspace not found"))
        .currentBillingAccountOnGoogleProject shouldBe originalBillingAccount

      verify(mockGcsDAO, times(0)).setBillingAccountName(ArgumentMatchers.eq(workspace.googleProjectId),
                                                         ArgumentMatchers.eq(originalBillingAccount.value),
                                                         any()
      )
    }

  it should "continue to update other workspace google projects even if one fails to update" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               originalBillingAccount,
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val workspace1 = Workspace(
        billingProject.projectName.value,
        "workspace1",
        UUID.randomUUID().toString,
        "bucketName1",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId(billingProject.projectName.value),
        billingProject.googleProjectNumber,
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val workspace2 = Workspace(
        billingProject.projectName.value,
        "workspace2",
        UUID.randomUUID().toString,
        "bucketName2",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("differentId"),
        Option(GoogleProjectNumber("43")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val badWorkspaceGoogleProjectId = GoogleProjectId("very bad")
      val badWorkspace = Workspace(
        billingProject.projectName.value,
        "bad",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        badWorkspaceGoogleProjectId,
        Option(GoogleProjectNumber("44")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspace1)
          _ <- workspaceQuery.createOrUpdate(workspace2)
          _ <- workspaceQuery.createOrUpdate(badWorkspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(newBillingAccount),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit
          executionContext: ExecutionContext
        ): Future[ProjectBillingInfo] =
          if (googleProjectId == badWorkspaceGoogleProjectId)
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, exceptionMessage)))
          else
            super.getBillingInfoForGoogleProject(googleProjectId)
      }

      BillingAccountChangeSynchronizer(dataSource,
                                       gcsDAO = failingGcsDao,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      def getBillingAccountOnGoogleProject(workspace: Workspace) =
        workspaceQuery
          .findByIdOrFail(workspace.workspaceId)
          .map(ws => (ws.currentBillingAccountOnGoogleProject, ws.errorMessage))

      runAndWait {
        for {
          (ws1BillingAccountOnGoogleProject, _) <- getBillingAccountOnGoogleProject(workspace1)
          (ws2BillingAccountOnGoogleProject, _) <- getBillingAccountOnGoogleProject(workspace2)
          (_, badWsBillingAccountErrorMessage) <- getBillingAccountOnGoogleProject(badWorkspace)
        } yield {
          ws1BillingAccountOnGoogleProject shouldBe Some(newBillingAccount)
          ws2BillingAccountOnGoogleProject shouldBe Some(newBillingAccount)
          badWsBillingAccountErrorMessage.value should include(exceptionMessage)
        }
      }
    }

  it should "sync only the latest billing account changes" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val finalBillingAccountName = runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- workspaceQuery.createOrUpdate(testData.workspace)

          mkBillingAccount = () => RawlsBillingAccountName(UUID.randomUUID.toString)
          setBillingAccount = (billingAccount: RawlsBillingAccountName) =>
            rawlsBillingProjectQuery.updateBillingAccount(
              testData.billingProject.projectName,
              Some(billingAccount),
              testData.userOwner.userSubjectId
            )

          _ <- setBillingAccount(mkBillingAccount())
          _ <- setBillingAccount(mkBillingAccount())
          _ <- setBillingAccount(mkBillingAccount())

          finalBillingAccount = mkBillingAccount()
          _ <- setBillingAccount(finalBillingAccount)
        } yield finalBillingAccount
      }

      val actor = BillingAccountChangeSynchronizer(dataSource, mockGcsDAO, mockSamDAO(dataSource))

      @nowarn("msg=not.*?exhaustive")
      val test = for {
        billingProjectUpdatesBefore <- actor.readABillingProjectChange
        _ <- actor.updateBillingAccounts
        billingProjectUpdatesAfter <- actor.readABillingProjectChange

        lastChange :: previousChanges <- dataSource.inTransaction { _ =>
          BillingAccountChanges
            .filter(_.billingProjectName === testData.billingProject.projectName.value)
            .sortBy(_.id.desc)
            .result
            .map(_.toList)
        }.io

      } yield {
        // before updating billing accounts, there should be one change only.
        billingProjectUpdatesBefore.size shouldBe 1
        // after updating, there should be no pending billing account changes.
        billingProjectUpdatesAfter shouldBe empty

        lastChange.googleSyncTime shouldBe defined
        lastChange.newBillingAccount shouldBe Some(finalBillingAccountName)

        // all previous changes should be ignored
        every(previousChanges.map(_.googleSyncTime)) shouldBe empty
      }

      test.unsafeRunSync()
    }

  it should "sync billing account changes even when there are no workspaces in the billing project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = RawlsBillingAccountName("my-fancy-billing-account").some,
            testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val gcsDao = new MockGoogleServicesDAO("test") {
        val timesCalled = new AtomicInteger(0)

        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           tracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] = {
          timesCalled.incrementAndGet()
          super.setBillingAccountName(googleProjectId, billingAccountName, tracingContext)
        }
      }

      BillingAccountChangeSynchronizer(dataSource, gcsDao, mockSamDAO(dataSource)).updateBillingAccounts
        .unsafeRunSync()

      gcsDao.timesCalled.get() shouldBe 1

      runAndWait {
        for {
          lastChange <- BillingAccountChanges.getLastChange(testData.billingProject.projectName)
        } yield lastChange.value.googleSyncTime shouldBe defined
      }
    }

  it should "mark the billing account as invalid in the billing project if terra does not have access" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- workspaceQuery.createOrUpdate(testData.workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = RawlsBillingAccountName(UUID.randomUUID.toString).some,
            testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val gcsDAO = new MockGoogleServicesDAO("test") {

        override def testTerraBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean] =
          Future.successful(false)

        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           rawlsTracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] =
          Future.failed(new RawlsException("You do not have access to this billing account or it does not exist."))
      }

      BillingAccountChangeSynchronizer(dataSource, gcsDAO, mockSamDAO(dataSource)).updateBillingAccounts
        .unsafeRunSync()

      runAndWait {
        for {
          lastChange <- BillingAccountChanges.getLastChange(testData.billingProject.projectName)
          billingProject <- rawlsBillingProjectQuery.load(testData.billingProject.projectName)
          workspace <- workspaceQuery.findByIdOrFail(testData.workspace.workspaceId)
        } yield {
          lastChange.value.googleSyncTime shouldBe defined
          lastChange.value.outcome.value.isFailure shouldBe true

          billingProject.value.invalidBillingAccount shouldBe true
          billingProject.value.message shouldBe defined

          workspace.errorMessage shouldBe defined
          workspace.currentBillingAccountOnGoogleProject shouldBe testData.workspace.currentBillingAccountOnGoogleProject
        }
      }
    }

  it should "mark the change as failed if updating the billing project fails" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      runAndWait {
        rawlsBillingProjectQuery.create(testData.billingProject) *>
          rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = RawlsBillingAccountName(UUID.randomUUID.toString).some,
            testData.userOwner.userSubjectId
          )
      }

      val gcsDAO = new MockGoogleServicesDAO("test") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           rawlsTracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] =
          Future.failed(new RawlsException(googleProjectId.value))
      }

      BillingAccountChangeSynchronizer(dataSource, gcsDAO, mockSamDAO(dataSource)).updateBillingAccounts
        .unsafeRunSync()

      runAndWait {
        for {
          lastChange <- BillingAccountChanges.getLastChange(testData.billingProject.projectName)
          billingProject <- rawlsBillingProjectQuery.load(testData.billingProject.projectName)
        } yield {
          lastChange.value.googleSyncTime shouldBe defined
          lastChange.value.outcome.value match {
            case Success => fail("should not succeed when updating billing project failed")
            case Failure(msg) =>
              msg should include(testData.billingProject.googleProjectId.value)
          }

          billingProject.value.invalidBillingAccount shouldBe false
          billingProject.value.message shouldBe defined
          billingProject.value.message.value should include(billingProject.value.googleProjectId.value)
        }
      }
    }

  it should "mark the change as failed if updating a workspace fails" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- List(testData.workspace,
                    testData.workspace.copy(
                      name = testData.workspace.name + "copy",
                      workspaceId = UUID.randomUUID().toString
                    )
          ).traverse_(workspaceQuery.createOrUpdate)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = RawlsBillingAccountName(UUID.randomUUID.toString).some,
            testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val gcsDAO = new MockGoogleServicesDAO("test") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           rawlsTracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] =
          Future.failed(new RawlsException(googleProjectId.value))
      }

      BillingAccountChangeSynchronizer(
        dataSource,
        gcsDAO,
        new MockSamDAO(dataSource) {
          override def listResourceChildren(resourceTypeName: SamResourceTypeName,
                                            resourceId: String,
                                            ctx: RawlsRequestContext
          ): Future[Seq[SamFullyQualifiedResourceId]] =
            Future.successful(Seq.empty)
        }
      ).updateBillingAccounts
        .unsafeRunSync()

      runAndWait {
        for {
          lastChange <- BillingAccountChanges.getLastChange(testData.billingProject.projectName)
          billingProject <- rawlsBillingProjectQuery.load(testData.billingProject.projectName)
          workspaces <- workspaceQuery.withBillingProject(testData.billingProject.projectName).read
        } yield {
          lastChange.value.googleSyncTime shouldBe defined
          lastChange.value.outcome.value match {
            case Success => fail("should not succeed when updating workspaces fail")
            case Failure(msg) =>
              forAll(workspaces) { workspace =>
                msg should include(workspace.googleProjectId.value)
              }
          }

          billingProject.value.invalidBillingAccount shouldBe false
          billingProject.value.message shouldBe empty

          forAll(workspaces) { ws =>
            ws.errorMessage shouldBe defined
            ws.errorMessage.value should include(ws.googleProjectId.value)
            ws.state shouldBe WorkspaceState.UpdateFailed
            ws.currentBillingAccountOnGoogleProject shouldBe Some(testData.billingAccountName)
          }
        }
      }
    }

  it should "mark the change as successful after updating a workspace in the UpdateFailed state" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val newBillingAccount = RawlsBillingAccountName(UUID.randomUUID.toString).some
      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- List(
            testData.workspace,
            testData.workspace.copy(
              name = testData.workspace.name + "copy",
              workspaceId = UUID.randomUUID().toString,
              state = WorkspaceState.UpdateFailed,
              errorMessage = "update failed error".some
            )
          ).traverse_(workspaceQuery.createOrUpdate)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = newBillingAccount,
            testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val gcsDAO = new MockGoogleServicesDAO("test") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           rawlsTracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] =
          Future.successful(
            new ProjectBillingInfo().setBillingAccountName(newBillingAccount.value.value).setBillingEnabled(true)
          )
      }

      BillingAccountChangeSynchronizer(
        dataSource,
        gcsDAO,
        new MockSamDAO(dataSource) {
          override def listResourceChildren(resourceTypeName: SamResourceTypeName,
                                            resourceId: String,
                                            ctx: RawlsRequestContext
          ): Future[Seq[SamFullyQualifiedResourceId]] =
            Future.successful(Seq.empty)
        }
      ).updateBillingAccounts
        .unsafeRunSync()

      runAndWait {
        for {
          lastChange <- BillingAccountChanges.getLastChange(testData.billingProject.projectName)
          billingProject <- rawlsBillingProjectQuery.load(testData.billingProject.projectName)
          workspaces <- workspaceQuery.withBillingProject(testData.billingProject.projectName).read
        } yield {
          lastChange.value.googleSyncTime shouldBe defined
          lastChange.value.outcome.value match {
            case Success =>
            case Failure(_) =>
              fail("should not fail when updating workspaces succeed")
          }

          billingProject.value.invalidBillingAccount shouldBe false
          billingProject.value.message shouldBe empty

          forAll(workspaces) { ws =>
            ws.errorMessage shouldNot be(defined)
            ws.state shouldBe WorkspaceState.Ready
            ws.currentBillingAccountOnGoogleProject shouldBe newBillingAccount
          }
        }
      }
    }

  it should "not try to update the (non-existent) google project of a v2 billing project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(RawlsBillingAccountName("original-ba"))
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("v2-Billing-Project"),
                                               CreationStatuses.Ready,
                                               originalBillingAccount,
                                               None,
                                               None
      )
      val v2Workspace = Workspace(
        billingProject.projectName.value,
        "v2 workspace",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("v2WorkspaceGoogleProject"),
        Option(GoogleProjectNumber("43")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(newBillingAccount),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val exceptionMessage = "it tried to update the non-existent google project of the v2 billing project."
      val failingGcsDao = spy(new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           rawlsTracingContext: RawlsTracingContext
        ): Future[ProjectBillingInfo] =
          if (googleProjectId == billingProject.googleProjectId)
            Future.failed(new RawlsException(exceptionMessage))
          else
            super.getBillingInfoForGoogleProject(googleProjectId)
      })

      val samDAO = new MockSamDAO(dataSource) {
        override def listResourceChildren(resourceTypeName: SamResourceTypeName,
                                          resourceId: String,
                                          ctx: RawlsRequestContext
        ): Future[Seq[SamFullyQualifiedResourceId]] =
          Future.successful(Seq.empty)
      }

      BillingAccountChangeSynchronizer(dataSource, gcsDAO = failingGcsDao, samDAO).updateBillingAccounts.unsafeRunSync

      def getErrorMessage(workspace: Workspace): ReadAction[Option[String]] =
        workspaceQuery.findByIdOrFail(workspace.workspaceId).map(_.errorMessage)

      val workspace = runAndWait(workspaceQuery.findByIdOrFail(v2Workspace.workspaceId))
      workspace.currentBillingAccountOnGoogleProject shouldBe Some(newBillingAccount)
      workspace.errorMessage shouldBe empty
    }

  it should "work with v2 workspaces that use the billing project's old Google project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(billingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )

      val workspaceWithBillingProjectGoogleProject = Workspace(
        billingProject.projectName.value,
        "reuse",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId(billingProject.projectName.value),
        billingProject.googleProjectNumber,
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val v2Workspace = Workspace(
        billingProject.projectName.value,
        "v2",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("differentId"),
        Option(GoogleProjectNumber("42")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val workspaceWithoutBillingAccount = v2Workspace.copy(
        name = "noBillingAccount",
        workspaceId = UUID.randomUUID().toString,
        googleProjectId = GoogleProjectId("noBillingAccount"),
        googleProjectNumber = Option(GoogleProjectNumber("44")),
        currentBillingAccountOnGoogleProject = None
      )

      val allWorkspaceGoogleProjects = List(workspaceWithBillingProjectGoogleProject.googleProjectId,
                                            v2Workspace.googleProjectId,
                                            workspaceWithoutBillingAccount.googleProjectId
      )

      val newBillingAccount = RawlsBillingAccountName("new-ba")
      val spiedGcsDao = spy(new MockGoogleServicesDAO("test"))
      val childlessMockSamDAO = new MockSamDAO(_: SlickDataSource) {
        override def listResourceChildren(resourceTypeName: SamResourceTypeName,
                                          resourceId: String,
                                          ctx: RawlsRequestContext
        ): Future[Seq[SamFullyQualifiedResourceId]] =
          Future.successful(Seq.empty)
      }
      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(workspaceWithBillingProjectGoogleProject)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- workspaceQuery.createOrUpdate(workspaceWithoutBillingAccount)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(newBillingAccount),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      BillingAccountChangeSynchronizer(dataSource,
                                       spiedGcsDao,
                                       childlessMockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      every(
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.currentBillingAccountOnGoogleProject)
      ) shouldBe Some(newBillingAccount)

      allWorkspaceGoogleProjects.map { googleProject =>
        verify(spiedGcsDao, times(1)).setBillingAccount(ArgumentMatchers.eq(googleProject),
                                                        ArgumentMatchers.eq(newBillingAccount.some),
                                                        any()
        )
      }
    }
}
