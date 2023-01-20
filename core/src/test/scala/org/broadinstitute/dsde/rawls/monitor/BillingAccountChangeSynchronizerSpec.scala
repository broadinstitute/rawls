package org.broadinstitute.dsde.rawls.monitor

import akka.http.scaladsl.model.StatusCodes
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxApply, catsSyntaxOptionId, toFoldableOps}
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

  "BillingAccountChangeSynchronizer" should "update the billing account on all v1 and v2 workspaces in a billing project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(billingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val v1Workspace = Workspace(
        billingProject.projectName.value,
        "v1",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V1,
        GoogleProjectId(billingProject.projectName.value),
        billingProject.googleProjectNumber,
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace
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
        WorkspaceType.RawlsWorkspace
      )
      val workspaceWithoutBillingAccount = v2Workspace.copy(
        name = "noBillingAccount",
        workspaceId = UUID.randomUUID().toString,
        googleProjectId = GoogleProjectId("anotherId"),
        googleProjectNumber = Option(GoogleProjectNumber("44")),
        currentBillingAccountOnGoogleProject = None
      )

      val allWorkspaceGoogleProjects =
        List(v1Workspace.googleProjectId, v2Workspace.googleProjectId, workspaceWithoutBillingAccount.googleProjectId)

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(v1Workspace)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- workspaceQuery.createOrUpdate(workspaceWithoutBillingAccount)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(newBillingAccount),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      BillingAccountChangeSynchronizer(dataSource,
                                       mockGcsDAO,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      every(
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.currentBillingAccountOnGoogleProject)
      ) shouldBe Some(newBillingAccount)

      allWorkspaceGoogleProjects.map { googleProject =>
        verify(mockGcsDAO, times(1)).setBillingAccount(ArgumentMatchers.eq(googleProject),
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
        WorkspaceType.RawlsWorkspace
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
        WorkspaceType.RawlsWorkspace
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
                                           span: OpenCensusSpan = null
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
        WorkspaceType.RawlsWorkspace
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

      verify(mockGcsDAO, times(0)).setBillingAccountName(workspace.googleProjectId, originalBillingAccount.value)
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
        WorkspaceVersions.V1,
        GoogleProjectId(billingProject.projectName.value),
        billingProject.googleProjectNumber,
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace
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
        WorkspaceVersions.V1,
        GoogleProjectId("differentId"),
        Option(GoogleProjectNumber("43")),
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace
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
        WorkspaceType.RawlsWorkspace
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

  // TODO: CA-1235 Remove during cleanup once all workspaces have their own Google project
  it should "propagate error messages to all v1 workspaces in a billing project" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(RawlsBillingAccountName("original-ba"))
      val billingProject = RawlsBillingProject(
        RawlsBillingProjectName("v1-Billing-Project"),
        CreationStatuses.Ready,
        originalBillingAccount,
        None,
        googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val v1GoogleProjectId = GoogleProjectId(billingProject.projectName.value)
      val firstV1Workspace = Workspace(
        billingProject.projectName.value,
        "first-v1-workspace",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V1,
        v1GoogleProjectId,
        billingProject.googleProjectNumber,
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace
      )
      val secondV1Workspace = Workspace(
        billingProject.projectName.value,
        "second-v1-workspace",
        UUID.randomUUID().toString,
        "bucketName",
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V1,
        v1GoogleProjectId,
        billingProject.googleProjectNumber,
        originalBillingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace
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
        WorkspaceType.RawlsWorkspace
      )

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(billingProject)
          _ <- workspaceQuery.createOrUpdate(firstV1Workspace)
          _ <- workspaceQuery.createOrUpdate(v2Workspace)
          _ <- workspaceQuery.createOrUpdate(secondV1Workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                             Option(newBillingAccount),
                                                             testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = spy(new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           span: OpenCensusSpan = null
        ): Future[ProjectBillingInfo] =
          if (googleProjectId == v1GoogleProjectId)
            Future.failed(new RawlsException(exceptionMessage))
          else
            super.getBillingInfoForGoogleProject(googleProjectId)
      })

      BillingAccountChangeSynchronizer(dataSource,
                                       gcsDAO = failingGcsDao,
                                       mockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      def getBillingAccountErrorMessage(workspace: Workspace): ReadAction[Option[String]] =
        workspaceQuery.findByIdOrFail(workspace.workspaceId).map(_.errorMessage)

      runAndWait {
        for {
          firstV1WsError <- getBillingAccountErrorMessage(firstV1Workspace)
          secondV1WsError <- getBillingAccountErrorMessage(secondV1Workspace)
          v2WsError <- getBillingAccountErrorMessage(v2Workspace)
        } yield {
          firstV1WsError.value should include(exceptionMessage)
          secondV1WsError.value should include(exceptionMessage)
          v2WsError shouldBe empty
        }
      }

      verify(failingGcsDao, times(1)).setBillingAccountName(
        ArgumentMatchers.eq(v1GoogleProjectId),
        ArgumentMatchers.eq(newBillingAccount),
        any[OpenCensusSpan]
      )
    }

  it should "sync only the latest billing account changes" in
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val finalBillingAccountName = runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- workspaceQuery.createOrUpdate(testData.v1Workspace)

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
                                           span: OpenCensusSpan
        ): Future[ProjectBillingInfo] = {
          timesCalled.incrementAndGet()
          super.setBillingAccountName(googleProjectId, billingAccountName, span)
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
          _ <- workspaceQuery.createOrUpdate(testData.v1Workspace)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = RawlsBillingAccountName(UUID.randomUUID.toString).some,
            testData.userOwner.userSubjectId
          )
        } yield ()
      }

      val gcsDAO = new MockGoogleServicesDAO("test") {

        override def testDMBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean] =
          Future.successful(false)

        override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                           billingAccountName: RawlsBillingAccountName,
                                           span: OpenCensusSpan
        ): Future[ProjectBillingInfo] =
          Future.failed(new RawlsException("You do not have access to this billing account or it does not exist."))
      }

      BillingAccountChangeSynchronizer(dataSource, gcsDAO, mockSamDAO(dataSource)).updateBillingAccounts
        .unsafeRunSync()

      runAndWait {
        for {
          lastChange <- BillingAccountChanges.getLastChange(testData.billingProject.projectName)
          billingProject <- rawlsBillingProjectQuery.load(testData.billingProject.projectName)
          workspace <- workspaceQuery.findByIdOrFail(testData.v1Workspace.workspaceId)
        } yield {
          lastChange.value.googleSyncTime shouldBe defined
          lastChange.value.outcome.value.isFailure shouldBe true

          billingProject.value.invalidBillingAccount shouldBe true
          billingProject.value.message shouldBe defined

          workspace.errorMessage shouldBe defined
          workspace.currentBillingAccountOnGoogleProject shouldBe testData.v1Workspace.currentBillingAccountOnGoogleProject
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
                                           span: OpenCensusSpan
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
                                           span: OpenCensusSpan
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
            ws.currentBillingAccountOnGoogleProject shouldBe Some(testData.billingAccountName)
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
        WorkspaceType.RawlsWorkspace
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
                                           span: OpenCensusSpan = null
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
        WorkspaceType.RawlsWorkspace
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
        WorkspaceType.RawlsWorkspace
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
                                       mockGcsDAO,
                                       childlessMockSamDAO(dataSource)
      ).updateBillingAccounts.unsafeRunSync

      every(
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.currentBillingAccountOnGoogleProject)
      ) shouldBe Some(newBillingAccount)

      allWorkspaceGoogleProjects.map { googleProject =>
        verify(mockGcsDAO, times(1)).setBillingAccount(ArgumentMatchers.eq(googleProject),
                                                       ArgumentMatchers.eq(newBillingAccount.some),
                                                       any()
        )
      }
    }
}
