package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsStdBimonadForFunction0.*>
import cats.implicits.catsSyntaxApplicativeId
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, RawlsBillingProject, WorkspaceName}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.mockito.Answers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import java.sql.SQLException
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class V1WorkspaceMigrationMonitorSpec
  extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with TestDriverComponent
    with Eventually
    with OptionValues {

  val testKit: ActorTestKit = ActorTestKit()
  implicit val logger = new ConsoleLogger("unit_test", LogLevel(false, false, true, true))

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "isMigrating" should "return false when a workspace is not being migrated" in {
    withMinimalTestDatabase { _ =>
      runAndWait(V1WorkspaceMigrationMonitor.isMigrating(minimalTestData.v1Workspace)) shouldBe false
    }
  }

  "schedule" should "error when a workspace is scheduled concurrently" in {
    withMinimalTestDatabase { _ =>
      runAndWait(V1WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)) shouldBe()
      assertThrows[SQLException] {
        runAndWait(V1WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace))
      }
    }
  }

  // use an existing test project (broad-dsde-dev)
  "temp Integration Test" should "for realsies create a new bucket in the same region" ignore {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val config = ConfigFactory.load()
    val gcsConfig = config.getConfig("gcs")
    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"
    val v1WorkspaceCopy = minimalTestData.v1Workspace.copy(namespace = sourceProject, googleProjectId = GoogleProjectId(sourceProject), bucketName = sourceBucket)

    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1WorkspaceCopy),
          V1WorkspaceMigrationMonitor.schedule(v1WorkspaceCopy)
        )
      }
      val attempt = runAndWait(migrations.filter(_.workspaceId === v1WorkspaceCopy.workspaceIdAsUUID).result).head
      val writeAction = GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- V1WorkspaceMigrationMonitor.createTempBucket(attempt, v1WorkspaceCopy, GoogleProject(destProject), googleStorageService)
          (bucketName, writeAction) = res
          loadedBucket <- googleStorageService.getBucket(GoogleProject(destProject), bucketName)
          _ <- googleStorageService.deleteBucket(GoogleProject(destProject), bucketName).compile.drain
        } yield {
          loadedBucket shouldBe defined
          writeAction
        }
      }.unsafeRunSync
      runAndWait(writeAction)
    }
  }

  "claimAndConfigureGoogleProject" should "return a valid database operation" in {
    withMinimalTestDatabase { dataSource =>
      runAndWait {
        DBIO.seq(
          V1WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)
        )
      }

      val wsService = mock[WorkspaceService](Answers.RETURNS_SMART_NULLS)
      when {
        wsService.setupGoogleProject(
          any[RawlsBillingProject],
          any[RawlsBillingAccountName],
          any[String],
          any[WorkspaceName],
          any[Span]
        )
      }
        .thenReturn {
          Future.pure((GoogleProjectId("google-project-id"), GoogleProjectNumber("google-project-number")))
        }

      val (_, _, dbOp) = Await.result(
        dataSource.database
          .run {
            V1WorkspaceMigrationMonitor.migrations
              .filter(_.workspaceId === minimalTestData.v1Workspace.workspaceIdAsUUID)
              .result
          }
          .map(_.head)
          .flatMap { attempt =>
            V1WorkspaceMigrationMonitor.claimAndConfigureNewGoogleProject(
              attempt,
              wsService,
              minimalTestData.v1Workspace,
              minimalTestData.billingProject
            )
          },
        5.seconds // really should be instant if we've mocked correctly?
      )

      runAndWait(DBIO.seq(dbOp))

    }
  }

}
