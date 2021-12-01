package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.google.cloud.storage.{Bucket, Storage}
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{RETURNS_SMART_NULLS, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatestplus.mockito.MockitoSugar.mock
import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import java.sql.SQLException
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.concurrent.Future
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
      runAndWait(V1WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)) shouldBe ()
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

  "createTempBucket" should "create a new bucket in the same region" in {
    val sourceProject = "something"
    val sourceBucketName = "something_bucket"
    val destProject = "something_else"
    val v1Workspace = minimalTestData.v1Workspace.copy(
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = sourceBucketName
    )

    val googleStorageService = mock[GoogleStorageService[IO]](RETURNS_SMART_NULLS)
    val sourceBucket = mock[Bucket](RETURNS_SMART_NULLS)

    when(sourceBucket.getLabels).thenReturn(null) // Thanks Google
    when(sourceBucket.getLocation).thenReturn("somewhere over there")

    when(
      googleStorageService.getBucket(
        any[GoogleProject],
        ArgumentMatchers.eq(GcsBucketName(sourceBucketName))
      )
    ).thenReturn(IO.pure(sourceBucket.some))

    when(
      googleStorageService.insertBucket(
        googleProject = ArgumentMatchers.eq(GoogleProject(destProject)),
        bucketName = any[GcsBucketName]
      )
    ).thenReturn(fs2.Stream.empty)

    withMinimalTestDatabase { dataSource =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1Workspace),
          V1WorkspaceMigrationMonitor.schedule(v1Workspace)
        )
      }

      val attempt = runAndWait(migrations.filter(_.workspaceId === v1Workspace.workspaceIdAsUUID).result).head
      def futureToIO[T] = IO.fromFuture[T] _ compose IO.pure[Future[T]]

      (for {
        res <- V1WorkspaceMigrationMonitor
          .createTempBucket(attempt, v1Workspace, GoogleProject(destProject), googleStorageService)
        (_, writeAction) = res
        _ <- futureToIO(dataSource.database.run(writeAction))
        query = migrations.filter(_.workspaceId === v1Workspace.workspaceIdAsUUID).result.map(_.head)
        record <- futureToIO(dataSource.database.run(query))
      } yield {
          record.tmpBucketName.value.value should startWith("terra-workspace-migration-")
          record.tmpBucketCreated shouldBe defined
      }).unsafeRunSync
    }
  }

}
