package org.broadinstitute.dsde.rawls.deltalayer

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.flatspec.AnyFlatSpec
import TestExecutionContext.testExecutionContext
import akka.actor.ActorSystem
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, IO}
import com.google.cloud.storage.Storage.BlobWriteOption
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{BlobInfo, Storage, StorageException}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, DeltaInsert, Destination, RawlsUserSubjectId}
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageInterpreter}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.RecoverMethods.recoverToExceptionIf
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.util.Try

class GcsDeltaLayerWriterSpec extends AnyFlatSpec with Eventually with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("GcsDeltaLayerWriterSpec")

  behavior of "GcsDeltaLayerWriter"

  it should "write the specified file to the bucket" in {
    val bucket = GcsBucketName("successful-write")

    // explicitly create the storage so we can query it directly
    val storage = LocalStorageHelper.getOptions.getService
    val deltaLayerWriter = getGcsWriter(bucket, Some(storage))

    // create the object to be written
    val dest = Destination(UUID.randomUUID(), UUID.randomUUID())
    val attrUpdates: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attrName"), AttributeString("attrValue")))
    val entityUpdates: Seq[EntityUpdateDefinition] = Seq(EntityUpdateDefinition("name", "type", attrUpdates))
    val testInsert = DeltaInsert("vTest", UUID.randomUUID(), new DateTime(), RawlsUserSubjectId("1234"), dest, entityUpdates)

    // calculate expected file path and contents
    val expectedPath = deltaLayerWriter.filePath(testInsert)
    val expectedContents = deltaLayerWriter.serializeFile(testInsert)

    // write the object via Delta Layer
    deltaLayerWriter.writeFile(testInsert) map { _ =>
      // confirm we wrote the file correctly. The LocalStorageHelper in-memory storage engine
      // seems to have some delays, so use eventually to allow for delays and retries
      eventually (timeout(Span(30, Seconds)), interval(Span(500, Millis))) {
        val optBlob = Try(storage.get(bucket.value, expectedPath.value).getContent()).toOption
        optBlob should not be empty
        optBlob.map { actual =>
          assertResult(expectedContents) {
            actual.mkString
          }
        }
      }
    }
  }

  it should "bubble up errors during write" in {
    val bucket = GcsBucketName("this-should-throw-error")

    // under the covers, Storage.writer is the Google library method that gets called. So, mock that
    // and force it to throw
    val mockedException = new StorageException(418, "intentional unit test failure")
    val throwingStorageHelper = mock[Storage]
    when(throwingStorageHelper.writer(any[BlobInfo], any[BlobWriteOption]))
      .thenThrow(mockedException)

    val throwingWriter = getGcsWriter(bucket, Some(throwingStorageHelper))

    // create the object to be written
    val dest = Destination(UUID.randomUUID(), UUID.randomUUID())
    val attrUpdates: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attrName"), AttributeString("attrValue")))
    val entityUpdates: Seq[EntityUpdateDefinition] = Seq(EntityUpdateDefinition("name", "type", attrUpdates))
    val testInsert = DeltaInsert("vTest", UUID.randomUUID(), new DateTime(), RawlsUserSubjectId("1234"), dest, entityUpdates)

    // write the object via the Delta Layer writer we have configured to run into an exception
    val caught = recoverToExceptionIf[StorageException] {
       throwingWriter.writeFile(testInsert)
    }

    caught.map { ex =>
      ex shouldBe mockedException
    }

  }

  it should "calculate the desired file path to which we will write" in {
    val bucket = GcsBucketName("path-calculation")
    val deltaLayerWriter = getGcsWriter(bucket)

    // workspace/${workspaceId}/reference/${referenceId}/insert/${insertId}.json
    val insertId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val referenceId = UUID.randomUUID()

    val dest = Destination(workspaceId, referenceId)
    val attrUpdates: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attrName"), AttributeString("attrValue")))
    val entityUpdates: Seq[EntityUpdateDefinition] = Seq(EntityUpdateDefinition("name", "type", attrUpdates))
    val testInsert = DeltaInsert("vTest", insertId, new DateTime(), RawlsUserSubjectId("1234"), dest, entityUpdates)

    val expected = GcsBlobName(s"workspace/${workspaceId}/reference/${referenceId}/insert/${insertId}.json")

    val actual = deltaLayerWriter.filePath(testInsert)

    actual shouldBe expected

  }

  it should "serialize DeltaInsert correctly" is (pending) // NB not worth testing until model class is stable

  def getGcsWriter(bucket: GcsBucketName, storage: Option[Storage] = None): GcsDeltaLayerWriter = {
    implicit val cs = IO.contextShift(testExecutionContext)
    implicit val timer = IO.timer(testExecutionContext)
    implicit val logger = Slf4jLogger.getLogger[IO]

    val db = storage.getOrElse(LocalStorageHelper.getOptions().getService())
    val blocker = Blocker.liftExecutionContext(testExecutionContext)
    val semaphore = Semaphore[IO](1).unsafeRunSync

    val localStorage = GoogleStorageInterpreter[IO](db, blocker, Some(semaphore))

    new GcsDeltaLayerWriter(localStorage, bucket, "metricsPrefix")
  }

}
