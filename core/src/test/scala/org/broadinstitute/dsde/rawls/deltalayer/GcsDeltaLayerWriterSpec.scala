package org.broadinstitute.dsde.rawls.deltalayer

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.flatspec.AnyFlatSpec
import TestExecutionContext.testExecutionContext
import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, DeltaInsert, Destination, RawlsUserSubjectId}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.util.UUID

class GcsDeltaLayerWriterSpec extends AnyFlatSpec with Eventually with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("GcsDeltaLayerWriterSpec")

  val localStorage = FakeGoogleStorageInterpreter

  val bucket = GcsBucketName("unittest-bucket-name")

  val deltaLayerWriter = new GcsDeltaLayerWriter(localStorage, bucket, "metricsPrefix")

  behavior of "GcsDeltaLayerWriter"

  it should "write the specified file to the bucket" in {
    // create the object to be written
    val dest = Destination(UUID.randomUUID(), UUID.randomUUID())
    val attrUpdates: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attrName"), AttributeString("attrValue")))
    val entityUpdates: Seq[EntityUpdateDefinition] = Seq(EntityUpdateDefinition("name", "type", attrUpdates))
    val testInsert = DeltaInsert("vTest", UUID.randomUUID(), new DateTime(), RawlsUserSubjectId("1234"), dest, entityUpdates)

    // calculate expected file path and contents
    val expectedPath = deltaLayerWriter.filePath(testInsert)
    val expectedContents = deltaLayerWriter.serializeFile(testInsert)

    // write the object via Delta Layer
    deltaLayerWriter.writeFile(testInsert)

    // confirm we wrote the file correctly. The FakeGoogleStorageInterpreter in-memory storage engine
    // seems to have some delays, so use eventually to allow for delays and retries
    eventually (timeout(Span(30, Seconds)), interval(Span(500, Millis))) {
      val optBlob = localStorage.unsafeGetBlobBody(bucket, expectedPath).unsafeRunSync()
      optBlob should not be empty
      optBlob.map { actual =>
        assertResult(expectedContents) {
          actual
        }
      }
    }
  }

  it should "bubble up errors during write" is (pending)

  it should "calculate the desired file path to which we will write" is (pending)

  it should "serialize DeltaInsert correctly" is (pending) // NB not worth testing until model class is stable

  // TODO: should write a test from the EntityApiService layer down, too


}
