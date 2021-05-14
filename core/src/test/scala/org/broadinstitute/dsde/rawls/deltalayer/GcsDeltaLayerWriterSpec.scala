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

import java.util.UUID

class GcsDeltaLayerWriterSpec extends AnyFlatSpec {

  implicit val actorSystem: ActorSystem = ActorSystem("GcsDeltaLayerWriterSpec")

  val localStorage = FakeGoogleStorageInterpreter

  val bucket = GcsBucketName("unittest-bucket-name")

  val deltaLayerWriter = new GcsDeltaLayerWriter(localStorage, bucket, "metricsPrefix")

  behavior of "GcsDeltaLayerWriter"

  it should "write the specified file to the bucket" in {
    val dest = Destination(UUID.randomUUID(), UUID.randomUUID())
    val attrUpdates: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attrName"), AttributeString("attrValue")))
    val entityUpdates: Seq[EntityUpdateDefinition] = Seq(EntityUpdateDefinition("name", "type", attrUpdates))
    val testInsert = DeltaInsert("vTest", UUID.randomUUID(), new DateTime(), RawlsUserSubjectId("1234"), dest, entityUpdates)

    val expectedPath = deltaLayerWriter.filePath(testInsert)
    val expectedContents = deltaLayerWriter.serializeFile(testInsert)

    deltaLayerWriter.writeFile(testInsert)

    val optBlob = localStorage.unsafeGetBlobBody(bucket, expectedPath).unsafeRunSync()

    assert(optBlob.nonEmpty, "file should have been written to storage")
    optBlob.map { actual =>
      assertResult(expectedContents) {
        actual
      }
    }
  }

}
