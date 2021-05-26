package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeEntityReference, AttributeName, AttributeNull, AttributeNumber, AttributeString, AttributeValueList, AttributeValueRawJson}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition, RemoveAttribute}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class DeltaLayerTranslatorSpec extends AnyFlatSpec with Matchers {

  behavior of "DeltaLayerTranslator"

  it should "reject updates if the collection of EntityUpdateDefinitions is empty" in {
    val updates = Seq.empty[EntityUpdateDefinition]

    val caught = intercept[DeltaLayerException] {
      DeltaLayerTranslator.validateEntityUpdates(updates)
    }

    caught.code shouldBe StatusCodes.BadRequest
  }

  it should "reject updates if the collection of EntityUpdateDefinitions has no AttributeUpdateOperations" in {
    val updates = (1 to 10) map { idx =>
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", Seq.empty[AttributeUpdateOperation])
    }
    val caught = intercept[DeltaLayerException] {
      DeltaLayerTranslator.validateEntityUpdates(updates)
    }

    caught.code shouldBe StatusCodes.BadRequest
  }

  it should "reject updates if the collection of AttributeUpdateOperations contains non-AddUpdateAttribute" in {
    val updates = (1 to 10) map { updateIdx =>
      val ops = (1 to 5) map { opIdx =>
        // toss in one offender at update 3 / operation 2
        if (updateIdx == 3 && opIdx == 2) {
          RemoveAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"))
        } else
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), AttributeNumber(opIdx))
      }
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", ops)
    }

    val caught = intercept[DeltaLayerException] {
      DeltaLayerTranslator.validateEntityUpdates(updates)
    }

    caught.code shouldBe StatusCodes.BadRequest
  }

  it should "allow updates if one AddUpdateAttribute for one entity" in {
    val numUpdates = 1
    val numOpsPerUpdate = 1
    val updates = (1 to numUpdates) map { updateIdx =>
      val ops = (1 to numOpsPerUpdate) map { opIdx =>
        AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), AttributeNumber(opIdx))
      }
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", ops)
    }

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  it should "allow updates if multiple AddUpdateAttributes for one entity" in {
    val numUpdates = 1
    val numOpsPerUpdate = 5
    val updates = (1 to numUpdates) map { updateIdx =>
      val ops = (1 to numOpsPerUpdate) map { opIdx =>
        AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), AttributeNumber(opIdx))
      }
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", ops)
    }

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  it should "allow updates if one AddUpdateAttribute for multiple entities" in {
    val numUpdates = 3
    val numOpsPerUpdate = 1
    val updates = (1 to numUpdates) map { updateIdx =>
      val ops = (1 to numOpsPerUpdate) map { opIdx =>
        AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), AttributeNumber(opIdx))
      }
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", ops)
    }

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  it should "allow updates if 0-n AddUpdateAttributes for multiple entities" in {
    val numUpdates = 10

    val updates = (1 to numUpdates) map { updateIdx =>
      val numOpsPerUpdate = updateIdx - 1
      val ops = (1 to numOpsPerUpdate) map { opIdx =>
        AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), AttributeNumber(opIdx))
      }
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", ops)
    }

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  val unsupportedValueTypes = Seq(AttributeNull,
    AttributeValueRawJson("""{"foo":["bar","baz"]}"""),
    AttributeEntityReference("someType", "someName"),
    AttributeValueList(Seq(AttributeString("foo"), AttributeString("bar"))))

  unsupportedValueTypes foreach { badType =>
    val badTypeName = badType.getClass.getSimpleName

    it should s"reject updates if they contain $badTypeName" in {
      val updates = (1 to 10) map { updateIdx =>
        val ops = (1 to 5) map { opIdx =>
          // toss in one offender at update 3 / operation 2
          if (updateIdx == 3 && opIdx == 2) {
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), badType)
          } else
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-$opIdx"), AttributeNumber(opIdx))
        }
        EntityUpdateDefinition(UUID.randomUUID().toString, "some-type", ops)
      }

      val caught = intercept[DeltaLayerException] {
        DeltaLayerTranslator.validateEntityUpdates(updates)
      }

      caught.code shouldBe StatusCodes.BadRequest
    }
  }

  it should "allow updates if they contain any of AttributeString, AttributeNumber, AttributeBoolean" in {
    val updates = Seq(
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeNumber(1)),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-2"), AttributeBoolean(false)))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-3"), AttributeString("hi")),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-4"), AttributeBoolean(true)))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "another-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-4"), AttributeNumber(2)),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-3"), AttributeNumber(3)))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "a-third-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-2"), AttributeString("hey")),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeString("hello")))),
    )

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  it should "reject updates if they contain entity names - aka datarepo_row_id - with an invalid UUID" in {
    val updates = Seq(
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeNumber(1)))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeString("hi")))),
      EntityUpdateDefinition("i-am-not-a-valid-uuid", "some-type", // <-- invalid UUID!
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeNumber(1)))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeString("hi")))),
    )

    val caught = intercept[DeltaLayerException] {
      DeltaLayerTranslator.validateEntityUpdates(updates)
    }

    caught.code shouldBe StatusCodes.BadRequest
  }

}
