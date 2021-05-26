package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeNumber}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition, RemoveAttribute}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      EntityUpdateDefinition(s"name-$idx", "some-type", Seq.empty[AttributeUpdateOperation])
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
      EntityUpdateDefinition(s"name-$updateIdx", "some-type", ops)
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
      EntityUpdateDefinition(s"name-$updateIdx", "some-type", ops)
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
      EntityUpdateDefinition(s"name-$updateIdx", "some-type", ops)
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
      EntityUpdateDefinition(s"name-$updateIdx", "some-type", ops)
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
      EntityUpdateDefinition(s"name-$updateIdx", "some-type", ops)
    }

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  it should "reject updates if they contain AttributeNull" is (pending)

  it should "reject updates if they contain AttributeValueRawJson" is (pending)

  it should "reject updates if they contain AttributeEntityReference" is (pending)

  it should "reject updates if they contain AttributeList" is (pending)

  it should "allow updates if they contain any of AttributeString, AttributeNumber, AttributeBoolean"

  it should "reject updates if they contain entity names - aka datarepo_row_id - with an invalid UUID" is (pending)

}
