package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition, RemoveAttribute}
import org.broadinstitute.dsde.rawls.model.deltalayer.v1.DeltaRow
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeBoolean, AttributeEntityReference, AttributeEntityReferenceEmptyList, AttributeEntityReferenceList, AttributeName, AttributeNull, AttributeNumber, AttributeString, AttributeValueEmptyList, AttributeValueList, AttributeValueRawJson}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsString, JsValue}

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

  val unsupportedValueTypes = Seq(
    AttributeValueRawJson("""{"foo":["bar","baz"]}"""),
    AttributeEntityReference("someType", "someName"))

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

  val supportedTypes = Seq(
    AttributeString("hi"),
    AttributeNumber(1),
    AttributeBoolean(true),
    AttributeNull
  )

  supportedTypes foreach { goodType =>
    val goodTypeName = goodType.getClass.getSimpleName

    it should s"allow updates if they contain $goodType" in {
      val updates = Seq(
        EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), goodType))),
        EntityUpdateDefinition(UUID.randomUUID().toString, "another-type",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS(s"another-attr"), goodType)))
      )

      DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
    }
  }

  it should "allow updates if they contain AttributeValueList, if the lists have only supported types" in {
    val updates = Seq(
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-scalar"), AttributeNumber(1)),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-list"), AttributeValueList(Seq(
            AttributeString("list item 1"),
            AttributeNumber(123),
            AttributeBoolean(true)))))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "another-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeBoolean(false)),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-2"), AttributeValueList(Seq(
            AttributeString("a string"),
            AttributeNumber(456),
            AttributeBoolean(false))))))
    )

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  it should "allow updates if they contain AttributeValueEmptyList" in {
    val updates = Seq(
      EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-scalar"), AttributeNumber(1)),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-list"), AttributeValueList(Seq(
            AttributeString("list item 1"),
            AttributeNumber(123),
            AttributeBoolean(true)))))),
      EntityUpdateDefinition(UUID.randomUUID().toString, "another-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeBoolean(false)),
          AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-2"), AttributeValueEmptyList)))
    )

    DeltaLayerTranslator.validateEntityUpdates(updates) shouldBe updates
  }

  val unsupportedLists = Seq(
    AttributeEntityReferenceEmptyList,
    AttributeEntityReferenceList(Seq(
      AttributeEntityReference("someType", "someName"),
      AttributeEntityReference("someType", "anotherName"),
    ))
  )

  unsupportedLists foreach { badList =>
    val badTypeName = badList.getClass.getSimpleName

    it should s"reject updates if they contain AttributeLists, if the list is a $badTypeName" in {
      val updates = Seq(
        EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-scalar"), AttributeNumber(1)),
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-list"), AttributeValueList(Seq(
              AttributeString("list item 1"),
              AttributeNumber(123),
              AttributeBoolean(true)))))),
        EntityUpdateDefinition(UUID.randomUUID().toString, "another-type",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeBoolean(false)),
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-2"), badList)))
      )

      val caught = intercept[DeltaLayerException] {
        DeltaLayerTranslator.validateEntityUpdates(updates)
      }

      caught.code shouldBe StatusCodes.BadRequest
    }
  }

  val unsupportedListElements = Seq(
      AttributeValueRawJson("""{"foo":["bar","baz"]}"""))

  unsupportedListElements foreach { badType =>
    val badTypeName = badType.getClass.getSimpleName

    it should s"reject updates if they contain AttributeValueLists, if the list contains an $badTypeName" in {
      val updates = Seq(
        EntityUpdateDefinition(UUID.randomUUID().toString, "some-type",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-scalar"), AttributeNumber(1)),
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-list"), AttributeValueList(Seq(
              AttributeString("list item 1"),
              AttributeNumber(123),
              AttributeBoolean(true)))))),
        EntityUpdateDefinition(UUID.randomUUID().toString, "another-type",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-1"), AttributeBoolean(false)),
            AddUpdateAttribute(AttributeName.withDefaultNS(s"attr-2"), AttributeValueList(Seq(
              AttributeString("list item 1"),
              badType, // <-- unsupported element
              AttributeBoolean(true))))))
      )

      val caught = intercept[DeltaLayerException] {
        DeltaLayerTranslator.validateEntityUpdates(updates)
      }

      caught.code shouldBe StatusCodes.BadRequest
    }
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

  val translations: Map[Attribute, JsValue] = Map(
    AttributeString("hello") -> JsString("hello"),
    AttributeNumber(123) -> JsNumber(123),
    AttributeBoolean(true) -> JsBoolean(true),
    AttributeNull -> JsNull,
    AttributeValueEmptyList -> JsArray.empty,
    AttributeValueList(Seq(AttributeString("hi"), AttributeNumber(456), AttributeNull, AttributeBoolean(false))) ->
      JsArray(JsString("hi"), JsNumber(456), JsNull, JsBoolean(false))
  )

  translations foreach {
    case (input, output) =>
      val inputName = input.getClass.getSimpleName
      val outputName = output.getClass.getSimpleName

      it should s"translate $inputName into DeltaRow with $outputName properly" in {
        val rowId = UUID.randomUUID()
        val updates = Seq(
          EntityUpdateDefinition(rowId.toString, "some-type",
            Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attr-name"), input)))
        )

        val actual = DeltaLayerTranslator.translateEntityUpdates(updates)
        val expected = Seq(DeltaRow(rowId, "attr-name", output))

        actual shouldBe expected
      }
  }

  it should "omit default attribute namespaces but keep other namespaces when translating" in {
    val rowId = UUID.randomUUID()

    val updates = Seq(
      EntityUpdateDefinition(rowId.toString, "some-type",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("defaultattr"), AttributeString("hello")),
          AddUpdateAttribute(AttributeName.withLibraryNS("libraryattr"), AttributeString("hi")),
          AddUpdateAttribute(AttributeName.withTagsNS(), AttributeString("imatag")),
        )))

    val actual = DeltaLayerTranslator.translateEntityUpdates(updates)
    val expected = Seq(
      DeltaRow(rowId, "defaultattr", JsString("hello")),
      DeltaRow(rowId, s"${AttributeName.libraryNamespace}:libraryattr", JsString("hi")),
      DeltaRow(rowId, s"${AttributeName.tagsNamespace}:tags", JsString("imatag")),
    )

    actual shouldBe expected
  }



}
