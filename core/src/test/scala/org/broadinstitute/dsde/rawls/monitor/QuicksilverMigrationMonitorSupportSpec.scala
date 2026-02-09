package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.slick.EntityCorrection
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueList,
  Entity
}
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatus.AttributeCorrectionStatusType
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.language.postfixOps

class QuicksilverMigrationMonitorSupportSpec
    extends QuicksilverMigrationMonitorSupport
    with AnyFlatSpecLike
    with Matchers {

  behavior of "compareLists"

  it should "return Correct for exact-same value lists" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Correct
  }

  it should "return Intersect when it starts with" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("baz")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Intersect
  }

  it should "return Intersect when common elements are in the same order" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("baz")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("bar"),
        AttributeString("baz"),
        AttributeString("qux"),
        AttributeString("qdf")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Intersect
  }

  it should "return Different when common elements are ordered but different quantity" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("bar"),
        AttributeString("baz")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("bar"),
        AttributeString("baz"),
        AttributeString("qux")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Different
  }

  it should "return Reordered when it was reordered" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("bar"),
        AttributeString("foo")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Reordered
  }

  it should "return Different with same elements but in different counts" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("foo")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Different
  }

  it should "return NothingInCommon with different elements" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("cat"),
        AttributeString("dog")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.NothingInCommon
  }

  behavior of "compareAttrs"

  it should "return TypeDifferent with different classes" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val correction = AttributeEntityReferenceList(
      Seq(
        AttributeEntityReference("type", "name1"),
        AttributeEntityReference("type", "name2")
      )
    )
    val actual = compareAttrs(current, correction, hint = "irrelevant")
    actual shouldBe AttributeCorrectionStatus.TypeDifferent
  }

  behavior of "compareEntities"

  it should "return CurrentGone if current entity does not exist" in {
    val current = None
    val correction = EntityCorrection(
      1,
      UUID.randomUUID(),
      "type",
      "name",
      Map()
    )
    val (actualStatus, actualAttrStatusMap) = compareEntities(current, correction)
    actualStatus shouldBe EntityCorrectionStatus.CurrentGone
    actualAttrStatusMap shouldBe empty
  }

  it should "skip attributes that no longer exist" in {
    val current = Some(
      Entity("type", "name", Map(AttributeName.withDefaultNS("second") -> AttributeValueList(Seq(AttributeNumber(42)))))
    )
    val correction = EntityCorrection(
      1,
      UUID.randomUUID(),
      "type",
      "name",
      Map(
        AttributeName.withDefaultNS("first") -> AttributeValueList(Seq(AttributeNumber(99))),
        AttributeName.withDefaultNS("second") -> AttributeValueList(Seq(AttributeNumber(42)))
      )
    )
    val (actualStatus, actualAttrStatusMap) = compareEntities(current, correction)
    actualStatus shouldBe EntityCorrectionStatus.Correct
    actualAttrStatusMap shouldBe Map(AttributeName.withDefaultNS("second") -> AttributeCorrectionStatus.Correct)
  }

  behavior of "calculateEntityStatus"

  it should "return Correct for empty attributes" in {
    val statusMap = Map.empty[AttributeName, AttributeCorrectionStatusType]
    val actual = calculateEntityStatus(statusMap)
    actual shouldBe EntityCorrectionStatus.Correct
  }

  val consistentCases = Map(
    AttributeCorrectionStatus.Correct -> EntityCorrectionStatus.Correct,
    AttributeCorrectionStatus.Reordered -> EntityCorrectionStatus.Correctable,
    AttributeCorrectionStatus.Corrected -> EntityCorrectionStatus.Corrected,
    AttributeCorrectionStatus.Intersect -> EntityCorrectionStatus.Intersect,
    AttributeCorrectionStatus.Different -> EntityCorrectionStatus.Different,
    AttributeCorrectionStatus.NothingInCommon -> EntityCorrectionStatus.NothingInCommon,
    AttributeCorrectionStatus.NotAList -> EntityCorrectionStatus.NotAList,
    AttributeCorrectionStatus.TypeDifferent -> EntityCorrectionStatus.TypeDifferent
  )

  consistentCases foreach { case (attrStatus, expectedEntityStatus) =>
    it should s"return $expectedEntityStatus when all attributes are $attrStatus" in {
      val statusMap = Map(
        AttributeName.withDefaultNS("foo") -> attrStatus,
        AttributeName.withLibraryNS("bar") -> attrStatus
      )
      val actual = calculateEntityStatus(statusMap)
      actual shouldBe expectedEntityStatus
    }
  }

  it should s"return MixedButNotCorrectable when attributes have multiple statuses but none are Reordered" in {
    val statusMap = Map(
      AttributeName.withDefaultNS("foo") -> AttributeCorrectionStatus.Correct,
      AttributeName.withLibraryNS("bar") -> AttributeCorrectionStatus.Different
    )
    val actual = calculateEntityStatus(statusMap)
    actual shouldBe EntityCorrectionStatus.MixedButNotCorrectable
  }

  it should s"return Mixed when attributes have multiple statuses and some are Reordered" in {
    val statusMap = Map(
      AttributeName.withDefaultNS("foo") -> AttributeCorrectionStatus.Correct,
      AttributeName.withLibraryNS("bar") -> AttributeCorrectionStatus.Reordered
    )
    val actual = calculateEntityStatus(statusMap)
    actual shouldBe EntityCorrectionStatus.Mixed
  }
}
