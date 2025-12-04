package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, EntityCorrection}
import org.broadinstitute.dsde.rawls.model.{
  Attribute,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeValue,
  AttributeValueList
}
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatus.AttributeCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatus.EntityCorrectionStatusType

object AttributeCorrectionStatus extends Enumeration {
  type AttributeCorrectionStatusType = Value
  val CurrentAttrGone, Correct, Reordered, StartsWith, Different, TypeDifferent = Value
}

object EntityCorrectionStatus extends Enumeration {
  type EntityCorrectionStatusType = Value
  val CurrentGone, Correct, Correctable, Mixed, StartsWith, Different = Value
}

object ModifiedStatusType extends Enumeration {
  type ModifiedStatusType = Value
  val Unmodified, Modified = Value
  def fromString(str: String): ModifiedStatusType = str.toLowerCase match {
    case "unmodified" => Unmodified
    case "modified"   => Modified
    case x            => throw new Exception(s"$x is not a ModifiedStatusType")
  }
}

trait QuicksilverMigrationMonitorSupport {

  def compareEntities(current: Option[CompactEntityRecord],
                      correction: EntityCorrection
  ): (EntityCorrectionStatusType, Map[AttributeName, AttributeCorrectionStatusType]) = current match {
    // current entity does not exist
    case None => (EntityCorrectionStatus.CurrentGone, Map())
    // current entity exists; compare its attributes
    case Some(cr) =>
      // get the current attribute map
      val currentAttributes = cr.toEntity.attributes
      // loop through the correction's attributes
      val attrComparisons: Map[AttributeName, AttributeCorrectionStatusType] = correction.attributes.map {
        case (attributeName, correctionValue) =>
          // get the corresponding current attribute
          val currentValue = currentAttributes.get(attributeName)
          currentValue match {
            // current attribute does not exist
            case None               => (attributeName, AttributeCorrectionStatus.CurrentAttrGone)
            case Some(currentValue) => (attributeName, compareAttrs(currentValue, correctionValue))
          }
      }
      // determine overall entity status from attribute statuses
      (EntityCorrectionStatus.CurrentGone, attrComparisons)
  }

  def compareAttrs(current: Attribute, correction: Attribute): AttributeCorrectionStatusType =
    (current, correction) match {
      // current attribute is not the same type as correction attribute
      case _ if current.getClass != correction.getClass =>
        AttributeCorrectionStatus.TypeDifferent
      case (x: AttributeValueList, y: AttributeValueList)                     => compareLists(x, y)
      case (x: AttributeEntityReferenceList, y: AttributeEntityReferenceList) => compareLists(x, y)
      case _                                                                  =>
        throw new Exception(
          s"unexpected classes found! current: ${current.getClass.getName}; correction: ${correction.getClass.getName}"
        )
    }

  def compareLists(current: AttributeValueList, correction: AttributeValueList): AttributeCorrectionStatusType =
    compareLists[AttributeValue](current.list, correction.list)

  def compareLists(current: AttributeEntityReferenceList,
                   correction: AttributeEntityReferenceList
  ): AttributeCorrectionStatusType =
    compareLists[AttributeEntityReference](current.list, correction.list)

  // inspect the elements of the lists
  def compareLists[T](current: Seq[T], correction: Seq[T]): AttributeCorrectionStatusType = {
    val sameSize = current.size == correction.size

    if (sameSize) {
      if (current == correction) {
        // exactly equal; this is correct
        AttributeCorrectionStatus.Correct
      } else {
        // generate a map of AttributeValue -> count, where $count is the number of times that element appears
        // in the list
        val currentCounts = current.groupBy(identity).map { case (attrVal, seq) => (attrVal, seq.size) }
        val correctionCounts = correction.groupBy(identity).map { case (attrVal, seq) => (attrVal, seq.size) }
        // compare the elements and their counts; this is an unordered comparison
        if (currentCounts == correctionCounts) {
          // list has the same elements, and in the same counts, but did not pass the
          // current.list == correction.list comparison; it must have been reordered
          AttributeCorrectionStatus.Reordered
        } else {
          // list has different elements, or some of those elements have different counts
          AttributeCorrectionStatus.Different
        }
      }
    } else {
      if (current.startsWith(correction)) {
        AttributeCorrectionStatus.StartsWith
      } else {
        AttributeCorrectionStatus.Different
      }
    }
  }

  def calculateEntityStatus(
    attributeStatuses: Map[AttributeName, AttributeCorrectionStatusType]
  ): EntityCorrectionStatusType = {
    val attrStatuses = attributeStatuses.values.toSet
    // CurrentAttrGone, Correct, Reordered, StartsWith, Different, TypeDifferent
    // TODO: calculate status!
    EntityCorrectionStatus.Mixed
  }

}
