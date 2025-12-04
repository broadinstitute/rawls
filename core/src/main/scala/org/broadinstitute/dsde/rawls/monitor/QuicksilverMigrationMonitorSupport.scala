package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.slick.EntityCorrection
import org.broadinstitute.dsde.rawls.model.{
  Attribute,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeValue,
  AttributeValueList,
  Entity
}
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatus.AttributeCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatus.EntityCorrectionStatusType

object AttributeCorrectionStatus extends Enumeration {
  type AttributeCorrectionStatusType = Value
  val Correct, Reordered, StartsWith, Different, TypeDifferent = Value
}

object EntityCorrectionStatus extends Enumeration {
  type EntityCorrectionStatusType = Value
  val CurrentGone, Correct, Correctable, Mixed, StartsWith, Different, TypeDifferent = Value
}

trait QuicksilverMigrationMonitorSupport {

  def compareEntities(current: Option[Entity],
                      correction: EntityCorrection
  ): (EntityCorrectionStatusType, Map[AttributeName, AttributeCorrectionStatusType]) = current match {
    // current entity does not exist
    case None => (EntityCorrectionStatus.CurrentGone, Map())
    // current entity exists; compare its attributes
    case Some(e) =>
      // get the current attribute map
      val currentAttributes = e.attributes
      // loop through the correction's attributes
      val attrComparisons: Map[AttributeName, AttributeCorrectionStatusType] = correction.attributes.flatMap {
        case (attributeName, correctionValue) =>
          // get the corresponding current attribute
          val currentValue = currentAttributes.get(attributeName)
          currentValue match {
            // current attribute does not exist
            case None               => None
            case Some(currentValue) => Some((attributeName, compareAttrs(currentValue, correctionValue)))
          }
      }
      // determine overall entity status from attribute statuses
      val entityStatus = calculateEntityStatus(attrComparisons)
      (entityStatus, attrComparisons)
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

    if (attrStatuses.isEmpty) {
      EntityCorrectionStatus.Correct
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Correct)) {
      EntityCorrectionStatus.Correct
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Reordered)) {
      EntityCorrectionStatus.Correctable
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Different)) {
      EntityCorrectionStatus.Different
    } else if (attrStatuses == Set(AttributeCorrectionStatus.StartsWith)) {
      EntityCorrectionStatus.StartsWith
    } else if (attrStatuses == Set(AttributeCorrectionStatus.TypeDifferent)) {
      EntityCorrectionStatus.TypeDifferent
    } else {
      EntityCorrectionStatus.Mixed
    }

  }

}
