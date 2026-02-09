package org.broadinstitute.dsde.rawls.monitor

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.EntityCorrection
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
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
  val Correct, Corrected, Reordered, Intersect, Different, NothingInCommon, NotAList, TypeDifferent = Value
}

object EntityCorrectionStatus extends Enumeration {
  type EntityCorrectionStatusType = Value
  val CurrentGone, Correct, CorrectModified, Correctable, CorrectableModified, Corrected, Mixed, MixedButNotCorrectable,
    MixedModified, Intersect, IntersectModified, Different, DifferentModified, NothingInCommon, NothingInCommonModified,
    NotAList, NotAListModified, TypeDifferent, TypeDifferentModified = Value
}

trait QuicksilverMigrationMonitorSupport extends LazyLogging {

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
            case Some(currentValue) =>
              Some(
                (attributeName,
                 compareAttrs(currentValue,
                              correctionValue,
                              hint =
                                s"${correction.entityType}/${correction.entityName}/${toDelimitedName(attributeName)}"
                 )
                )
              )
          }
      }
      // determine overall entity status from attribute statuses
      val entityStatus = calculateEntityStatus(attrComparisons)
      (entityStatus, attrComparisons)
  }

  def compareAttrs(current: Attribute, correction: Attribute, hint: String): AttributeCorrectionStatusType =
    (current, correction) match {
      // when the correction is a value list
      case (x: AttributeValueList, y: AttributeValueList) => compareLists(x, y)
      case (_, _: AttributeValueList)                     => AttributeCorrectionStatus.TypeDifferent
      // when the correction is an entity-reference list
      case (x: AttributeEntityReferenceList, y: AttributeEntityReferenceList) => compareLists(x, y)
      case (_, _: AttributeEntityReferenceList)                               => AttributeCorrectionStatus.TypeDifferent
      // when the correction is something else, which should never happen but
      // in reality we've seen it does happen
      case _ =>
        logger.warn(
          s"unexpected attribute class found in $hint - current: ${current.getClass.getName}; correction: ${correction.getClass.getName}"
        )
        AttributeCorrectionStatus.NotAList

    }

  def compareLists(current: AttributeValueList, correction: AttributeValueList): AttributeCorrectionStatusType =
    compareLists[AttributeValue](current.list, correction.list)

  def compareLists(current: AttributeEntityReferenceList,
                   correction: AttributeEntityReferenceList
  ): AttributeCorrectionStatusType =
    compareLists[AttributeEntityReference](current.list, correction.list)

  // inspect the elements of the lists
  def compareLists[T](current: Seq[T], correction: Seq[T]): AttributeCorrectionStatusType =

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
        // the elements in the two lists are different. Check the intersection of the two lists
        // to see if the intersection is in the same order; if so, it is because elements have
        // been added/removed without otherwise reordering.
        val intersection = current.toSet intersect correction.toSet

        if (intersection.isEmpty) {
          // no elements in common
          AttributeCorrectionStatus.NothingInCommon
        } else {
          // filter each list to only contain the elements in the intersection
          val currentFiltered = current.filter(x => intersection contains x)
          val correctionFiltered = correction.filter(x => intersection contains x)

          if (currentFiltered == correctionFiltered) {
            // the intersection of
            AttributeCorrectionStatus.Intersect
          } else {
            AttributeCorrectionStatus.Different
          }
        }
      }
    }

  def calculateEntityStatus(
    attributeStatuses: Map[AttributeName, AttributeCorrectionStatusType]
  ): EntityCorrectionStatusType = {
    val attrStatuses = attributeStatuses.values.toSet

    // when no attributes exist, use Correct; there is nothing that could be corrected
    if (attrStatuses.isEmpty) {
      EntityCorrectionStatus.Correct

      // if any attribute was corrected, mark the entity as Corrected
    } else if (attrStatuses.contains(AttributeCorrectionStatus.Corrected)) {
      EntityCorrectionStatus.Corrected

      // handle cases where all attributes have the same status
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Correct)) {
      EntityCorrectionStatus.Correct
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Reordered)) {
      EntityCorrectionStatus.Correctable
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Corrected)) {
      EntityCorrectionStatus.Corrected
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Different)) {
      EntityCorrectionStatus.Different
    } else if (attrStatuses == Set(AttributeCorrectionStatus.Intersect)) {
      EntityCorrectionStatus.Intersect
    } else if (attrStatuses == Set(AttributeCorrectionStatus.NothingInCommon)) {
      EntityCorrectionStatus.NothingInCommon
    } else if (attrStatuses == Set(AttributeCorrectionStatus.NotAList)) {
      EntityCorrectionStatus.NotAList
    } else if (attrStatuses == Set(AttributeCorrectionStatus.TypeDifferent)) {
      EntityCorrectionStatus.TypeDifferent

      // single-status sets were handled above. Now handle cases where attributes
      // have multiple cases.
      //
      // if any attribute is correctable (e.g. "Reordered"), mark the entity as Mixed.
      // Mixed entities will get processed by the correction monitor.
    } else if (attrStatuses.contains(AttributeCorrectionStatus.Reordered)) {
      EntityCorrectionStatus.Mixed

      // We have multiple attribute statuses, but none of them is "Reordered".
      // Mark the entity as MixedButNotCorrectable, which will not be processed by
      // the correction monitor.
    } else {
      EntityCorrectionStatus.MixedButNotCorrectable
    }

  }

}
