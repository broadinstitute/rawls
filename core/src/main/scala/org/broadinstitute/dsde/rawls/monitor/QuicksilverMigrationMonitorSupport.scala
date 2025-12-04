package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  CompactEntityQuery,
  CompactEntityRecord,
  EntityCorrection,
  ReadAction
}
import org.broadinstitute.dsde.rawls.model.{
  Attribute,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeValueList,
  Entity
}
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatusType._
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatusType._
import slick.jdbc.MySQLProfile.api._

import java.util.UUID
import scala.collection.Set

object AttributeCorrectionStatusType extends Enumeration {
  type AttributeCorrectionStatusType = Value
  val CurrentAttrGone, Correct, Reordered, StartsWith, Different, TypeDifferent = Value
}

object EntityCorrectionStatusType extends Enumeration {
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
    case None => (CurrentGone, Map())
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
            case None               => (attributeName, CurrentAttrGone)
            case Some(currentValue) => (attributeName, compareAttrs(currentValue, correctionValue))
          }
      }
      // determine overall entity status from attribute statuses
      (CurrentGone, attrComparisons)
  }

  def compareAttrs(current: Attribute, correction: Attribute): AttributeCorrectionStatusType =
    (current, correction) match {
      // current attribute is not the same type as correction attribute
      case _ if current.getClass != correction.getClass =>
        TypeDifferent
      case (x: AttributeValueList, y: AttributeValueList)                     => compareLists(x, y)
      case (x: AttributeEntityReferenceList, y: AttributeEntityReferenceList) => compareLists(x, y)
      case _                                                                  =>
        throw new Exception(
          s"unexpected classes found! current: ${current.getClass.getName}; correction: ${correction.getClass.getName}"
        )
    }

  def compareLists(current: AttributeValueList, correction: AttributeValueList): AttributeCorrectionStatusType = ???
  // TODO: compare!

  def compareLists(current: AttributeEntityReferenceList,
                   correction: AttributeEntityReferenceList
  ): AttributeCorrectionStatusType =
    ???
  // TODO: compare!

  def calculateEntityStatus(
    attributeStatuses: Map[AttributeName, AttributeCorrectionStatusType]
  ): EntityCorrectionStatusType = {
    val attrStatuses = attributeStatuses.values.toSet
    // CurrentAttrGone, Correct, Reordered, StartsWith, Different, TypeDifferent
    // TODO: calculate status!
    Mixed
  }

}
