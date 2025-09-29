package org.broadinstitute.dsde.rawls.entities

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.StringValidationUtils
import org.broadinstitute.dsde.rawls.model.{AttributeName, Entity, ErrorReportSource}

object EntityUtils extends StringValidationUtils with LazyLogging {
  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def validateAttrName(attrName: AttributeName, entityType: String): Unit = {
    validateUserDefinedString(attrName.name)
    validateAttributeName(attrName, entityType)
  }

  def validateEntity(entity: Entity): Unit = {
    validateEntityType(entity.entityType)
    validateEntityName(entity.name)
    entity.attributes.keys.foreach(attrName => validateAttrName(attrName, entity.entityType))
  }

}
