package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.StringValidationUtils
import org.broadinstitute.dsde.rawls.model.{Entity, ErrorReportSource}

object EntityUtils extends StringValidationUtils {
  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def validateEntity(entity: Entity): Unit = {
    validateEntityType(entity.entityType)
    validateEntityName(entity.name)
    entity.attributes.keys.foreach { attrName =>
      validateUserDefinedString(attrName.name)
      validateAttributeName(attrName, entity.entityType)
    }
  }
}
