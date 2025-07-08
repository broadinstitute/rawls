package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}

case class DataReferenceName(value: String) extends ValueObject

object DataReferenceModelJsonSupport extends JsonSupport {
  implicit val DataReferenceNameFormat: ValueObjectFormat[DataReferenceName] = ValueObjectFormat(DataReferenceName)
}
