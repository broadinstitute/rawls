package org.broadinstitute.dsde.rawls.model

object WDLJsonSupport extends JsonSupport {
  implicit override val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer
}
