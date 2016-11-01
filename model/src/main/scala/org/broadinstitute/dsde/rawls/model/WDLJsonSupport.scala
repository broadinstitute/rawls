package org.broadinstitute.dsde.rawls.model

object WDLJsonSupport extends JsonSupport {
  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer
}
