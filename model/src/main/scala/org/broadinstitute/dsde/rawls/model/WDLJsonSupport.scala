package org.broadinstitute.dsde.rawls.model

import spray.json.{DeserializationException, JsArray, JsValue}

object WDLJsonSupport extends JsonSupport {
  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer
}
