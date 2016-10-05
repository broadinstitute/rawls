package org.broadinstitute.dsde.rawls.model

import spray.json.{DeserializationException, JsArray, JsValue}

object WDLJsonSupport extends JsonSupport {

  override def writeListType(obj: Attribute): JsValue = obj match {
    //lists
    case AttributeValueEmptyList => JsArray()
    case AttributeValueList(l) => JsArray(l.map(AttributeFormat.write(_)):_*)
    case AttributeEntityReferenceEmptyList => JsArray()
    case AttributeEntityReferenceList(l) => JsArray(l.map(AttributeFormat.write(_)):_*)
  }

  override def readListType(json: JsValue): Attribute = json match {
    case JsArray(a) =>
      val attrList: Seq[Attribute] = a.map(AttributeFormat.read(_))
      attrList match {
        case e: Seq[_] if e.isEmpty => AttributeValueEmptyList
        case v: Seq[AttributeValue @unchecked] if attrList.map(_.isInstanceOf[AttributeValue]).reduce(_&&_) => AttributeValueList(v)
        case r: Seq[AttributeEntityReference @unchecked] if attrList.map(_.isInstanceOf[AttributeEntityReference]).reduce(_&&_) => AttributeEntityReferenceList(r)
        case _ => throw new DeserializationException("illegal array type")
      }
    case _ => throw new DeserializationException("unexpected json type")
  }
}
