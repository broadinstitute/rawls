package org.broadinstitute.dsde.rawls.model

import spray.json._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

/**
 * @author tsharpe
 */
trait JsonSupport extends DefaultJsonProtocol {
  //Magic strings we use in JSON serialization
  //Lists get serialized to e.g. { "itemsType" : "AttributeValue", "items" : [1,2,3] }
  val LIST_ITEMS_TYPE_KEY = "itemsType"
  val LIST_ITEMS_KEY = "items"
  val LIST_OBJECT_KEYS = Set(LIST_ITEMS_TYPE_KEY, LIST_ITEMS_KEY)

  val VALUE_LIST_TYPE = "AttributeValue"
  val REF_LIST_TYPE = "EntityReference"

  //Entity refs get serialized to e.g. { "entityType" : "sample", "entityName" : "theBestSample" }
  val ENTITY_TYPE_KEY = "entityType"
  val ENTITY_NAME_KEY = "entityName"
  val ENTITY_OBJECT_KEYS = Set(ENTITY_TYPE_KEY, ENTITY_NAME_KEY)

  def writeListType(obj: Attribute): JsValue = obj match {
    //lists
    case AttributeValueEmptyList => AttributeFormat.writeAttributeList(VALUE_LIST_TYPE, Seq.empty[AttributeValue])
    case AttributeValueList(l) => AttributeFormat.writeAttributeList(VALUE_LIST_TYPE, l)
    case AttributeEntityReferenceEmptyList => AttributeFormat.writeAttributeList(REF_LIST_TYPE, Seq.empty[AttributeEntityReference])
    case AttributeEntityReferenceList(l) => AttributeFormat.writeAttributeList(REF_LIST_TYPE, l)
  }

  def readListType(json: JsValue): Attribute = json match {
    case JsObject(members) if LIST_OBJECT_KEYS subsetOf members.keySet => AttributeFormat.readAttributeList(members)

    case _ => throw new DeserializationException("unexpected json type")
  }

  implicit object AttributeFormat extends RootJsonFormat[Attribute] {

    def write(obj: Attribute): JsValue = obj match {
      //vals
      case AttributeNull => JsNull
      case AttributeBoolean(b) => JsBoolean(b)
      case AttributeNumber(n) => JsNumber(n)
      case AttributeString(s) => JsString(s)
      //ref
      case AttributeEntityReference(entityType, entityName) => JsObject(Map(ENTITY_TYPE_KEY -> JsString(entityType), ENTITY_NAME_KEY -> JsString(entityName)))

      case _ => writeListType(obj)
    }

    override def read(json: JsValue): Attribute = json match {
      case JsNull => AttributeNull
      case JsString(s) => AttributeString(s)
      case JsBoolean(b) => AttributeBoolean(b)
      case JsNumber(n) => AttributeNumber(n)

      case JsObject(members) if ENTITY_OBJECT_KEYS subsetOf members.keySet =>
        AttributeEntityReference(members(ENTITY_TYPE_KEY).asInstanceOf[JsString].value, members(ENTITY_NAME_KEY).asInstanceOf[JsString].value)

      case _ => readListType(json)
    }

    def writeAttributeList[T <: Attribute](listType: String, list: Seq[T]): JsValue = {
      JsObject( Map(LIST_ITEMS_TYPE_KEY -> JsString(listType), LIST_ITEMS_KEY -> JsArray(list.map( AttributeFormat.write(_) ).toSeq:_*)) )
    }

    def readAttributeList(jsMap: Map[String, JsValue]) = {
      val attrList: Seq[Attribute] = jsMap(LIST_ITEMS_TYPE_KEY) match {
        case JsArray(elems) => elems.map(AttributeFormat.read(_))
        case _ => throw new DeserializationException(s"the value of %s should be an array".format(LIST_ITEMS_KEY))
      }

      (jsMap(LIST_ITEMS_KEY), attrList) match {
        case (JsString(VALUE_LIST_TYPE), vals: Seq[AttributeValue @unchecked]) if vals.isEmpty => AttributeValueEmptyList
        case (JsString(VALUE_LIST_TYPE), vals: Seq[AttributeValue @unchecked]) if vals.map(_.isInstanceOf[AttributeValue]).reduce(_&&_) => AttributeValueList(vals)

        case (JsString(REF_LIST_TYPE), refs: Seq[AttributeEntityReference @unchecked]) if refs.isEmpty => AttributeEntityReferenceEmptyList
        case (JsString(REF_LIST_TYPE), refs: Seq[AttributeEntityReference @unchecked]) if refs.map(_.isInstanceOf[AttributeEntityReference]).reduce(_&&_) => AttributeEntityReferenceList(refs)

        case _ => throw new DeserializationException("illegal array type")
      }
    }
  }

  implicit object AttributeStringFormat extends RootJsonFormat[AttributeString] {
    override def write(obj: AttributeString): JsValue = AttributeFormat.write(obj)
    override def read(json: JsValue): AttributeString = json match {
      case JsString(s) => AttributeString(s)
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object AttributeReferenceFormat extends RootJsonFormat[AttributeEntityReference] {
    override def write(obj: AttributeEntityReference): JsValue = AttributeFormat.write(obj)
    override def read(json: JsValue): AttributeEntityReference = json match {
      case JsObject(members) => AttributeEntityReference(members(ENTITY_TYPE_KEY).asInstanceOf[JsString].value, members(ENTITY_NAME_KEY).asInstanceOf[JsString].value)
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object DateJsonFormat extends RootJsonFormat[DateTime] {
    private val parserISO : DateTimeFormatter = {
      ISODateTimeFormat.dateTime
    }

    override def write(obj: DateTime) = {
      JsString(parserISO.print(obj))
    }

    override def read(json: JsValue): DateTime = json match {
      case JsString(s) => parserISO.parseDateTime(s)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object SeqAttributeFormat extends RootJsonFormat[Seq[AttributeValue]] {
    override def write(obj: Seq[AttributeValue]) = {
      JsArray(obj.map( AttributeFormat.write ).toVector)
    }

    override def read(json: JsValue): Seq[AttributeValue] = {
      AttributeFormat.read(json) match {
        case AttributeValueEmptyList => Seq.empty
        case AttributeValueList(l) => l
        case _ => throw new DeserializationException("unexpected json type")
      }
    }
  }
}
