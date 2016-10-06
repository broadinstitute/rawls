package org.broadinstitute.dsde.rawls.model

import spray.json.{JsArray, _}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

//Mix in one of these with your AttributeFormat so you can serialize lists
//This also needs mixing in with an AttributeFormat because they're symbiotic
sealed trait AttributeListSerializer {
  def writeListType(obj: Attribute): JsValue
  def readListType(json: JsValue): Attribute

  def writeAttribute(obj: Attribute): JsValue
  def readAttribute(json: JsValue): Attribute
}

//Serializes attribute lists to e.g. [1,2,3]
trait PlainArrayAttributeListSerializer extends AttributeListSerializer {
  override def writeListType(obj: Attribute): JsValue = obj match {
    //lists
    case AttributeValueEmptyList => JsArray()
    case AttributeValueList(l) => JsArray(l.map(writeAttribute):_*)
    case AttributeEntityReferenceEmptyList => JsArray()
    case AttributeEntityReferenceList(l) => JsArray(l.map(writeAttribute):_*)
  }

  override def readListType(json: JsValue): Attribute = json match {
    case JsArray(a) =>
      val attrList: Seq[Attribute] = a.map(readAttribute)
      attrList match {
        case e: Seq[_] if e.isEmpty => AttributeValueEmptyList
        case v: Seq[AttributeValue @unchecked] if attrList.map(_.isInstanceOf[AttributeValue]).reduce(_&&_) => AttributeValueList(v)
        case r: Seq[AttributeEntityReference @unchecked] if attrList.map(_.isInstanceOf[AttributeEntityReference]).reduce(_&&_) => AttributeEntityReferenceList(r)
        case _ => throw new DeserializationException("illegal array type")
      }
    case _ => throw new DeserializationException("unexpected json type")
  }
}

//Serializes attribute lists to e.g. { "itemsType" : "AttributeValue", "items" : [1,2,3] }
trait TypedAttributeListSerializer extends AttributeListSerializer {
  val LIST_ITEMS_TYPE_KEY = "itemsType"
  val LIST_ITEMS_KEY = "items"
  val LIST_OBJECT_KEYS = Set(LIST_ITEMS_TYPE_KEY, LIST_ITEMS_KEY)

  val VALUE_LIST_TYPE = "AttributeValue"
  val REF_LIST_TYPE = "EntityReference"

  def writeListType(obj: Attribute): JsValue = obj match {
    //lists
    case AttributeValueEmptyList => writeAttributeList(VALUE_LIST_TYPE, Seq.empty[AttributeValue])
    case AttributeValueList(l) => writeAttributeList(VALUE_LIST_TYPE, l)
    case AttributeEntityReferenceEmptyList => writeAttributeList(REF_LIST_TYPE, Seq.empty[AttributeEntityReference])
    case AttributeEntityReferenceList(l) => writeAttributeList(REF_LIST_TYPE, l)
  }

  def readListType(json: JsValue): Attribute = json match {
    case JsObject(members) if LIST_OBJECT_KEYS subsetOf members.keySet => readAttributeList(members)

    case _ => throw new DeserializationException("unexpected json type")
  }

  def writeAttributeList[T <: Attribute](listType: String, list: Seq[T]): JsValue = {
    JsObject( Map(LIST_ITEMS_TYPE_KEY -> JsString(listType), LIST_ITEMS_KEY -> JsArray(list.map( writeAttribute ).toSeq:_*)) )
  }

  def readAttributeList(jsMap: Map[String, JsValue]) = {
    val attrList: Seq[Attribute] = jsMap(LIST_ITEMS_KEY) match {
      case JsArray(elems) => elems.map(readAttribute)
      case _ => throw new DeserializationException(s"the value of %s should be an array".format(LIST_ITEMS_KEY))
    }

    (jsMap(LIST_ITEMS_TYPE_KEY), attrList) match {
      case (JsString(VALUE_LIST_TYPE), vals: Seq[AttributeValue @unchecked]) if vals.isEmpty => AttributeValueEmptyList
      case (JsString(VALUE_LIST_TYPE), vals: Seq[AttributeValue @unchecked]) if vals.map(_.isInstanceOf[AttributeValue]).reduce(_&&_) => AttributeValueList(vals)

      case (JsString(REF_LIST_TYPE), refs: Seq[AttributeEntityReference @unchecked]) if refs.isEmpty => AttributeEntityReferenceEmptyList
      case (JsString(REF_LIST_TYPE), refs: Seq[AttributeEntityReference @unchecked]) if refs.map(_.isInstanceOf[AttributeEntityReference]).reduce(_&&_) => AttributeEntityReferenceList(refs)

      case _ => throw new DeserializationException("illegal array type")
    }
  }
}

/**
 * NOTE: When subclassing this, you may want to define your own implicit val attributeFormat with an AttributeListSerializer mixin
 * if you want e.g. the plain old JSON array attribute list serialization strategy
 */
class JsonSupport extends DefaultJsonProtocol {
  //Magic strings we use in JSON serialization

  //Entity refs get serialized to e.g. { "entityType" : "sample", "entityName" : "theBestSample" }
  val ENTITY_TYPE_KEY = "entityType"
  val ENTITY_NAME_KEY = "entityName"
  val ENTITY_OBJECT_KEYS = Set(ENTITY_TYPE_KEY, ENTITY_NAME_KEY)

  trait AttributeFormat extends RootJsonFormat[Attribute] with AttributeListSerializer {

    override def write(obj: Attribute): JsValue = writeAttribute(obj)
    def writeAttribute(obj: Attribute): JsValue = obj match {
      //vals
      case AttributeNull => JsNull
      case AttributeBoolean(b) => JsBoolean(b)
      case AttributeNumber(n) => JsNumber(n)
      case AttributeString(s) => JsString(s)
      //ref
      case AttributeEntityReference(entityType, entityName) => JsObject(Map(ENTITY_TYPE_KEY -> JsString(entityType), ENTITY_NAME_KEY -> JsString(entityName)))
      //list types
      case x: AttributeList[_] => writeListType(x)

      case _ => throw new SerializationException("AttributeFormat doesn't know how to write JSON for type " + obj.getClass.getSimpleName)
    }

    override def read(json: JsValue): Attribute = readAttribute(json)
    def readAttribute(json: JsValue): Attribute = json match {
      case JsNull => AttributeNull
      case JsString(s) => AttributeString(s)
      case JsBoolean(b) => AttributeBoolean(b)
      case JsNumber(n) => AttributeNumber(n)

      case JsObject(members) if ENTITY_OBJECT_KEYS subsetOf members.keySet =>
        AttributeEntityReference(members(ENTITY_TYPE_KEY).asInstanceOf[JsString].value, members(ENTITY_NAME_KEY).asInstanceOf[JsString].value)

      case _ => readListType(json)
    }
  }

  implicit val attributeFormat = new AttributeFormat with TypedAttributeListSerializer

  implicit object AttributeStringFormat extends RootJsonFormat[AttributeString] {
    override def write(obj: AttributeString): JsValue = attributeFormat.write(obj)
    override def read(json: JsValue): AttributeString = json match {
      case JsString(s) => AttributeString(s)
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object AttributeReferenceFormat extends RootJsonFormat[AttributeEntityReference] {
    override def write(obj: AttributeEntityReference): JsValue = attributeFormat.write(obj)
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
      JsArray(obj.map( attributeFormat.write ).toVector)
    }

    override def read(json: JsValue): Seq[AttributeValue] = {
      attributeFormat.read(json) match {
        case AttributeValueEmptyList => Seq.empty
        case AttributeValueList(l) => l
        case _ => throw new DeserializationException("unexpected json type")
      }
    }
  }
}
