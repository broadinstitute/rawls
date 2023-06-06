package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import spray.json._

import java.util.UUID
import scala.util.Try

//Mix in one of these with your AttributeFormat so you can serialize lists
//This also needs mixing in with an AttributeFormat because they're symbiotic
sealed trait AttributeListSerializer {
  def writeListType(obj: Attribute): JsValue

  // distinguish between lists and RawJson types here
  def readComplexType(json: JsValue): Attribute

  def writeAttribute(obj: Attribute): JsValue
  def readAttribute(json: JsValue): Attribute
}

//Serializes attribute lists to e.g. [1,2,3]
//When reading in JSON, assumes that [] is an empty value list, not an empty ref list
trait PlainArrayAttributeListSerializer extends AttributeListSerializer {
  override def writeListType(obj: Attribute): JsValue = obj match {
    // lists
    case AttributeValueEmptyList           => JsArray()
    case AttributeValueList(l)             => JsArray(l.map(writeAttribute): _*)
    case AttributeEntityReferenceEmptyList => JsArray()
    case AttributeEntityReferenceList(l)   => JsArray(l.map(writeAttribute): _*)
    case _                                 => throw new RawlsException("you can't pass a non-list to writeListType")
  }

  override def readComplexType(json: JsValue): Attribute = json match {
    case JsArray(a) =>
      val attrList: Seq[Attribute] = a.map(readAttribute)
      attrList match {
        case e: Seq[_] if e.isEmpty => AttributeValueEmptyList
        case v: Seq[AttributeValue @unchecked] if attrList.map(_.isInstanceOf[AttributeValue]).reduce(_ && _) =>
          AttributeValueList(v)
        case r: Seq[AttributeEntityReference @unchecked]
            if attrList.map(_.isInstanceOf[AttributeEntityReference]).reduce(_ && _) =>
          AttributeEntityReferenceList(r)
        case _ => AttributeValueRawJson(json) // heterogeneous array type? ok, we'll treat it as raw json
      }
    case _ => AttributeValueRawJson(json) // something else? ok, we'll treat it as raw json
  }
}

//Serializes attribute lists to e.g. { "itemsType" : "AttributeValue", "items" : [1,2,3] }
trait TypedAttributeListSerializer extends AttributeListSerializer {
  val LIST_ITEMS_TYPE_KEY = "itemsType"
  val LIST_ITEMS_KEY = "items"
  val LIST_OBJECT_KEYS = Set(LIST_ITEMS_TYPE_KEY, LIST_ITEMS_KEY)

  val VALUE_LIST_TYPE = "AttributeValue"
  val REF_LIST_TYPE = "EntityReference"
  val ALLOWED_LIST_TYPES = Seq(VALUE_LIST_TYPE, REF_LIST_TYPE)

  def writeListType(obj: Attribute): JsValue = obj match {
    // lists
    case AttributeValueEmptyList           => writeAttributeList(VALUE_LIST_TYPE, Seq.empty[AttributeValue])
    case AttributeValueList(l)             => writeAttributeList(VALUE_LIST_TYPE, l)
    case AttributeEntityReferenceEmptyList => writeAttributeList(REF_LIST_TYPE, Seq.empty[AttributeEntityReference])
    case AttributeEntityReferenceList(l)   => writeAttributeList(REF_LIST_TYPE, l)
    case _                                 => throw new RawlsException("you can't pass a non-list to writeListType")
  }

  def readComplexType(json: JsValue): Attribute = json match {
    case JsObject(members) if LIST_OBJECT_KEYS subsetOf members.keySet => readAttributeList(members)

    // in this serializer, [1,2,3] is not the representation for an AttributeValueList, so it's raw json
    case _ => AttributeValueRawJson(json)
  }

  def writeAttributeList[T <: Attribute](listType: String, list: Seq[T]): JsValue =
    JsObject(
      Map(LIST_ITEMS_TYPE_KEY -> JsString(listType), LIST_ITEMS_KEY -> JsArray(list.map(writeAttribute).toSeq: _*))
    )

  def readAttributeList(jsMap: Map[String, JsValue]) = {
    val attrList: Seq[Attribute] = jsMap(LIST_ITEMS_KEY) match {
      case JsArray(elems) => elems.map(readAttribute)
      case _ => throw new DeserializationException(s"the value of %s should be an array".format(LIST_ITEMS_KEY))
    }

    (jsMap(LIST_ITEMS_TYPE_KEY), attrList) match {
      case (JsString(VALUE_LIST_TYPE), vals: Seq[AttributeValue @unchecked]) if attrList.isEmpty =>
        AttributeValueEmptyList
      case (JsString(VALUE_LIST_TYPE), vals: Seq[AttributeValue @unchecked])
          if attrList.map(_.isInstanceOf[AttributeValue]).reduce(_ && _) =>
        AttributeValueList(vals)

      case (JsString(REF_LIST_TYPE), refs: Seq[AttributeEntityReference @unchecked]) if attrList.isEmpty =>
        AttributeEntityReferenceEmptyList
      case (JsString(REF_LIST_TYPE), refs: Seq[AttributeEntityReference @unchecked])
          if attrList.map(_.isInstanceOf[AttributeEntityReference]).reduce(_ && _) =>
        AttributeEntityReferenceList(refs)

      case (JsString(s), _) if !ALLOWED_LIST_TYPES.contains(s) =>
        throw new DeserializationException(
          s"illegal array type: $LIST_ITEMS_TYPE_KEY must be one of ${ALLOWED_LIST_TYPES.mkString(", ")}"
        )
      case _ => throw new DeserializationException("illegal array type: array elements don't match array type")
    }
  }
}

object AttributeFormat {
  // Magic strings we use in JSON serialization

  // Entity refs get serialized to e.g. { "entityType" : "sample", "entityName" : "theBestSample" }
  val ENTITY_TYPE_KEY = "entityType"
  val ENTITY_NAME_KEY = "entityName"
  val ENTITY_OBJECT_KEYS = Set(ENTITY_TYPE_KEY, ENTITY_NAME_KEY)
}

trait AttributeFormat extends RootJsonFormat[Attribute] with AttributeListSerializer {
  import AttributeFormat._

  override def write(obj: Attribute): JsValue = writeAttribute(obj)
  def writeAttribute(obj: Attribute): JsValue = obj match {
    // vals
    case AttributeNull            => JsNull
    case AttributeBoolean(b)      => JsBoolean(b)
    case AttributeNumber(n)       => JsNumber(n)
    case AttributeString(s)       => JsString(s)
    case AttributeValueRawJson(j) => j
    // ref
    case AttributeEntityReference(entityType, entityName) =>
      JsObject(Map(ENTITY_TYPE_KEY -> JsString(entityType), ENTITY_NAME_KEY -> JsString(entityName)))
    // list types
    case x: AttributeList[_] => writeListType(x)

    case _ =>
      throw new SerializationException(
        "AttributeFormat doesn't know how to write JSON for type " + obj.getClass.getSimpleName
      )
  }

  override def read(json: JsValue): Attribute = readAttribute(json)
  def readAttribute(json: JsValue): Attribute = json match {
    case JsNull       => AttributeNull
    case JsString(s)  => AttributeString(s)
    case JsBoolean(b) => AttributeBoolean(b)
    case JsNumber(n)  => AttributeNumber(n)
    // NOTE: we handle AttributeValueRawJson in readComplexType below

    case JsObject(members) if ENTITY_OBJECT_KEYS subsetOf members.keySet =>
      (members(ENTITY_TYPE_KEY), members(ENTITY_NAME_KEY)) match {
        case (JsString(typeKey), JsString(nameKey)) => AttributeEntityReference(typeKey, nameKey)
        case _ => throw new RawlsException(s"the values for $ENTITY_TYPE_KEY and $ENTITY_NAME_KEY must be strings")
      }

    case _ => readComplexType(json)
  }
}

/**
 * NOTE: When subclassing this, you may want to define your own implicit val attributeFormat with an AttributeListSerializer mixin
 * if you want e.g. the plain old JSON array attribute list serialization strategy
 */
class JsonSupport {
  import AttributeFormat._

  implicit val attributeFormat: AttributeFormat = new AttributeFormat with TypedAttributeListSerializer

  implicit object AttributeStringFormat extends RootJsonFormat[AttributeString] {
    override def write(obj: AttributeString): JsValue = attributeFormat.write(obj)
    override def read(json: JsValue): AttributeString = json match {
      case JsString(s) => AttributeString(s)
      case _           => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object AttributeReferenceFormat extends RootJsonFormat[AttributeEntityReference] {
    override def write(obj: AttributeEntityReference): JsValue = attributeFormat.write(obj)
    override def read(json: JsValue): AttributeEntityReference = json match {
      case JsObject(members) =>
        AttributeEntityReference(members(ENTITY_TYPE_KEY).asInstanceOf[JsString].value,
                                 members(ENTITY_NAME_KEY).asInstanceOf[JsString].value
        )
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object DateJsonFormat extends RootJsonFormat[DateTime] {
    private val parserISO: DateTimeFormatter =
      ISODateTimeFormat.dateTime

    override def write(obj: DateTime) =
      JsString(parserISO.print(obj))

    override def read(json: JsValue): DateTime = json match {
      case JsString(s) => parserISO.parseDateTime(s)
      case _           => throw new DeserializationException("only string supported")
    }
  }

  implicit object SeqAttributeFormat extends RootJsonFormat[Seq[AttributeValue]] {
    override def write(obj: Seq[AttributeValue]) =
      JsArray(obj.map(attributeFormat.write).toVector)

    override def read(json: JsValue): Seq[AttributeValue] =
      attributeFormat.read(json) match {
        case AttributeValueEmptyList => Seq.empty
        case AttributeValueList(l)   => l
        case _                       => throw new DeserializationException("unexpected json type")
      }
  }

  def rawlsEnumerationFormat[T <: RawlsEnumeration[T]](construct: String => T): RootJsonFormat[T] =
    new RootJsonFormat[T] {
      override def write(obj: T): JsValue = JsString(obj.toString)
      override def read(json: JsValue): T = json match {
        case JsString(name) => construct(name)
        case x              => throw new DeserializationException("invalid value: " + x)
      }
    }

  implicit object UUIDFormat extends JsonFormat[UUID] {
    override def write(uuid: UUID) = JsString(uuid.toString)
    override def read(json: JsValue): UUID = json match {
      case JsString(str) =>
        Try(UUID.fromString(str))
          .getOrElse(throw DeserializationException(s"Couldn't parse ${str.take(8)}... into a UUID"))
      case x => throw DeserializationException(s"UUID can only be parsed from a string, got ${x.getClass.getName}")
    }
  }
}
