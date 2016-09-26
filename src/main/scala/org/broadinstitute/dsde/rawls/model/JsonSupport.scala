package org.broadinstitute.dsde.rawls.model

import spray.json._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

/**
 * @author tsharpe
 */
trait JsonSupport extends DefaultJsonProtocol {

  implicit object AttributeFormat extends RootJsonFormat[Attribute] {

    override def write(obj: Attribute): JsValue = obj match {
      case AttributeNull => JsNull
      case AttributeBoolean(b) => JsBoolean(b)
      case AttributeNumber(n) => JsNumber(n)
      case AttributeString(s) => JsString(s)
      case AttributeValueList(l) => JsArray(l.map(write(_)):_*)
      case AttributeEntityReferenceList(l) => JsArray(l.map(write(_)).toSeq:_*)
      case AttributeEntityReference(entityType, entityName) => JsObject(Map("entityType" -> JsString(entityType), "entityName" -> JsString(entityName)))
      case AttributeValueEmptyList => JsArray()
      case AttributeEntityReferenceEmptyList => JsArray()
    }

    override def read(json: JsValue): Attribute = json match {
      case JsNull => AttributeNull
      case JsString(s) => AttributeString(s)
      case JsBoolean(b) => AttributeBoolean(b)
      case JsNumber(n) => AttributeNumber(n)
      case JsArray(a) => getAttributeList(a.map(read(_)))
      case JsObject(members) => AttributeEntityReference(members("entityType").asInstanceOf[JsString].value, members("entityName").asInstanceOf[JsString].value)
      case _ => throw new DeserializationException("unexpected json type")
    }

    def getAttributeList(s: Seq[Attribute]) = s match {
      case e: Seq[_] if e.isEmpty => AttributeValueEmptyList
        //TODO: What to do about AttributeEntityReferenceEmptyList?
      case v: Seq[AttributeValue @unchecked] if (s.map(_.isInstanceOf[AttributeValue]).reduce(_&&_)) => AttributeValueList(v)
      case r: Seq[AttributeEntityReference @unchecked] if (s.map(_.isInstanceOf[AttributeEntityReference]).reduce(_&&_)) => AttributeEntityReferenceList(r)
      case _ => throw new DeserializationException("illegal array type")
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
      case JsObject(members) => AttributeEntityReference(members("entityType").asInstanceOf[JsString].value, members("entityName").asInstanceOf[JsString].value)
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

    override def read(json: JsValue): Seq[AttributeValue] = json match {
      case JsArray(a) if a.map(_.isInstanceOf[AttributeValue]).reduce(_&&_) => a.map(AttributeFormat.read(_).asInstanceOf[AttributeValue]).toSeq
      case _ => throw new DeserializationException("unexpected json type")
    }
  }
}
