package org.broadinstitute.dsde.rawls.model

import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter}
import spray.json._
import org.joda.time.DateTime

object AgoraEntityType extends Enumeration {
  type EntityType = Value
  val Task = Value("Task")
  val Workflow = Value("Workflow")
  val Configuration = Value("Configuration")
}

case class AgoraEntity(
                        namespace: Option[String] = None,
                        name: Option[String] = None,
                        snapshotId: Option[Int] = None,
                        synopsis: Option[String] = None,
                        documentation: Option[String] = None,
                        owner: Option[String] = None,
                        createDate: Option[DateTime] = None,
                        payload: Option[String] = None,
                        url: Option[String] = None,
                        entityType: Option[AgoraEntityType.EntityType] = None)

case class AgoraStatus(
  up: Boolean,
  messages: Seq[String]
)

class MethodRepoJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  // need to override the default date time format, because Agora uses dateTimeNoMillis instead of dateTime
  implicit object AgoraDateJsonFormat extends RootJsonFormat[DateTime] {
    private val parserISO : DateTimeFormatter = {
      ISODateTimeFormat.dateTimeNoMillis
    }

    override def write(obj: DateTime) = {
      JsString(parserISO.print(obj))
    }

    override def read(json: JsValue): DateTime = json match {
      case JsString(s) => parserISO.parseDateTime(s)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object AgoraEntityTypeFormat extends RootJsonFormat[AgoraEntityType.EntityType] {
    override def write(obj: AgoraEntityType.EntityType): JsValue = JsString(obj.toString)

    override def read(value: JsValue): AgoraEntityType.EntityType = value match {
      case JsString(name) => AgoraEntityType.withName(name)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object AgoraStatusFormat extends RootJsonFormat[AgoraStatus] {
    override def write(obj: AgoraStatus): JsObject = {
      JsObject("status" -> JsString(if (obj.up) "up" else "down"),
        "message" -> JsArray(obj.messages.map(JsString.apply).toVector)
      )
    }
    override def read(json: JsValue): AgoraStatus = json.asJsObject.getFields("status", "message") match {
      case Seq(JsString(status), m @ JsArray(_)) =>
        AgoraStatus(status == "up", m.convertTo[Seq[String]])
      case _ => throw new DeserializationException("Cannot read AgoraStatus in JSON")
    }
  }

  implicit val AgoraEntityFormat = jsonFormat10(AgoraEntity)

}

object MethodRepoJsonSupport extends MethodRepoJsonSupport
