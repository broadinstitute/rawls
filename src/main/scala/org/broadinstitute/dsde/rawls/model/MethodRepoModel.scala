package org.broadinstitute.dsde.rawls.model

import scala.annotation.meta.field
import spray.json._
import com.wordnik.swagger.annotations.{ApiModelProperty, ApiModel}
import org.joda.time.DateTime

object AgoraEntityType extends Enumeration {
  type EntityType = Value
  val Task = Value("Task")
  val Workflow = Value("Workflow")
  val Configuration = Value("Configuration")
}

@ApiModel(value = "AgoraEntity")
case class AgoraEntity(
                        @(ApiModelProperty@field)(required = false, value = "The namespace to which the entity belongs")
                        namespace: Option[String] = None,
                        @(ApiModelProperty@field)(required = false, value = "The entity name ")
                        name: Option[String] = None,
                        @(ApiModelProperty@field)(required = false, value = "The entity snapshot id")
                        snapshotId: Option[Int] = None,
                        @(ApiModelProperty@field)(required = false, value = "A short description of the entity")
                        synopsis: Option[String] = None,
                        @(ApiModelProperty@field)(required = false, value = "Entity documentation")
                        documentation: Option[String] = None,
                        @(ApiModelProperty@field)(required = false, value = "User who owns this entity in the methods repo")
                        owner: Option[String] = None,
                        @(ApiModelProperty@field)(required = false, value = "The date the entity was inserted in the methods repo")
                        createDate: Option[DateTime] = None,
                        @(ApiModelProperty@field)(required = false, value = "The entity payload")
                        payload: Option[String] = None,
                        @(ApiModelProperty@field)(required = false, value = "URI for entity details")
                        url: Option[String] = None,
                        @(ApiModelProperty@field)(required = true, value = "Which agora entity type is this: Task, Workflow, or Configuration")
                        entityType: Option[AgoraEntityType.EntityType] = None)

object MethodRepoJsonSupport extends JsonSupport {

  implicit object AgoraEntityTypeFormat extends RootJsonFormat[AgoraEntityType.EntityType] {
    override def write(obj: AgoraEntityType.EntityType): JsValue = JsString(obj.toString)

    override def read(value: JsValue): AgoraEntityType.EntityType = value match {
      case JsString(name) => AgoraEntityType.withName(name)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit val AgoraEntityFormat = jsonFormat10(AgoraEntity)

}
