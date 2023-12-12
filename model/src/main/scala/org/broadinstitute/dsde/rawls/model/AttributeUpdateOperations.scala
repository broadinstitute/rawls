package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json._

/**
 * Created by mbemis on 7/23/15.
 */
object AttributeUpdateOperations {
  import spray.json.DefaultJsonProtocol._

  sealed trait AttributeUpdateOperation {
    def name: AttributeName = this match {
      case AddUpdateAttribute(attributeName, _)                  => attributeName
      case RemoveAttribute(attributeName)                        => attributeName
      case AddListMember(attributeListName, _)                   => attributeListName
      case RemoveListMember(attributeListName, _)                => attributeListName
      case CreateAttributeEntityReferenceList(attributeListName) => attributeListName
      case CreateAttributeValueList(attributeListName)           => attributeListName
    }
  }

  case class AddUpdateAttribute(attributeName: AttributeName, addUpdateAttribute: Attribute)
      extends AttributeUpdateOperation
  case class RemoveAttribute(attributeName: AttributeName) extends AttributeUpdateOperation
  case class CreateAttributeEntityReferenceList(attributeListName: AttributeName) extends AttributeUpdateOperation
  case class CreateAttributeValueList(attributeName: AttributeName) extends AttributeUpdateOperation
  case class AddListMember(attributeListName: AttributeName, newMember: Attribute) extends AttributeUpdateOperation
  case class RemoveListMember(attributeListName: AttributeName, removeMember: Attribute)
      extends AttributeUpdateOperation

  private val AddUpdateAttributeFormat = jsonFormat2(AddUpdateAttribute)
  private val RemoveAttributeFormat = jsonFormat1(RemoveAttribute)
  private val CreateAttributeEntityReferenceListFormat = jsonFormat1(CreateAttributeEntityReferenceList)
  private val CreateAttributeValueListFormat = jsonFormat1(CreateAttributeValueList)
  private val AddListMemberFormat = jsonFormat2(AddListMember)
  private val RemoveListMemberFormat = jsonFormat2(RemoveListMember)

  implicit object AttributeUpdateOperationFormat extends RootJsonFormat[AttributeUpdateOperation] {

    override def write(obj: AttributeUpdateOperation): JsValue = {
      val json = obj match {
        case x: AddUpdateAttribute                 => AddUpdateAttributeFormat.write(x)
        case x: RemoveAttribute                    => RemoveAttributeFormat.write(x)
        case x: CreateAttributeEntityReferenceList => CreateAttributeEntityReferenceListFormat.write(x)
        case x: CreateAttributeValueList           => CreateAttributeValueListFormat.write(x)
        case x: AddListMember                      => AddListMemberFormat.write(x)
        case x: RemoveListMember                   => RemoveListMemberFormat.write(x)
      }

      JsObject(json.asJsObject.fields + ("op" -> JsString(obj.getClass.getSimpleName)))
    }

    override def read(json: JsValue): AttributeUpdateOperation = json match {
      case JsObject(fields) =>
        val op = fields.getOrElse("op", throw new DeserializationException("missing op property"))
        op match {
          case JsString("AddUpdateAttribute")                 => AddUpdateAttributeFormat.read(json)
          case JsString("RemoveAttribute")                    => RemoveAttributeFormat.read(json)
          case JsString("CreateAttributeEntityReferenceList") => CreateAttributeEntityReferenceListFormat.read(json)
          case JsString("CreateAttributeValueList")           => CreateAttributeValueListFormat.read(json)
          case JsString("AddListMember")                      => AddListMemberFormat.read(json)
          case JsString("RemoveListMember")                   => RemoveListMemberFormat.read(json)
          case x => throw new DeserializationException("unrecognized op: " + x)
        }

      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  case class EntityUpdateDefinition(
    name: String,
    entityType: String,
    operations: Seq[AttributeUpdateOperation]
  )

  implicit val entityUpdateDefinitionFormat: RootJsonFormat[EntityUpdateDefinition] = jsonFormat3(
    EntityUpdateDefinition
  )
}
