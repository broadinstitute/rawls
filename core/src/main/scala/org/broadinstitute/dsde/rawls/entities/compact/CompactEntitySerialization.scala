package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityAttributeListSerializer
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._

// allow for static access to methods from classes that don't want to mix in the trait
object CompactEntitySerialization extends CompactEntitySerialization

/**
  * Utilities for working with compact entities, including methods to serialize to/deserialize from the database
  */
trait CompactEntitySerialization {

  // serialization format for translating to/from SQL
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with CompactEntityAttributeListSerializer

  // translate a AttributeMap into a JsValue suitable for persisting into the db
  def toSql(attributes: AttributeMap): JsValue =
    attributes.toJson

  // translate a SQL column value back into an AttributeMap
  def fromSql(attributes: Option[String]): AttributeMap =
    attributes.getOrElse("{}").parseJson.convertTo[AttributeMap]

}
