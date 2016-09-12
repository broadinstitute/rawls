package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.ErrorReport
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import spray.http.StatusCodes

case class AttributeNamespaceRecord(id: Long, name: String)

trait AttributeNamespaceComponent {
  this: DriverComponent =>

  import driver.api._

  class AttributeNamespaceTable(tag: Tag) extends Table[AttributeNamespaceRecord](tag, "ATTRIBUTE_NAMESPACE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME", O.Length(254))

    def * = (id, name) <> (AttributeNamespaceRecord.tupled, AttributeNamespaceRecord.unapply)
  }

  object attributeNamespaceQuery extends TableQuery(new AttributeNamespaceTable(_)) {
    def invalidNamespace(namespace: String) = throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Attribute namespace $namespace not found"))
    def invalidNamespace(namespaceId: Long) = throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Attribute namespace ID $namespaceId not found"))

    def getId(namespace: String): ReadAction[Long] = {
      uniqueResult[AttributeNamespaceRecord](attributeNamespaceQuery.filter(_.name === namespace)).map {
        case Some(rec) => rec.id
        case None => invalidNamespace(namespace)
      }
    }

    def getMap: ReadAction[Map[String, Long]] = {
      attributeNamespaceQuery.result.map { _.map { r => r.name -> r.id }.toMap }
    }

    def unmarshalNamespace(map: Map[String, Long], namespaceId: Long): String = {
      map.map(_.swap).getOrElse(namespaceId, invalidNamespace(namespaceId))
    }

    def marshalNamespace(map: Map[String, Long], namespace: String): Long = {
      map.getOrElse(namespace, invalidNamespace(namespace))
    }
  }
}
