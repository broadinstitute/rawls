package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException

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
    def getId(namespace: String): ReadAction[Long] = {
      uniqueResult[AttributeNamespaceRecord](attributeNamespaceQuery.filter(_.name === namespace)).map {
        case None => throw new RawlsException(s"Attribute namespace $namespace not found")
        case Some(rec) => rec.id
      }
    }

    def getMap: ReadAction[Map[String, Long]] = {
      attributeNamespaceQuery.result.map { _.map { r => r.name -> r.id }.toMap }
    }
  }
}
