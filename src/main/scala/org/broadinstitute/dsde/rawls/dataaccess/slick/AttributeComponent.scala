package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

/**
 * Created by dvoet on 2/4/16.
 */
case class AttributeRecord(id: Long,
                           name: String,
                           valueString: Option[String],
                           valueNumber: Option[Double],
                           valueDate: Option[Timestamp],
                           valueBoolean: Option[Boolean],
                           listIndex: Option[Int])

trait AttributeComponent {
  this: DriverComponent =>

  import driver.api._

  class AttributeTable(tag: Tag) extends Table[AttributeRecord](tag, "ATTRIBUTE") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def valueString = column[Option[String]]("value_string")
    def valueNumber = column[Option[Double]]("value_number")
    def valueDate = column[Option[Timestamp]]("value_date")
    def valueBoolean = column[Option[Boolean]]("value_boolean")
    def listIndex = column[Option[Int]]("list_index")

    def * = (id, name, valueString, valueNumber, valueDate, valueBoolean, listIndex) <> (AttributeRecord.tupled, AttributeRecord.unapply)
  }

  protected val attributeQuery = TableQuery[AttributeTable]
}
