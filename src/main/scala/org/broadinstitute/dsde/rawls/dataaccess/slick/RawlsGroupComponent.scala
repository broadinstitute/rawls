package org.broadinstitute.dsde.rawls.dataaccess.slick

case class RawlsGroupRecord(groupName: String, groupEmail: String)

trait RawlsGroupComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsGroupTable(tag: Tag) extends Table[RawlsGroupRecord](tag, "GROUP") {
    def groupName = column[String]("groupName", O.PrimaryKey)
    def groupEmail = column[String]("groupEmail")

    def * = (groupName, groupEmail) <> (RawlsGroupRecord.tupled, RawlsGroupRecord.unapply)
  }

  val rawlsGroupQuery = TableQuery[RawlsGroupTable]

  def rawlsGroupByNameQuery(name: String) = Compiled {
    rawlsGroupQuery.filter(_.groupName === name)
  }

  def saveRawlsGroup(group: RawlsGroupRecord) = {
    rawlsGroupQuery insertOrUpdate group map { _ => group }
  }

  def loadRawlsGroup(name: String) = rawlsGroupByNameQuery(name).result

  def deleteRawlsGroup(name: String) = rawlsGroupByNameQuery(name).delete
}
