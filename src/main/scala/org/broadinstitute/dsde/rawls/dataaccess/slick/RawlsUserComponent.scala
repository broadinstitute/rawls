package org.broadinstitute.dsde.rawls.dataaccess.slick

case class RawlsUserRecord(userSubjectId: String, userEmail: String)

trait RawlsUserComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsUserTable(tag: Tag) extends Table[RawlsUserRecord](tag, "USER") {
    def userSubjectId = column[String]("userSubjectId", O.PrimaryKey)
    def userEmail = column[String]("userEmail")

    def * = (userSubjectId, userEmail) <> (RawlsUserRecord.tupled, RawlsUserRecord.unapply)
  }

  val rawlsUserQuery = TableQuery[RawlsUserTable]

  def rawlsUserByIdQuery(subjId: String) = Compiled {
    rawlsUserQuery.filter(_.userSubjectId === subjId)
  }

  def saveRawlsUser(user: RawlsUserRecord) = {
    rawlsUserQuery insertOrUpdate user map { _ => user }
  }

  def loadRawlsUser(subjId: String) = rawlsUserByIdQuery(subjId).result

  def deleteRawlsUser(subjId: String) = rawlsUserByIdQuery(subjId).delete
}
