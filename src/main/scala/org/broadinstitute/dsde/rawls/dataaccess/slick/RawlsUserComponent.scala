package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{RawlsUserSubjectId, RawlsUserEmail, RawlsUser, RawlsUserRef}

case class RawlsUserRecord(userSubjectId: String, userEmail: String)

trait RawlsUserComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsUserTable(tag: Tag) extends Table[RawlsUserRecord](tag, "USER") {
    def userSubjectId = column[String]("SUBJECT_ID", O.PrimaryKey)
    def userEmail = column[String]("EMAIL")

    def * = (userSubjectId, userEmail) <> (RawlsUserRecord.tupled, RawlsUserRecord.unapply)
  }

  private type RawlsUserQuery = Query[RawlsUserTable, RawlsUserRecord, Seq]

  object rawlsUserQuery extends TableQuery(new RawlsUserTable(_)) {

    def save(rawlsUser: RawlsUser): WriteAction[RawlsUser] = {
      rawlsUserQuery insertOrUpdate marshalRawlsUser(rawlsUser) map { _ => rawlsUser }
    }

    def load(ref: RawlsUserRef): ReadAction[Option[RawlsUser]] = {
      val subjId = ref.userSubjectId.value
      findUserBySubjectId(subjId).result.map {
        case Seq() => None
        case Seq(rec) => Option(unmarshalRawlsUser(rec))
        case _ => throw new RawlsException(s"Primary key violation: found multiple records for user subject ID '$subjId'")
      }
    }

    def loadAllUsers(): ReadAction[Seq[RawlsUser]] = {
      rawlsUserQuery.result map { _ map unmarshalRawlsUser }
    }

    def loadUserByEmail(userEmail: RawlsUserEmail): ReadAction[Option[RawlsUser]] = {
      val email = userEmail.value
      findUserByEmail(email).result.map {
        case Seq() => None
        case Seq(rec) => Option(unmarshalRawlsUser(rec))
        case _ => throw new RawlsException(s"Primary key violation: found multiple records for user email '$email'")
      }
    }
  }

  private def findUserBySubjectId(subjId: String): RawlsUserQuery = {
    rawlsUserQuery.filter(_.userSubjectId === subjId)
  }

  private def findUserByEmail(email: String): RawlsUserQuery = {
    rawlsUserQuery.filter(_.userEmail === email)
  }

  private def marshalRawlsUser(user: RawlsUser): RawlsUserRecord = {
    RawlsUserRecord(user.userSubjectId.value, user.userEmail.value)
  }

  private def unmarshalRawlsUser(record: RawlsUserRecord): RawlsUser = {
    RawlsUser(RawlsUserSubjectId(record.userSubjectId), RawlsUserEmail(record.userEmail))
  }
}
