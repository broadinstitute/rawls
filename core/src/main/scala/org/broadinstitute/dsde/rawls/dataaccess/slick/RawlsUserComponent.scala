package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

case class RawlsUserRecord(userSubjectId: String, userEmail: String)

trait RawlsUserComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsUserTable(tag: Tag) extends Table[RawlsUserRecord](tag, "USER") {
    def userSubjectId = column[String]("SUBJECT_ID", O.PrimaryKey, O.Length(254))
    def userEmail = column[String]("EMAIL", O.Length(254))

    def * = (userSubjectId, userEmail) <> (RawlsUserRecord.tupled, RawlsUserRecord.unapply)

    def uniqueEmail = index("IDX_USER_EMAIL", userEmail, unique = true)
  }

  private type RawlsUserQuery = Query[RawlsUserTable, RawlsUserRecord, Seq]

  object rawlsUserQuery extends TableQuery(new RawlsUserTable(_)) {
//replicate
    def save(rawlsUser: RawlsUser): WriteAction[RawlsUser] = {
      rawlsUserQuery insertOrUpdate marshalRawlsUser(rawlsUser) map { _ => rawlsUser }
    }

    def load(ref: RawlsUserRef): ReadAction[Option[RawlsUser]] = {
      loadCommon(findUserBySubjectId(ref.userSubjectId.value))
    }
//replicate
    def loadAllUsers(): ReadAction[Seq[RawlsUser]] = {
      rawlsUserQuery.result map { _ map unmarshalRawlsUser }
    }
//replicate
    def countUsers(): ReadAction[SingleStatistic] = {
      rawlsUserQuery.countDistinct.result.map(count => SingleStatistic(count))
    }
//replicate
    def loadUserByEmail(userEmail: RawlsUserEmail): ReadAction[Option[RawlsUser]] = {
      loadCommon(findUserByEmail(userEmail.value))
    }

    def loadUserRefsByEmails(userEmails: Seq[RawlsUserEmail]): ReadAction[Map[String, RawlsUserRef]] = {
      rawlsUserQuery.filter(_.userEmail.inSetBind(userEmails.map(_.value))).result.map(_.map(rec => rec.userEmail -> RawlsUserRef(RawlsUserSubjectId(rec.userSubjectId))).toMap)
    }

    private def loadCommon(query: RawlsUserQuery): ReadAction[Option[RawlsUser]] = {
      uniqueResult[RawlsUserRecord](query).map {
        case None => None
        case Some(rec) => Option(unmarshalRawlsUser(rec))
      }
    }
  
    def findUserBySubjectId(subjId: String): RawlsUserQuery = {
      rawlsUserQuery.filter(_.userSubjectId === subjId)
    }
  
    private def findUserByEmail(email: String): RawlsUserQuery = {
      rawlsUserQuery.filter(_.userEmail === email)
    }
  
    private def marshalRawlsUser(user: RawlsUser): RawlsUserRecord = {
      RawlsUserRecord(user.userSubjectId.value, user.userEmail.value)
    }
  
    def unmarshalRawlsUser(record: RawlsUserRecord): RawlsUser = {
      RawlsUser(RawlsUserSubjectId(record.userSubjectId), RawlsUserEmail(record.userEmail))
    }
  }
}
