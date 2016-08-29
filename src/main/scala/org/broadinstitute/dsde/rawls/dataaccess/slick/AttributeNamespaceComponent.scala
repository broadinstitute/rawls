package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException

case class AttributeNamespaceRecord(id: Int, name: String)

trait AttributeNamespaceComponent {
  this: DriverComponent =>

  import driver.api._

  class AttributeNamespaceTable(tag: Tag) extends Table[AttributeNamespaceRecord](tag, "ATTRIBUTE_NAMESPACE") {
    def id = column[Int]("ID", O.PrimaryKey)
    def name = column[String]("NAME", O.Length(254))

    def * = (id, name) <> (AttributeNamespaceRecord.tupled, AttributeNamespaceRecord.unapply)
  }

  object attributeNamespaceQuery extends TableQuery(new AttributeNamespaceTable(_)) {
    def getId(namespace: String): ReadAction[Int] = {
      uniqueResult[AttributeNamespaceRecord](attributeNamespaceQuery.filter(_.name === namespace)).map {
        case None => throw new RawlsException(s"Attribute namespace $namespace not found")
        case Some(rec) => rec.id
      }
    }

    def getMap: ReadAction[Map[String, Int]] = {
      attributeNamespaceQuery.result.map { _.map { r => r.name -> r.id }.toMap }
    }
                               /*
    def save(a: AttributeNamespaceTable): WriteAction[RawlsUser] = {
      rawlsUserQuery insertOrUpdate marshalRawlsUser(rawlsUser) map { _ => rawlsUser }
    }

    def load(ref: RawlsUserRef): ReadAction[Option[RawlsUser]] = {
      loadCommon(findUserBySubjectId(ref.userSubjectId.value))
    }

    def loadAllUsers(): ReadAction[Seq[RawlsUser]] = {
      rawlsUserQuery.result map { _ map unmarshalRawlsUser }
    }

    def countUsers(): ReadAction[SingleStatistic] = {
      rawlsUserQuery.countDistinct.result.map(count => SingleStatistic(count))
    }

    def loadUserByEmail(userEmail: RawlsUserEmail): ReadAction[Option[RawlsUser]] = {
      loadCommon(findUserByEmail(userEmail.value))
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
    }                 */
  }
}
