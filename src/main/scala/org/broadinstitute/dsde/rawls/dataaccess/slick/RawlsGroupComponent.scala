package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.RawlsGroup

case class RawlsGroupRecord(groupName: String, groupEmail: String)

trait RawlsGroupComponent {
  this: DriverComponent with GroupUsersComponent with GroupSubgroupsComponent =>

  import driver.api._

  class RawlsGroupTable(tag: Tag) extends Table[RawlsGroupRecord](tag, "GROUP") {
    def groupName = column[String]("groupName", O.PrimaryKey)
    def groupEmail = column[String]("groupEmail")

    def * = (groupName, groupEmail) <> (RawlsGroupRecord.tupled, RawlsGroupRecord.unapply)
  }

  object rawlsGroupQuery extends TableQuery(new RawlsGroupTable(_)) {
    def save(group: RawlsGroup): ReadWriteAction[RawlsGroup] = {
      rawlsGroupQuery.insertOrUpdate(RawlsGroupRecord(group.groupName.value, group.groupEmail.value)) andThen {
        DBIO.seq(group.users.map { user =>
          groupUsersQuery += GroupUsersRecord(user.userSubjectId.value, group.groupName.value)
        }.toSeq:_*)
      } andThen {
        DBIO.seq(group.subGroups.map { subGroup =>
          groupSubgroupsQuery += GroupSubgroupsRecord(group.groupName.value, subGroup.groupName.value)
        }.toSeq:_*)
      } map { _ => group }
    }
  }

  def rawlsGroupByNameQuery(name: String) = Compiled {
    rawlsGroupQuery.filter(_.groupName === name)
  }

  def saveRawlsGroup(group: RawlsGroupRecord) = {
    rawlsGroupQuery insertOrUpdate group map { _ => group }
  }

  def loadRawlsGroup(name: String) = rawlsGroupByNameQuery(name).result

  def deleteRawlsGroup(name: String) = rawlsGroupByNameQuery(name).delete
}
