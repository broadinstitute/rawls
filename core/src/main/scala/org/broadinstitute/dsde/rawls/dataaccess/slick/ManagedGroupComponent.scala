package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ManagedGroupRoles.ManagedGroupRole
import org.broadinstitute.dsde.rawls.model._

/**
 * Managed groups are groups that can be managed by users. They consist of 2 rawls groups: one containing everyone who
 * is considered members the group (users) and another containing everyone who can change the membership (owners)
 *
 * Created by dvoet on 3/8/17.
 */
case class ManagedGroupRecord(usersGroupName: String, ownersGroupName: String)

trait ManagedGroupComponent {
  this: DriverComponent
    with RawlsGroupComponent =>

  import driver.api._

  class ManagedGroupTable(tag: Tag) extends Table[ManagedGroupRecord](tag, "REALM") {
    def usersGroupName = column[String]("USERS_GROUP_NAME", O.Length(254))
    def ownersGroupName = column[String]("OWNERS_GROUP_NAME", O.Length(254))

    def * = (usersGroupName, ownersGroupName) <> (ManagedGroupRecord.tupled, ManagedGroupRecord.unapply)

    def usersGroup = foreignKey("FK_MANAGED_GROUP_USERS", usersGroupName, rawlsGroupQuery)(_.groupName)
    def ownersGroup = foreignKey("FK_MANAGED_GROUP_OWNERS", ownersGroupName, rawlsGroupQuery)(_.groupName)
  }

  type ManagedGroupQuery = Query[ManagedGroupTable, ManagedGroupRecord, Seq]

  object managedGroupQuery extends TableQuery(new ManagedGroupTable(_)) {
    def createManagedGroup(managedGroup: ManagedGroup): WriteAction[ManagedGroup] = {
      (managedGroupQuery += marshalManagedGroup(managedGroup)).map(_ => managedGroup)
    }

    def load(managedGroupRef: ManagedGroupRef): ReadAction[Option[ManagedGroup]] = {
      uniqueResult(findManagedGroup(managedGroupRef)).map { x: Option[ManagedGroupRecord] => x.map(unmarshalManagedGroup) }
    }

    def loadFull(managedGroupRef: ManagedGroupRef): ReadAction[Option[ManagedGroupFull]] = {
      load(managedGroupRef).flatMap {
        case Some(mg) =>
          for {
            usersGroup <- rawlsGroupQuery.load(mg.usersGroup)
            ownersGroup <- rawlsGroupQuery.load(mg.ownersGroup)
          } yield {
            Option(ManagedGroupFull(
              usersGroup.getOrElse(throw new RawlsException(s"users group ${mg.usersGroup} does not exist")),
              ownersGroup.getOrElse(throw new RawlsException(s"owners group ${mg.ownersGroup} does not exist"))
            ))
          }
        case None => DBIO.successful(None)
      }
    }

    def listManagedGroupsForUser(userRef: RawlsUserRef): ReadAction[Set[ManagedGroupAccess]] = {
      for {
        allManagedGroups <- listAllManagedGroups()
        groupsForUser <- rawlsGroupQuery.listGroupsForUser(userRef)
      } yield {
        allManagedGroups.collect {
          case managedGroup if groupsForUser.contains(managedGroup.ownersGroup) => ManagedGroupAccess(managedGroup, ManagedGroupRoles.Owner)
          case managedGroup if groupsForUser.contains(managedGroup.usersGroup) => ManagedGroupAccess(managedGroup, ManagedGroupRoles.User)
        }
      }
    }

    def listAllManagedGroups(): ReadAction[Set[ManagedGroup]] = {
      (managedGroupQuery.result.map(recs => recs.map(unmarshalManagedGroup).toSet))
    }

    def deleteManagedGroup(managedGroupRef: ManagedGroupRef): ReadWriteAction[Int] = {
      (findManagedGroup(managedGroupRef)).delete
    }

    private def marshalManagedGroup(managedGroup: ManagedGroup): ManagedGroupRecord = {
      ManagedGroupRecord(managedGroup.usersGroup.groupName.value, managedGroup.ownersGroup.groupName.value)
    }

    private def unmarshalManagedGroup(managedGroupRecord: ManagedGroupRecord): ManagedGroup = {
      ManagedGroup(RawlsGroupRef(RawlsGroupName(managedGroupRecord.usersGroupName)), RawlsGroupRef(RawlsGroupName(managedGroupRecord.ownersGroupName)))
    }

    private def findManagedGroup(managedGroupRef: ManagedGroupRef): ManagedGroupQuery = {
      managedGroupQuery.filter(_.usersGroupName === managedGroupRef.usersGroupName.value)
    }
  }

}
