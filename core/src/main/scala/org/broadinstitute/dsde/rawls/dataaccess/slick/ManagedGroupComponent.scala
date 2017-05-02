package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
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

  class ManagedGroupTable(tag: Tag) extends Table[ManagedGroupRecord](tag, "MANAGED_GROUP") {
    def usersGroupName = column[String]("USERS_GROUP_NAME", O.Length(254), O.PrimaryKey)
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
      uniqueResult[ManagedGroupRecord](findManagedGroup(managedGroupRef)).flatMap {
        case Some(mg) =>
          for {
            usersGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(mg.usersGroupName)))
            ownersGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(mg.ownersGroupName)))
          } yield {
            Option(ManagedGroup(
              usersGroup.getOrElse(throw new RawlsException(s"users group ${mg.usersGroupName} does not exist")),
              ownersGroup.getOrElse(throw new RawlsException(s"owners group ${mg.ownersGroupName} does not exist"))
            ))
          }
        case None => DBIO.successful(None)
      }
    }

    def listManagedGroupsForUser(userRef: RawlsUserRef): ReadAction[Set[ManagedGroupAccess]] = {
      for {
        allManagedGroupRecs <- listAllManagedGroups()
        groupsForUser <- rawlsGroupQuery.listGroupsForUser(userRef)
      } yield {
        for {
          managedGroupRecord <- allManagedGroupRecs
          groupForUser <- groupsForUser if Seq(managedGroupRecord.ownersGroupName, managedGroupRecord.usersGroupName).contains(groupForUser.groupName.value)
        } yield {
          val role = groupForUser match {
            case RawlsGroupRef(RawlsGroupName(name)) if name == managedGroupRecord.ownersGroupName => ManagedRoles.Administrator
            case RawlsGroupRef(RawlsGroupName(name)) if name == managedGroupRecord.usersGroupName => ManagedRoles.Member
            case _ => throw new RawlsException("this should not have happened") // the guard in the for statement prevents this
          }
          ManagedGroupAccess(unmarshalManagedGroupRef(managedGroupRecord), role)
        }
      }
    }

    private def listAllManagedGroups(): ReadAction[Set[ManagedGroupRecord]] = {
      managedGroupQuery.result.map(_.toSet)
    }

    def deleteManagedGroup(managedGroupRef: ManagedGroupRef): ReadWriteAction[Int] = {
      findManagedGroup(managedGroupRef).delete
    }

    private def marshalManagedGroup(managedGroup: ManagedGroup): ManagedGroupRecord = {
      ManagedGroupRecord(managedGroup.membersGroup.groupName.value, managedGroup.adminsGroup.groupName.value)
    }

    private def findManagedGroup(managedGroupRef: ManagedGroupRef): ManagedGroupQuery = {
      managedGroupQuery.filter(_.usersGroupName === managedGroupRef.usersGroupName.value)
    }

    private def unmarshalManagedGroupRef(managedGroup: ManagedGroupRecord): ManagedGroupRef = {
      ManagedGroupRef(RawlsGroupName(managedGroup.usersGroupName))
    }
  }
}
