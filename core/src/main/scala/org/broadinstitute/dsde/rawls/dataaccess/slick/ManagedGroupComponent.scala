package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

/**
 * Managed groups are groups that can be managed by users. They consist of 2 rawls groups: one containing everyone who
 * is considered members of the group and another containing everyone who can administer the membership
 *
 * Created by dvoet on 3/8/17.
 */
case class ManagedGroupRecord(membersGroupName: String, adminsGroupName: String, accessInstructions: Option[String] = None)

trait ManagedGroupComponent {
  this: DriverComponent
    with RawlsGroupComponent =>

  import driver.api._

  class ManagedGroupTable(tag: Tag) extends Table[ManagedGroupRecord](tag, "MANAGED_GROUP") {
    def membersGroupName = column[String]("MEMBERS_GROUP_NAME", O.Length(254), O.PrimaryKey)
    def adminsGroupName = column[String]("ADMINS_GROUP_NAME", O.Length(254))
    def accessInstructions = column[Option[String]]("ACCESS_INSTRUCTIONS")

    def * = (membersGroupName, adminsGroupName, accessInstructions) <> (ManagedGroupRecord.tupled, ManagedGroupRecord.unapply)

    def membersGroup = foreignKey("FK_MANAGED_GROUP_MEMBERS", membersGroupName, rawlsGroupQuery)(_.groupName)
    def adminsGroup = foreignKey("FK_MANAGED_GROUP_ADMINS", adminsGroupName, rawlsGroupQuery)(_.groupName)
  }

  type ManagedGroupQuery = Query[ManagedGroupTable, ManagedGroupRecord, Seq]

  object managedGroupQuery extends TableQuery(new ManagedGroupTable(_)) {
    def createManagedGroup(managedGroup: ManagedGroup): WriteAction[ManagedGroup] = {
      (managedGroupQuery += marshalManagedGroup(managedGroup)).map(_ => managedGroup)
    }

    def setManagedGroupAccessInstructions(managedGroupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions): WriteAction[Int] = {
      managedGroupQuery.filter(_.membersGroupName === managedGroupRef.membersGroupName.value).map(_.accessInstructions).update(Option(instructions.instructions))
    }

    def load(managedGroupRef: ManagedGroupRef): ReadAction[Option[ManagedGroup]] = {
      uniqueResult[ManagedGroupRecord](findManagedGroup(managedGroupRef)).flatMap {
        case Some(mg) =>
          for {
            membersGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(mg.membersGroupName)))
            adminsGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(mg.adminsGroupName)))
          } yield {
            Option(ManagedGroup(
              membersGroup.getOrElse(throw new RawlsException(s"members group ${mg.membersGroupName} does not exist")),
              adminsGroup.getOrElse(throw new RawlsException(s"admins group ${mg.adminsGroupName} does not exist"))
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
          groupForUser <- groupsForUser if Seq(managedGroupRecord.adminsGroupName, managedGroupRecord.membersGroupName).contains(groupForUser.groupName.value)
        } yield {
          val role = groupForUser match {
            case RawlsGroupRef(RawlsGroupName(name)) if name == managedGroupRecord.adminsGroupName => ManagedRoles.Admin
            case RawlsGroupRef(RawlsGroupName(name)) if name == managedGroupRecord.membersGroupName => ManagedRoles.Member
            case _ => throw new RawlsException("this should not have happened") // the guard in the for statement prevents this
          }
          ManagedGroupAccess(unmarshalManagedGroupRef(managedGroupRecord), role)
        }
      }
    }

    def getManagedGroupAccessInstructions(groupList: Set[ManagedGroupRef]): ReadAction[Seq[ManagedGroupAccessInstructions]] = {
      val managedGroupsQuery: ReadAction[Seq[ManagedGroupRecord]] = managedGroupQuery.filter(_.membersGroupName inSetBind groupList.map(_.membersGroupName.value)).result

      managedGroupsQuery.map { managedGroups =>
        for {
          managedGroup <- managedGroups
          instructions <- managedGroup.accessInstructions
        } yield ManagedGroupAccessInstructions(managedGroup.membersGroupName, instructions)
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
      managedGroupQuery.filter(_.membersGroupName === managedGroupRef.membersGroupName.value)
    }

    private def unmarshalManagedGroupRef(managedGroup: ManagedGroupRecord): ManagedGroupRef = {
      ManagedGroupRef(RawlsGroupName(managedGroup.membersGroupName))
    }
  }
}
