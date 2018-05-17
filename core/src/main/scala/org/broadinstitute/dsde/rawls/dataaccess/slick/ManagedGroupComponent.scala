package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.jndi.JndiDirectoryDAO
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
    with JndiDirectoryDAO =>

  import driver.api._

  class ManagedGroupTable(tag: Tag) extends Table[ManagedGroupRecord](tag, "MANAGED_GROUP") {
    def membersGroupName = column[String]("MEMBERS_GROUP_NAME", O.Length(254), O.PrimaryKey)
    def adminsGroupName = column[String]("ADMINS_GROUP_NAME", O.Length(254))
    def accessInstructions = column[Option[String]]("ACCESS_INSTRUCTIONS")

    def * = (membersGroupName, adminsGroupName, accessInstructions) <> (ManagedGroupRecord.tupled, ManagedGroupRecord.unapply)
  }

  type ManagedGroupQuery = Query[ManagedGroupTable, ManagedGroupRecord, Seq]

  object managedGroupQuery extends TableQuery(new ManagedGroupTable(_)) {
    def createManagedGroup(managedGroup: ManagedGroupRef): WriteAction[ManagedGroupRef] = {
      (managedGroupQuery += marshalManagedGroup(managedGroup)).map(_ => managedGroup)
    }

    def setManagedGroupAccessInstructions(managedGroupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions): WriteAction[Int] = {
      managedGroupQuery.filter(_.membersGroupName === managedGroupRef.membersGroupName.value).map(_.accessInstructions).update(Option(instructions.instructions))
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

    private def marshalManagedGroup(managedGroup: ManagedGroupRef): ManagedGroupRecord = {
      ManagedGroupRecord(managedGroup.membersGroupName.value, managedGroup.membersGroupName.value)
    }
  }
}
