package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

case class RawlsGroupRecord(groupName: String, groupEmail: String)
case class GroupUsersRecord(userSubjectId: String, groupName: String)
case class GroupSubgroupsRecord(parentGroupName: String, childGroupName: String)

trait RawlsGroupComponent {
  this: DriverComponent
    with RawlsUserComponent =>

  import driver.api._

  class RawlsGroupTable(tag: Tag) extends Table[RawlsGroupRecord](tag, "GROUP") {
    def groupName = column[String]("NAME", O.PrimaryKey, O.Length(254))
    def groupEmail = column[String]("EMAIL", O.Length(254))

    def * = (groupName, groupEmail) <> (RawlsGroupRecord.tupled, RawlsGroupRecord.unapply)

    def uniqueEmail = index("IDX_GROUP_EMAIL", groupEmail, unique = true)
  }

  class GroupUsersTable(tag: Tag) extends Table[GroupUsersRecord](tag, "GROUP_USERS") {
    def userSubjectId = column[String]("USER_SUBJECT_ID", O.Length(254))
    def groupName = column[String]("GROUP_NAME", O.Length(254))

    def * = (userSubjectId, groupName) <>(GroupUsersRecord.tupled, GroupUsersRecord.unapply)

    def user = foreignKey("FK_GROUP_USERS_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def group = foreignKey("FK_GROUP_USERS_GROUP", groupName, rawlsGroupQuery)(_.groupName)
    def pk = primaryKey("PK_GROUP_USERS", (userSubjectId, groupName))
  }

  class GroupSubgroupsTable(tag: Tag) extends Table[GroupSubgroupsRecord](tag, "GROUP_SUBGROUPS") {
    def parentGroupName = column[String]("PARENT_NAME", O.Length(254))
    def childGroupName = column[String]("CHILD_NAME", O.Length(254))

    def * = (parentGroupName, childGroupName) <> (GroupSubgroupsRecord.tupled, GroupSubgroupsRecord.unapply)

    def parentGroup = foreignKey("FK_GROUP_SUBGROUPS_PARENT", parentGroupName, rawlsGroupQuery)(_.groupName)
    def childGroup = foreignKey("FK_GROUP_SUBGROUPS_CHILD", childGroupName, rawlsGroupQuery)(_.groupName)
    def pk = primaryKey("PK_GROUP_SUBGROUPS", (parentGroupName, childGroupName))
  }

  protected val groupUsersQuery = TableQuery[GroupUsersTable]
  protected val groupSubgroupsQuery = TableQuery[GroupSubgroupsTable]
  private type GroupQuery = Query[RawlsGroupTable, RawlsGroupRecord, Seq]
  private type GroupUsersQuery = Query[GroupUsersTable, GroupUsersRecord, Seq]
  private type GroupSubgroupsQuery = Query[GroupSubgroupsTable, GroupSubgroupsRecord, Seq]

  object rawlsGroupQuery extends TableQuery(new RawlsGroupTable(_)) {

    def save(rawlsGroup: RawlsGroup): WriteAction[RawlsGroup] = {
      val groupName = rawlsGroup.groupName.value
      val groupInsert = rawlsGroupQuery insertOrUpdate marshalRawlsGroup(rawlsGroup)

      val userDeletes = findUsersByGroupName(groupName).delete
      val userInserts = rawlsGroup.users.toSeq.map {
        groupUsersQuery insertOrUpdate marshalGroupUsers(_, rawlsGroup)
      }
      val subGroupDeletes = findSubgroupsByGroupName(groupName).delete
      val subgroupInserts = rawlsGroup.subGroups.toSeq.map {
        groupSubgroupsQuery insertOrUpdate marshalGroupSubgroups(_, rawlsGroup)
      }

      val actions = groupInsert andThen userDeletes andThen DBIO.seq(userInserts: _*) andThen subGroupDeletes andThen DBIO.seq(subgroupInserts: _*)
      actions map { _ => rawlsGroup }
    }

    def load(groupRef: RawlsGroupRef): ReadAction[Option[RawlsGroup]] = {
      loadGroup(findGroupByName(groupRef.groupName.value))
    }

    def loadGroupByEmail(groupEmail: RawlsGroupEmail): ReadAction[Option[RawlsGroup]] = {
      loadGroup(findGroupByEmail(groupEmail.value))
    }

    def delete(groupRef: RawlsGroupRef): ReadWriteAction[Boolean] = {
      val name = groupRef.groupName.value
      val groupQuery = findGroupByName(name)

      uniqueResult[RawlsGroupRecord](groupQuery).flatMap {
        case None => DBIO.successful(false)
        case Some(groupRec) =>
          val actions = findUsersByGroupName(name).delete andThen findSubgroupsByGroupName(name).delete andThen groupQuery.delete
          actions map { count => count > 0 }
      }
    }

    def loadFromEmail(email: String): ReadAction[Option[Either[RawlsUser, RawlsGroup]]] = {
      val user: ReadAction[Option[RawlsUser]] = rawlsUserQuery.loadUserByEmail(RawlsUserEmail(email))
      val group: ReadAction[Option[RawlsGroup]] = loadGroupByEmail(RawlsGroupEmail(email))

      user zip group map {
        case (Some(u), Some(g)) => throw new RawlsException(s"Database error: email $email refers to both a user and a group")
        case (Some(u), None) => Option(Left(u))
        case (None, Some(g)) => Option(Right(g))
        case _ => None
      }
    }

    def loadGroupIfMember(groupRef: RawlsGroupRef, userRef: RawlsUserRef): ReadAction[Option[RawlsGroup]] = {
      checkMembershipRecursively(userRef, Set.empty, Set(groupRef)).flatMap {
        case true => load(groupRef)
        case false => DBIO.successful(None)
      }
    }

    def loadGroupUserEmails(groupRef: RawlsGroupRef): ReadAction[Seq[RawlsUserEmail]] = {
      (findUsersByGroupName(groupRef.groupName.value) join
        rawlsUserQuery on (_.userSubjectId === _.userSubjectId) map (_._2.userEmail)).result.map(_.map(RawlsUserEmail))
    }

    def loadGroupSubGroupEmails(groupRef: RawlsGroupRef): ReadAction[Seq[RawlsGroupEmail]] = {
      (findSubgroupsByGroupName(groupRef.groupName.value) join
        rawlsGroupQuery on (_.childGroupName === _.groupName) map (_._2.groupEmail)).result.map(_.map(RawlsGroupEmail))
    }
    
    def listGroupsForUser(userRef: RawlsUserRef): ReadAction[Set[RawlsGroupRef]] = {
      val firstLevel = for {
        groupUser <- groupUsersQuery if (groupUser.userSubjectId === userRef.userSubjectId.value)
        group <- rawlsGroupQuery if (groupUser.groupName === group.groupName)
      } yield group
      
      firstLevel.result.map(_.toSet).flatMap(groups => listParentGroupsRecursive(groups, groups).map(_.map(groupRec => RawlsGroupRef(RawlsGroupName(groupRec.groupName)))))
    }
    
    private def listParentGroupsRecursive(groups: Set[RawlsGroupRecord], cumulativeGroups: Set[RawlsGroupRecord]): ReadAction[Set[RawlsGroupRecord]] = {
      if (groups.isEmpty) DBIO.successful(cumulativeGroups)
      else {
        val nextLevelUp = for {
          groupSubGroup <- groupSubgroupsQuery if (groupSubGroup.childGroupName.inSetBind(groups.map(_.groupName)))
          parentGroup <- rawlsGroupQuery if (groupSubGroup.parentGroupName === parentGroup.groupName)
        } yield parentGroup
        
        nextLevelUp.result.flatMap { nextGroups =>
          val nextCumulativeGroups = cumulativeGroups ++ nextGroups
          listParentGroupsRecursive(nextGroups.toSet -- nextCumulativeGroups, nextCumulativeGroups)
        }
      }
    }

    private def checkMembershipRecursively(userRef: RawlsUserRef, previouslyCheckedGroups: Set[RawlsGroupRef], groupsToCheck: Set[RawlsGroupRef]): ReadAction[Boolean] = {
      groupsToCheck.headOption match {
        case Some(checkingNow) =>
          val remainingToCheck = groupsToCheck - checkingNow

          load(checkingNow).flatMap {
            case Some(group) if group.users.contains(userRef) => DBIO.successful(true)
            case Some(group) =>
              // not found: add this group's subgroups to the check-set if they have not been seen, and try again
              val newGroupsToCheck = group.subGroups -- previouslyCheckedGroups
              checkMembershipRecursively(userRef, previouslyCheckedGroups + checkingNow, remainingToCheck ++ newGroupsToCheck)
            // this group does not exist (and may indicate a database error?) so let's keep trying with the rest
            case _ => checkMembershipRecursively(userRef, previouslyCheckedGroups + checkingNow, remainingToCheck)
          }

        // no more groups to check: not found
        case _ => DBIO.successful(false)
      }
    }

  }

  private def loadGroup(query: GroupQuery): ReadAction[Option[RawlsGroup]] = {
    uniqueResult[RawlsGroupRecord](query).flatMap {
      case None => DBIO.successful(None)
      case Some(groupRec) =>
        for {
          users <- findUsersByGroupName(groupRec.groupName).result
          subgroups <- findSubgroupsByGroupName(groupRec.groupName).result
        } yield Option(unmarshalRawlsGroup(groupRec, users, subgroups))
    }
  }

  private def marshalRawlsGroup(group: RawlsGroup): RawlsGroupRecord = {
    RawlsGroupRecord(group.groupName.value, group.groupEmail.value)
  }

  private def marshalGroupUsers(ref: RawlsUserRef, rawlsGroup: RawlsGroup): GroupUsersRecord = {
    GroupUsersRecord(ref.userSubjectId.value, rawlsGroup.groupName.value)
  }

  private def marshalGroupSubgroups(child: RawlsGroupRef, parent: RawlsGroup): GroupSubgroupsRecord = {
    GroupSubgroupsRecord(parent.groupName.value, child.groupName.value)
  }

  private def unmarshalRawlsGroup(groupRecord: RawlsGroupRecord, userRecords: Seq[GroupUsersRecord], subGroupRecords: Seq[GroupSubgroupsRecord]): RawlsGroup = {
    val userRefs = userRecords map { r => RawlsUserRef(RawlsUserSubjectId(r.userSubjectId)) }
    val subGroupRefs = subGroupRecords map { r => RawlsGroupRef(RawlsGroupName(r.childGroupName)) }
    RawlsGroup(RawlsGroupName(groupRecord.groupName), RawlsGroupEmail(groupRecord.groupEmail), userRefs.toSet, subGroupRefs.toSet)
  }

  private def findGroupByName(name: String): GroupQuery = {
    rawlsGroupQuery.filter(_.groupName === name)
  }

  private def findGroupByEmail(email: String): GroupQuery = {
    rawlsGroupQuery.filter(_.groupEmail === email)
  }

  private def findUsersByGroupName(groupName: String): GroupUsersQuery = {
    groupUsersQuery.filter(_.groupName === groupName)
  }

  private def findSubgroupsByGroupName(groupName: String): GroupSubgroupsQuery = {
    groupSubgroupsQuery.filter(_.parentGroupName === groupName)
  }

}
