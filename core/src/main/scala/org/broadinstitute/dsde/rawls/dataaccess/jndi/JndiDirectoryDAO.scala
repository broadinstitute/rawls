package org.broadinstitute.dsde.rawls.dataaccess.jndi

import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Executors
import javax.naming._
import javax.naming.directory._

import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model._
import slick.dbio.DBIO
import spray.http.StatusCodes

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Created by dvoet on 11/5/15.
 */
trait JndiDirectoryDAO extends DirectorySubjectNameSupport with JndiSupport {

  def dateFormat = new SimpleDateFormat("yyyyMMddHHmmss.SSSZ")

  implicit val executionContext: ExecutionContext

  // special exec context to use when searching isMemberOf but not also specifying a single user to search
  private val isMemberOfExecCtx: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  /** a bunch of attributes used in directory entries */
  private object Attr {
    val member = "uniqueMember"
    val memberOf = "isMemberOf"
    val email = "mail"
    val givenName = "givenName"
    val sn = "sn"
    val cn = "cn"
    val dn = "dn"
    val uid = "uid"
    val groupUpdatedTimestamp = "groupUpdatedTimestamp"
    val groupSynchronizedTimestamp = "groupSynchronizedTimestamp"
  }

  def initLdap(): ReadWriteAction[Unit] = {
    for {
      _ <- removeWorkbenchGroupSchema()
      _ <- createWorkbenchGroupSchema()
    } yield ()
  }

  def removeWorkbenchGroupSchema(): ReadWriteAction[Unit] = withContext { ctx =>
    val schema = ctx.getSchema("")

    Try { schema.destroySubcontext("ClassDefinition/workbenchGroup") }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.groupSynchronizedTimestamp) }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.groupUpdatedTimestamp) }
  }

  def createWorkbenchGroupSchema(): ReadWriteAction[Unit] = withContext { ctx =>
    val schema = ctx.getSchema("")

    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.200", Attr.groupUpdatedTimestamp, "time when group was updated", true, Option("generalizedTimeMatch"), Option("generalizedTimeOrderingMatch"), Option("1.3.6.1.4.1.1466.115.121.1.24"))
    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.201", Attr.groupSynchronizedTimestamp, "time when group was synchronized", true, Option("generalizedTimeMatch"), Option("generalizedTimeOrderingMatch"), Option("1.3.6.1.4.1.1466.115.121.1.24"))

    val attrs = new BasicAttributes(true) // Ignore case
    attrs.put("NUMERICOID", "1.3.6.1.4.1.18060.0.4.3.2.100")
    attrs.put("NAME", "workbenchGroup")
    attrs.put("SUP", "groupofuniquenames")
    attrs.put("STRUCTURAL", "true")

    val must = new BasicAttribute("MUST")
    must.add("objectclass")
    must.add(Attr.email)
    attrs.put(must)

    val may = new BasicAttribute("MAY")
    may.add(Attr.groupUpdatedTimestamp)
    may.add(Attr.groupSynchronizedTimestamp)
    attrs.put(may)


    // Add the new schema object for "fooObjectClass"
    schema.createSubcontext("ClassDefinition/workbenchGroup", attrs)
  }

  object rawlsGroupQuery {
    /**
      * @param emptyValueFn a function called when no members are present
      */
    private def addMemberAttributes(users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef], myAttrs: BasicAttributes)(emptyValueFn: BasicAttributes => Unit): Any = {
      val memberDns = users.map(user => userDn(user.userSubjectId)) ++ subGroups.map(subGroup => groupDn(subGroup.groupName))
      if (!memberDns.isEmpty) {
        val members = new BasicAttribute(Attr.member)
        memberDns.foreach(subject => members.add(subject))
        myAttrs.put(members)
      } else {
        emptyValueFn(myAttrs)
      }
    }

    def loadAllGroups(): ReadWriteAction[Seq[RawlsGroup]] = withContext { ctx =>
      ctx.search(groupsOu, new BasicAttributes("objectclass", "workbenchGroup", true)).asScala.map { result =>
        unmarshallGroup(result.getAttributes)
      }.toSeq
    }

    def save(group: RawlsGroup): ReadWriteAction[RawlsGroup] = withContext { ctx =>
      @tailrec
      def verifyUsersAndSubGroupsExist(deadline: Long): Unit = {
        val userChecks = group.users.map { userRef => Try { ctx.getAttributes(userDn(userRef.userSubjectId)) }}
        val subGroupChecks = group.subGroups.map { groupRef => Try { ctx.getAttributes(groupDn(groupRef.groupName)) }}

        if ((userChecks ++ subGroupChecks).find(_.isFailure).isDefined) {
          if (System.currentTimeMillis() > deadline) {
            throw new SQLException
          } else {
            Thread.sleep(100)
            verifyUsersAndSubGroupsExist(deadline)
          }
        }
      }

      verifyUsersAndSubGroupsExist(System.currentTimeMillis() + 1000)

      try {
        val groupContext = new BaseDirContext {
          override def getAttributes(name: String): Attributes = {
            val myAttrs = new BasicAttributes(true) // Case ignore

            val oc = new BasicAttribute("objectclass")
            Seq("top", "workbenchGroup").foreach(oc.add)
            myAttrs.put(oc)

            myAttrs.put(new BasicAttribute(Attr.groupUpdatedTimestamp, dateFormat.format(new Date())))
            myAttrs.put(new BasicAttribute(Attr.email, group.groupEmail.value))

            addMemberAttributes(group.users, group.subGroups, myAttrs) { _ => () } // do nothing when no members present

            myAttrs
          }
        }

        ctx.bind(groupDn(group.groupName), groupContext)

      } catch {
        case e: NameAlreadyBoundException =>
          val myAttrs = new BasicAttributes(true) // Case ignore

          addMemberAttributes(group.users, group.subGroups, myAttrs) { _.put(new BasicAttribute(Attr.member)) } // add attribute with no value when no member present
          myAttrs.put(new BasicAttribute(Attr.groupUpdatedTimestamp, dateFormat.format(new Date())))
          ctx.modifyAttributes(groupDn(group.groupName), DirContext.REPLACE_ATTRIBUTE, myAttrs)
      }
      group
    }

    def delete(groupRef: RawlsGroupRef) = withContext { ctx =>
      val groupPresent = Try{
        ctx.getAttributes(groupDn(groupRef.groupName))
      }
      ctx.unbind(groupDn(groupRef.groupName))
      groupPresent.isSuccess
    }

    def removeGroupMember(groupName: RawlsGroupName, removeMember: RawlsUserSubjectId): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.modifyAttributes(groupDn(groupName), DirContext.REMOVE_ATTRIBUTE, new BasicAttributes(Attr.member, userDn(removeMember)))
    }

    def removeGroupMember(groupName: RawlsGroupName, removeMember: RawlsGroupName): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.modifyAttributes(groupDn(groupName), DirContext.REMOVE_ATTRIBUTE, new BasicAttributes(Attr.member, groupDn(removeMember)))
    }

    def addGroupMember(groupName: RawlsGroupName, addMember: RawlsUserSubjectId): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.modifyAttributes(groupDn(groupName), DirContext.ADD_ATTRIBUTE, new BasicAttributes(Attr.member, userDn(addMember)))
    }

    def addGroupMember(groupName: RawlsGroupName, addMember: RawlsGroupName): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.modifyAttributes(groupDn(groupName), DirContext.ADD_ATTRIBUTE, new BasicAttributes(Attr.member, groupDn(addMember)))
    }

    def load(groupRef: RawlsGroupRef): ReadWriteAction[Option[RawlsGroup]] = withContext { ctx =>
      Try {
        val attributes = ctx.getAttributes(groupDn(groupRef.groupName))

        Option(unmarshallGroup(attributes))

      }.recover {
        case e: NameNotFoundException => None

      }.get
    }

    def load(groupRefs: TraversableOnce[RawlsGroupRef]): ReadWriteAction[Seq[RawlsGroup]] = batchedLoad(groupRefs.toSeq) { batch => { ctx =>
      val filters = batch.toSet[RawlsGroupRef].map { ref => s"(${Attr.cn}=${ref.groupName.value})" }
      ctx.search(groupsOu, s"(|${filters.mkString})", new SearchControls()).asScala.map { result =>
        unmarshallGroup(result.getAttributes)
      }.toSeq
    } }

    /** talk to doge before calling this function - loads groups and subgroups and subgroups ... */
    def loadGroupsRecursive(groups: Set[RawlsGroupRef], accumulated: Set[RawlsGroup] = Set.empty): ReadWriteAction[Set[RawlsGroup]] = {
      load(groups).flatMap { thisLevel =>
        val newAccumulated = accumulated ++ thisLevel
        val nextLevelRefs = thisLevel.toSet[RawlsGroup].flatMap(_.subGroups)
        val unvisitedNextLevelRefs = nextLevelRefs -- newAccumulated.map(RawlsGroup.toRef)

        if (unvisitedNextLevelRefs.isEmpty) {
          DBIO.successful(newAccumulated)
        } else {
          loadGroupsRecursive(unvisitedNextLevelRefs, newAccumulated)
        }
      }
    }

    def flattenGroupMembership(groupRef: RawlsGroupRef): ReadWriteAction[Set[RawlsUserRef]] = withContextUsingIsMemberOf { ctx =>
      ctx.search(peopleOu, new BasicAttributes(Attr.memberOf, groupDn(groupRef.groupName), true)).asScala.map { result =>
        RawlsUserRef(unmarshalUser(result.getAttributes).userSubjectId)
      }.toSet
    }

    def isGroupMember(groupRef: RawlsGroupRef, userRef: RawlsUserRef): ReadWriteAction[Boolean] = withContext { ctx =>
      val results = ctx.search(peopleOu, s"(&(${Attr.uid}=${userRef.userSubjectId.value})(${Attr.memberOf}=${groupDn(groupRef.groupName)}))", new SearchControls())
      results.hasMore
    }

    def loadGroupIfMember(groupRef: RawlsGroupRef, userRef: RawlsUserRef): ReadWriteAction[Option[RawlsGroup]] = {
      isGroupMember(groupRef, userRef).flatMap {
        case true => load(groupRef)
        case false => DBIO.successful(None)
      }
    }

    def listGroupsForUser(userRef: RawlsUserRef): ReadWriteAction[Set[RawlsGroupRef]] = withContext { ctx =>
      val groups = Try {
        for (
          attr <- ctx.getAttributes(userDn(userRef.userSubjectId), Array(Attr.memberOf)).getAll.asScala;
          attrE <- attr.getAll.asScala
        ) yield RawlsGroupRef(dnToGroupName(attrE.asInstanceOf[String]))
      } recover {
        // user does not exist so they can't have any groups
        case t: NameNotFoundException => Iterator.empty
      }

      groups.get.toSet
    }

    def loadFromEmail(email: String): ReadWriteAction[Option[Either[RawlsUser, RawlsGroup]]] = withContext { ctx =>
      val subjectResults = ctx.search(directoryConfig.baseDn, s"(${Attr.email}=${email})", new SearchControls(SearchControls.SUBTREE_SCOPE, 0, 0, null, false, false)).asScala.toSeq
      val subjects = subjectResults.map { result =>
        dnToSubject(result.getNameInNamespace) match {
          case Left(groupName) => Right(unmarshallGroup(result.getAttributes))
          case Right(userSubjectId) => Left(unmarshalUser(result.getAttributes))
        }
      }

      subjects match {
        case Seq() => None
        case Seq(subject) => Option(subject)
        case _ => throw new RawlsException(s"Database error: email $email refers to too many subjects: $subjects")
      }
    }

    def loadMemberEmails(groupRef: RawlsGroupRef): ReadWriteAction[Seq[String]] = {
      load(groupRef).flatMap {
        case Some(rawlsGroup) =>
          val subGroupActions = rawlsGroup.subGroups.map(load(_).map(_.get.groupEmail.value))
          val userActions = rawlsGroup.users.map(rawlsUserQuery.load(_).map(_.get.userEmail.value))
          DBIO.sequence(subGroupActions.toSeq ++ userActions)
        case None => DBIO.successful(Seq.empty)
      }
    }

    def loadEmails(refs: Seq[RawlsGroupRef]): ReadWriteAction[Map[RawlsGroupRef, RawlsGroupEmail]] = {
      DBIO.sequence(refs.map(load)).map { groups =>
        groups.collect {
          case Some(group) => RawlsGroup.toRef(group) -> group.groupEmail
        }.toMap
      }
    }

    def loadRefsFromEmails(emails: Seq[String]): ReadWriteAction[Map[String, Either[RawlsUserRef, RawlsGroupRef]]] = {
      DBIO.sequence(emails.map(loadFromEmail)).map { subjects =>
        subjects.collect {
          case Some(Left(user)) => user.userEmail.value -> Left(RawlsUser.toRef(user))
          case Some(Right(group)) => group.groupEmail.value -> Right(RawlsGroup.toRef(group))
        }.toMap
      }
    }

    def loadGroupByEmail(groupEmail: RawlsGroupEmail): ReadWriteAction[Option[RawlsGroup]] = withContext { ctx =>
      val group = ctx.search(groupsOu, new BasicAttributes(Attr.email, groupEmail.value, true)).asScala.toSeq
      group match {
        case Seq() => None
        case Seq(result) => Option(unmarshallGroup(result.getAttributes))
        case _ => throw new RawlsException(s"Found more than one group for email ${groupEmail}")
      }
    }

    def updateSynchronizedDate(rawlsGroupRef: RawlsGroupRef): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.modifyAttributes(groupDn(rawlsGroupRef.groupName), DirContext.REPLACE_ATTRIBUTE, new BasicAttributes(Attr.groupSynchronizedTimestamp, dateFormat.format(new Date()), true))
    }

    def overwriteGroupUsers(groupsWithUsers: Seq[(RawlsGroupRef, Set[RawlsUserRef])]) = withContext { ctx =>
      groupsWithUsers.map { case (groupRef, users) =>
        val myAttrs = new BasicAttributes(true)
        addMemberAttributes(users, Set.empty, myAttrs) { _.put(new BasicAttribute(Attr.member)) }  // add attribute with no value when no member present
        ctx.modifyAttributes(groupDn(groupRef.groupName), DirContext.REPLACE_ATTRIBUTE, myAttrs)
        groupRef
      }
    }

    def removeUserFromAllGroups(userRef: RawlsUserRef): ReadWriteAction[Boolean] = withContext { ctx =>
      val userAttributes = new BasicAttributes(Attr.member, userDn(userRef.userSubjectId), true)
      val groupResults = ctx.search(groupsOu, userAttributes, Array[String]()).asScala

      groupResults.foreach { result =>
        ctx.modifyAttributes(result.getNameInNamespace, DirContext.REMOVE_ATTRIBUTE, userAttributes)
      }

      !groupResults.isEmpty
    }

    def listAncestorGroups(groupName: RawlsGroupName): ReadWriteAction[Set[RawlsGroupName]] = withContext { ctx =>
      val groups = for (
        attr <- ctx.getAttributes(groupDn(groupName), Array(Attr.memberOf)).getAll.asScala;
        attrE <- attr.getAll.asScala
      ) yield dnToGroupName(attrE.asInstanceOf[String])

      groups.toSet
    }

    def intersectGroupMembership(groups: Set[RawlsGroupRef]): ReadWriteAction[Set[RawlsUserRef]] = withContextUsingIsMemberOf { ctx =>
      val groupFilters = groups.map(g => s"(${Attr.memberOf}=${groupDn(g.groupName)})")
      ctx.search(peopleOu, s"(&${groupFilters.mkString})", new SearchControls()).asScala.map { result =>
        RawlsUserRef(dnToUserSubjectId(result.getNameInNamespace))
      }.toSet
    }

  }

  private def unmarshallGroup(attributes: Attributes) = {
    val cn = getAttribute[String](attributes, Attr.cn).getOrElse(throw new RawlsException(s"${Attr.cn} attribute missing"))
    val email = getAttribute[String](attributes, Attr.email).getOrElse(throw new RawlsException(s"${Attr.email} attribute missing"))
    val memberDns = getAttributes[String](attributes, Attr.member).getOrElse(Set.empty).toSet

    val members = memberDns.map(dnToSubject)
    val users = members.collect { case Right(user) => RawlsUserRef(user) }
    val groups = members.collect { case Left(group) => RawlsGroupRef(group) }
    val group = RawlsGroup(RawlsGroupName(cn), RawlsGroupEmail(email), users, groups)
    group
  }

  object rawlsUserQuery {

    def loadAllUsers(): ReadWriteAction[Seq[RawlsUser]] = withContext { ctx =>
      ctx.search(peopleOu, new BasicAttributes("objectclass", "inetOrgPerson", true)).asScala.map { result =>
        unmarshalUser(result.getAttributes)
      }.toSeq
    }

    def loadUserByEmail(email: RawlsUserEmail): ReadWriteAction[Option[RawlsUser]] = withContext { ctx =>
      val person = ctx.search(peopleOu, new BasicAttributes(Attr.email, email.value, true)).asScala.toSeq
      person match {
        case Seq() => None
        case Seq(result) => Option(unmarshalUser(result.getAttributes))
        case _ => throw new RawlsException(s"Found more than one user for email ${email}")
      }
    }

    def createUser(user: RawlsUser): ReadWriteAction[RawlsUser] = withContext { ctx =>
      try {
        val userContext = new BaseDirContext {
          override def getAttributes(name: String): Attributes = {
            val myAttrs = new BasicAttributes(true) // Case ignore

            val oc = new BasicAttribute("objectclass")
            Seq("top", "inetOrgPerson").foreach(oc.add)
            myAttrs.put(oc)

            myAttrs.put(new BasicAttribute(Attr.email, user.userEmail.value))
            myAttrs.put(new BasicAttribute(Attr.sn, user.userSubjectId.value))
            myAttrs.put(new BasicAttribute(Attr.cn, user.userSubjectId.value))
            myAttrs.put(new BasicAttribute(Attr.uid, user.userSubjectId.value))

            myAttrs
          }
        }

        ctx.bind(userDn(user.userSubjectId), userContext)
        user
      } catch {
        case e: NameAlreadyBoundException => user // user already exists, do nothing
      }
    }

    def load(userId: RawlsUserRef): ReadWriteAction[Option[RawlsUser]] = withContext { ctx =>
      Try {
        val attributes = ctx.getAttributes(userDn(userId.userSubjectId))

        Option(unmarshalUser(attributes))

      }.recover {
        case e: NameNotFoundException => None

      }.get
    }

    def load(userRefs: TraversableOnce[RawlsUserRef]): ReadWriteAction[Seq[RawlsUser]] = batchedLoad(userRefs.toSeq) { batch => { ctx =>
      val filters = batch.toSet[RawlsUserRef].map { ref => s"(${Attr.uid}=${ref.userSubjectId.value})" }
      ctx.search(peopleOu, s"(|${filters.mkString})", new SearchControls()).asScala.map { result =>
        unmarshalUser(result.getAttributes)
      }.toSeq
    } }

    def deleteUser(userId: RawlsUserSubjectId): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.unbind(userDn(userId))
    }

    def countUsers(): ReadWriteAction[SingleStatistic] = {
      loadAllUsers().map(users => SingleStatistic(users.size))
    }

  }

  private def unmarshalUser(attributes: Attributes): RawlsUser = {
    val uid = getAttribute[String](attributes, Attr.uid).getOrElse(throw new RawlsException(s"${Attr.uid} attribute missing"))
    val email = getAttribute[String](attributes, Attr.email).getOrElse(throw new RawlsException(s"${Attr.email} attribute missing"))

    RawlsUser(RawlsUserSubjectId(uid), RawlsUserEmail(email))
  }  
  
  private def getAttribute[T](attributes: Attributes, key: String): Option[T] = {
    Option(attributes.get(key)).map(_.get.asInstanceOf[T])
  }

  private def getAttributes[T](attributes: Attributes, key: String): Option[TraversableOnce[T]] = {
    Option(attributes.get(key)).map(_.getAll.asScala.map(_.asInstanceOf[T]))
  }

  private def withContext[T](op: InitialDirContext => T): ReadWriteAction[T] = DBIO.from(withContext(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password)(op))
  private def withContextUsingIsMemberOf[T](op: InitialDirContext => T): ReadWriteAction[T] = DBIO.from(withContext(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password)(op)(isMemberOfExecCtx))
  private def batchedLoad[T, R](input: Seq[T])(op: Seq[T] => InitialDirContext => Seq[R]): ReadWriteAction[Seq[R]] = DBIO.from(batchedLoad(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password)(input)(op))
}


