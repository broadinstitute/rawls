package org.broadinstitute.dsde.rawls.dataaccess.jndi

import javax.naming._
import javax.naming.directory._

import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model._
import slick.dbio.DBIO
import spray.http.StatusCodes

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Created by dvoet on 11/5/15.
 */
trait JndiDirectoryDAO extends DirectorySubjectNameSupport with JndiSupport {
  protected val directoryConfig: DirectoryConfig
  implicit val executionContext: ExecutionContext
  /** a bunch of attributes used in directory entries */
  private object Attr {
    val member = "uniqueMember"
    val memberOf = "isMemberOf"
    val email = "mail"
    val givenName = "givenName"
    val sn = "sn"
    val cn = "cn"
    val uid = "uid"
    val groupUpdatedTimestamp = "groupUpdatedTimestamp"
    val groupSynchronizedTimestamp = "groupSynchronizedTimestamp"
  }

  def init(): ReadWriteAction[Unit] = {
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

  def createGroup(group: RawlsGroup): ReadWriteAction[RawlsGroup] = withContext { ctx =>
    try {
      val groupContext = new BaseDirContext {
        override def getAttributes(name: String): Attributes = {
          val myAttrs = new BasicAttributes(true)  // Case ignore

          val oc = new BasicAttribute("objectclass")
          Seq("top", "workbenchGroup").foreach(oc.add)
          myAttrs.put(oc)

          myAttrs.put(new BasicAttribute(Attr.email, group.groupEmail.value))

          val memberDns = group.users.map(user => userDn(user.userSubjectId)) ++ group.subGroups.map(subGroup => groupDn(subGroup.groupName))
          if (!memberDns.isEmpty) {
            val members = new BasicAttribute(Attr.member)
            memberDns.foreach(subject => members.add(subject))
            myAttrs.put(members)
          }

          myAttrs
        }
      }

      ctx.bind(groupDn(group.groupName), groupContext)
      group

    } catch {
      case e: NameAlreadyBoundException =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"group name ${group.groupName.value} already exists"))
    }
  }

  def deleteGroup(groupName: RawlsGroupName): ReadWriteAction[Unit] = withContext { ctx =>
    ctx.unbind(groupDn(groupName))
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

  def loadGroup(groupName: RawlsGroupName): ReadWriteAction[Option[RawlsGroup]] = withContext { ctx =>
    Try {
      val attributes = ctx.getAttributes(groupDn(groupName))

      val cn = getAttribute[String](attributes, Attr.cn).getOrElse(throw new RawlsException(s"${Attr.cn} attribute missing: $groupName"))
      val email = getAttribute[String](attributes, Attr.email).getOrElse(throw new RawlsException(s"${Attr.email} attribute missing: $groupName"))
      val memberDns = getAttributes[String](attributes, Attr.member).getOrElse(Set.empty).toSet

      val members = memberDns.map(dnToSubject)
      val users = members.collect{case Right(user) => RawlsUserRef(user)}
      val groups = members.collect{case Left(group) => RawlsGroupRef(group)}
      Option(RawlsGroup(RawlsGroupName(cn), RawlsGroupEmail(email), users, groups))

    }.recover {
      case e: NameNotFoundException => None

    }.get
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

    def save(user: RawlsUser): ReadWriteAction[RawlsUser] = withContext { ctx =>
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
        case e: NameAlreadyBoundException =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"user with id ${user.userSubjectId.value} already exists"))
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

    def deleteUser(userId: RawlsUserSubjectId): ReadWriteAction[Unit] = withContext { ctx =>
      ctx.unbind(userDn(userId))
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

  def listUsersGroups(userId: RawlsUserSubjectId): ReadWriteAction[Set[RawlsGroupName]] = withContext { ctx =>
    val groups = for (
      attr <- ctx.getAttributes(userDn(userId), Array(Attr.memberOf)).getAll.asScala;
      attrE <- attr.getAll.asScala
    ) yield dnToGroupName(attrE.asInstanceOf[String])

    groups.toSet
  }

  def listFlattenedGroupUsers(groupName: RawlsGroupName): ReadWriteAction[Set[RawlsUserSubjectId]] = withContext { ctx =>
    ctx.search(peopleOu, new BasicAttributes(Attr.memberOf, groupDn(groupName), true)).asScala.map { result =>
      unmarshalUser(result.getAttributes).userSubjectId
    }.toSet
  }

  def listAncestorGroups(groupName: RawlsGroupName): ReadWriteAction[Set[RawlsGroupName]] = withContext { ctx =>
    val groups = for (
      attr <- ctx.getAttributes(groupDn(groupName), Array(Attr.memberOf)).getAll.asScala;
      attrE <- attr.getAll.asScala
    ) yield dnToGroupName(attrE.asInstanceOf[String])

    groups.toSet
  }

  private def withContext[T](op: InitialDirContext => T): ReadWriteAction[T] = DBIO.from(withContext(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password)(op))
}


