package org.broadinstitute.dsde.rawls

import javax.naming.{NameAlreadyBoundException, NameNotFoundException}
import javax.naming.directory._

import org.broadinstitute.dsde.rawls.dataaccess.jndi.{BaseDirContext, DirectorySubjectNameSupport, JndiSupport}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsGroup, RawlsGroupRef, RawlsUserRef}
import spray.http.StatusCodes

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class SamDataSaver(implicit executionContext: ExecutionContext) extends JndiSupport with DirectorySubjectNameSupport {
  object Attr {
    val resourceId = "resourceId"
    val resourceType = "resourceType"
    val subject = "subject"
    val action = "action"
    val role = "role"
    val email = "mail"
    val ou = "ou"
    val cn = "cn"
    val policy = "policy"
    val uniqueMember = "uniqueMember"
    val groupUpdatedTimestamp = "groupUpdatedTimestamp"
    val groupSynchronizedTimestamp = "groupSynchronizedTimestamp"
    val member = "member"
    val memberOf = "isMemberOf"
    val givenName = "givenName"
    val sn = "sn"
    val uid = "uid"
    val project = "project"
  }

  object ObjectClass {
    val workbenchGroup = "workbenchGroup"
    val petServiceAccount = "petServiceAccount"
    val resourceType = "resourceType"
    val resource = "resource"
    val policy = "policy"
  }

  def savePolicyGroup(policyGroup: RawlsGroup, resourceType: String, resourceId: String): Future[RawlsGroup] = withContext { ctx =>
    createOrgUnit(resourcesOu, ctx)
    createResourceType(resourceType, ctx)
    createResource(resourceType, resourceId, ctx)
    save(policyGroup, resourceType, resourceId, ctx)

    policyGroup
  }

  protected def resourceTypeDn(resourceTypeName: String) = s"${Attr.resourceType}=${resourceTypeName},$resourcesOu"
  protected def resourceDn(resourceTypeName: String, resourceId: String) = s"${Attr.resourceId}=${resourceId},${resourceTypeDn(resourceTypeName)}"

  private def save(group: RawlsGroup, resourceType: String, resourceId: String, ctx: DirContext): Unit = {
    if (!isPolicyGroupName(group.groupName)) {
      throw new RawlsException("this is only for saving policies")
    }

    try {
      val groupContext = new BaseDirContext {
        override def getAttributes(name: String): Attributes = {
          val myAttrs = new BasicAttributes(true) // Case ignore

          val oc = new BasicAttribute("objectclass")
          Seq("top", ObjectClass.policy).foreach(oc.add)
          myAttrs.put(oc)

          myAttrs.put(new BasicAttribute(Attr.cn, group.groupName.value))
          myAttrs.put(new BasicAttribute(Attr.email, group.groupEmail.value))
          myAttrs.put(new BasicAttribute(Attr.resourceId, resourceId))
          myAttrs.put(new BasicAttribute(Attr.resourceType, resourceType))

          addMemberAttributes(group.users, group.subGroups, myAttrs) { _ => () } // do nothing when no members present

          myAttrs
        }
      }

      ctx.bind(groupDn(group.groupName), groupContext)

    } catch {
      case e: NameAlreadyBoundException =>
//        val myAttrs = new BasicAttributes(true) // Case ignore
//
//        addMemberAttributes(group.users, group.subGroups, myAttrs) { _.put(new BasicAttribute(Attr.uniqueMember)) } // add attribute with no value when no member present
//        ctx.modifyAttributes(groupDn(group.groupName), DirContext.REPLACE_ATTRIBUTE, myAttrs)
      case e: NameNotFoundException =>
    }
  }

  private def addMemberAttributes(users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef], myAttrs: BasicAttributes)(emptyValueFn: BasicAttributes => Unit): Any = {
    val memberDns = users.map(user => userDn(user.userSubjectId)) ++ subGroups.map(subGroup => groupDn(subGroup.groupName))
    if (!memberDns.isEmpty) {
      val members = new BasicAttribute(Attr.uniqueMember)
      memberDns.foreach(subject => members.add(subject))
      myAttrs.put(members)
    } else {
      emptyValueFn(myAttrs)
    }
  }

  private def createOrgUnit(dn: String, ctx: DirContext): Unit = {
    try {
      val resourcesContext = new BaseDirContext {
        override def getAttributes(name: String): Attributes = {
          val myAttrs = new BasicAttributes(true)  // Case ignore

          val oc = new BasicAttribute("objectclass")
          Seq("top", "organizationalUnit").foreach(oc.add)
          myAttrs.put(oc)

          myAttrs
        }
      }

      ctx.bind(dn, resourcesContext)

    } catch {
      case e: NameAlreadyBoundException => // ignore
    }
  }

  private def createResourceType(resourceTypeName: String, ctx: DirContext): Unit = {
    try {
      val resourceContext = new BaseDirContext {
        override def getAttributes(name: String): Attributes = {
          val myAttrs = new BasicAttributes(true) // Case ignore

          val oc = new BasicAttribute("objectclass")
          Seq("top", ObjectClass.resourceType).foreach(oc.add)
          myAttrs.put(oc)
          myAttrs.put(Attr.ou, "resources")

          myAttrs
        }
      }

      ctx.bind(resourceTypeDn(resourceTypeName), resourceContext)
    } catch {
      case _: NameAlreadyBoundException =>
    }
  }

  private def createResource(resourceTypeName: String, resource: String, ctx: DirContext): Unit = {
    try {
      val resourceContext = new BaseDirContext {
        override def getAttributes(name: String): Attributes = {
          val myAttrs = new BasicAttributes(true) // Case ignore

          val oc = new BasicAttribute("objectclass")
          Seq("top", ObjectClass.resource).foreach(oc.add)
          myAttrs.put(oc)
          myAttrs.put(Attr.resourceType, resourceTypeName)

          myAttrs
        }
      }

      ctx.bind(resourceDn(resourceTypeName, resource), resourceContext)
    } catch {
      case _: NameAlreadyBoundException =>
    }
  }

  def createPolicySchema(): Future[Unit] = withContext { ctx =>
    val schema = ctx.getSchema("")

    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.1", Attr.resourceType, "the type of the resource", true, equality = Option("caseIgnoreMatch"))
    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.8", Attr.resourceId, "the id of the resource", true, equality = Option("caseIgnoreMatch"))
    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.4", Attr.action, "the actions applicable to a policy", false)
    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.6", Attr.role, "the roles for the policy, if any", false)
    createAttributeDefinition(schema, "1.3.6.1.4.1.18060.0.4.3.2.7", Attr.policy, "the policy name", true, equality = Option("caseIgnoreMatch"))

    val policyAttrs = new BasicAttributes(true) // Ignore case
    policyAttrs.put("NUMERICOID", "1.3.6.1.4.1.18060.0.4.3.2.0")
    policyAttrs.put("NAME", ObjectClass.policy)
    policyAttrs.put("DESC", "list subjects for a policy")
    policyAttrs.put("SUP", ObjectClass.workbenchGroup)
    policyAttrs.put("STRUCTURAL", "true")

    val policyMust = new BasicAttribute("MUST")
    policyMust.add("objectclass")
    policyMust.add(Attr.resourceType)
    policyMust.add(Attr.resourceId)
    policyMust.add(Attr.cn)
    policyMust.add(Attr.policy)
    policyAttrs.put(policyMust)

    val policyMay = new BasicAttribute("MAY")
    policyMay.add(Attr.action)
    policyMay.add(Attr.role)
    policyAttrs.put(policyMay)

    val resourceTypeAttrs = new BasicAttributes(true) // Ignore case
    resourceTypeAttrs.put("NUMERICOID", "1.3.6.1.4.1.18060.0.4.3.2.1000")
    resourceTypeAttrs.put("NAME", ObjectClass.resourceType)
    resourceTypeAttrs.put("DESC", "type of the resource")
    resourceTypeAttrs.put("SUP", "top")
    resourceTypeAttrs.put("STRUCTURAL", "true")

    val resourceTypeMust = new BasicAttribute("MUST")
    resourceTypeMust.add("objectclass")
    resourceTypeMust.add(Attr.resourceType)
    resourceTypeMust.add(Attr.ou)
    resourceTypeAttrs.put(resourceTypeMust)

    val resourceAttrs = new BasicAttributes(true) // Ignore case
    resourceAttrs.put("NUMERICOID", "1.3.6.1.4.1.18060.0.4.3.2.1001")
    resourceAttrs.put("NAME", ObjectClass.resource)
    resourceAttrs.put("DESC", "the resource")
    resourceAttrs.put("SUP", "top")
    resourceAttrs.put("STRUCTURAL", "true")

    val resourceMust = new BasicAttribute("MUST")
    resourceMust.add("objectclass")
    resourceMust.add(Attr.resourceId)
    resourceMust.add(Attr.resourceType)
    resourceAttrs.put(resourceMust)

    // Add the new schema object for "fooObjectClass"
    schema.createSubcontext("ClassDefinition/" + ObjectClass.resourceType, resourceTypeAttrs)
    schema.createSubcontext("ClassDefinition/" + ObjectClass.resource, resourceAttrs)
    schema.createSubcontext("ClassDefinition/" + ObjectClass.policy, policyAttrs)
  }

  def removePolicySchema(): Future[Unit] = withContext { ctx =>
    val schema = ctx.getSchema("")

    // Intentionally ignores errors
    Try { schema.destroySubcontext("ClassDefinition/" + ObjectClass.policy) }
    Try { schema.destroySubcontext("ClassDefinition/" + ObjectClass.resource) }
    Try { schema.destroySubcontext("ClassDefinition/" + ObjectClass.resourceType) }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.resourceType) }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.resourceId) }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.action) }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.role) }
    Try { schema.destroySubcontext("AttributeDefinition/" + Attr.policy) }
  }

  private def withContext[T](op: InitialDirContext => T): Future[T] = withContext(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password)(op)
}
