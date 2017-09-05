package org.broadinstitute.dsde.rawls.dataaccess

import java.util
import java.util.concurrent.Executors
import javax.naming._
import javax.naming.directory._

import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsUserSubjectId}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.http.StatusCodes

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Created by dvoet on 11/5/15.
 */
class JndiUserDirectoryDAO(providerUrl: String, user: String, password: String, groupDn: String, memberAttribute: String, userObjectClasses: List[String], userAttributes: List[String], userDnFormat: String) extends UserDirectoryDAO {

  implicit private val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(200))

  override def createUser(user: RawlsUserSubjectId): Future[Unit] = withContext { ctx =>
    try {
      val p = new Person(user)
      ctx.bind(p.name, p)
    } catch {
      case e: NameAlreadyBoundException =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, e.getMessage))
    }
  }

  override def removeUser(user: RawlsUserSubjectId): Future[Unit] = withContext { ctx =>
    ctx.unbind(new Person(user).name)
  }

  override def disableUser(user: RawlsUserSubjectId): Future[Unit] = withContext { ctx =>
    ctx.modifyAttributes(groupDn, DirContext.REMOVE_ATTRIBUTE, new BasicAttributes(memberAttribute, new Person(user).name))
  }

  override def enableUser(user: RawlsUserSubjectId): Future[Unit] = withContext { ctx =>
    ctx.modifyAttributes(groupDn, DirContext.ADD_ATTRIBUTE, new BasicAttributes(memberAttribute, new Person(user).name))
  }

  override def isEnabled(user: RawlsUserSubjectId): Future[Boolean] = withContext { ctx =>
    val members = for (
      attr <- ctx.getAttributes(groupDn, Array(memberAttribute)).getAll;
      attrE <- attr.getAll
    ) yield attrE.asInstanceOf[String]

    members.contains(new Person(user).name)
  }

  override def listUsers(implicit executionContext: ExecutionContext): Future[List[RawlsUserSubjectId]] = withContext { ctx =>
    // LDAP craziness bears some explanation:

    // Strip out the cn (common name) from the userDnFormat, leaving something like this (for example):
    // ou=people,dc=dsde-dev,dc=broadinstitute,dc=org
    // This defines a target context (base object) for the search.
    val name = userDnFormat.replaceAll("cn=.*?,", "")

    // Define a filter which is a logical AND of all the configured user object classes.
    // The resulting filter looks something like this:
    // (&(objectclass=inetOrgPerson)(objectclass=organizationalPerson)(objectclass=person)(objectclass=top))
    val filter = s"(&${userObjectClasses.map(oc => s"(objectclass=$oc)").mkString})"

    // Define SUBTREE_SCOPE to recursively search all subtrees rooted at the named object.
    val controls = new SearchControls()
    controls.setSearchScope(SearchControls.SUBTREE_SCOPE)

    // Run the search and map results to RawlsUserSubjectId
    val results = ctx.search(name, filter, controls)
    results.toList.map(r => RawlsUserSubjectId(r.getName))
  }

  private def getContext(): InitialDirContext = {
    val env = new util.Hashtable[String, String]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, providerUrl)
    env.put(Context.SECURITY_PRINCIPAL, user)
    env.put(Context.SECURITY_CREDENTIALS, password)

    new InitialDirContext(env)
  }

  private def withContext[T](op: InitialDirContext => T)(implicit executionContext: ExecutionContext): Future[T] = Future {
    val ctx = getContext()
    val t = Try(op(ctx))
    ctx.close()
    t.get
  }

  private class Person(rawlsSubjectId: RawlsUserSubjectId) extends BaseDirContext {
    override def getAttributes(name: String): Attributes = {
      val myAttrs = new BasicAttributes(true)  // Case ignore

      val oc = new BasicAttribute("objectclass")
      userObjectClasses.foreach(oc.add)
      myAttrs.put(oc)

      userAttributes.foreach(myAttrs.put(_, rawlsSubjectId.value))

      myAttrs
    }

    val name = userDnFormat.format(rawlsSubjectId.value)
  }
}


/**
 * this does nothing but throw new OperationNotSupportedException but makes extending classes nice
 */
trait BaseDirContext extends DirContext {
  override def getAttributes(name: Name): Attributes = throw new OperationNotSupportedException
  override def getAttributes(name: String): Attributes = throw new OperationNotSupportedException
  override def getAttributes(name: Name, attrIds: Array[String]): Attributes = throw new OperationNotSupportedException
  override def getAttributes(name: String, attrIds: Array[String]): Attributes = throw new OperationNotSupportedException
  override def getSchema(name: Name): DirContext = throw new OperationNotSupportedException
  override def getSchema(name: String): DirContext = throw new OperationNotSupportedException
  override def createSubcontext(name: Name, attrs: Attributes): DirContext = throw new OperationNotSupportedException
  override def createSubcontext(name: String, attrs: Attributes): DirContext = throw new OperationNotSupportedException
  override def modifyAttributes(name: Name, mod_op: Int, attrs: Attributes): Unit = throw new OperationNotSupportedException
  override def modifyAttributes(name: String, mod_op: Int, attrs: Attributes): Unit = throw new OperationNotSupportedException
  override def modifyAttributes(name: Name, mods: Array[ModificationItem]): Unit = throw new OperationNotSupportedException
  override def modifyAttributes(name: String, mods: Array[ModificationItem]): Unit = throw new OperationNotSupportedException
  override def getSchemaClassDefinition(name: Name): DirContext = throw new OperationNotSupportedException
  override def getSchemaClassDefinition(name: String): DirContext = throw new OperationNotSupportedException
  override def rebind(name: Name, obj: scala.Any, attrs: Attributes): Unit = throw new OperationNotSupportedException
  override def rebind(name: String, obj: scala.Any, attrs: Attributes): Unit = throw new OperationNotSupportedException
  override def bind(name: Name, obj: scala.Any, attrs: Attributes): Unit = throw new OperationNotSupportedException
  override def bind(name: String, obj: scala.Any, attrs: Attributes): Unit = throw new OperationNotSupportedException
  override def search(name: Name, matchingAttributes: Attributes, attributesToReturn: Array[String]): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: String, matchingAttributes: Attributes, attributesToReturn: Array[String]): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: Name, matchingAttributes: Attributes): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: String, matchingAttributes: Attributes): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: Name, filter: String, cons: SearchControls): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: String, filter: String, cons: SearchControls): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: Name, filterExpr: String, filterArgs: Array[AnyRef], cons: SearchControls): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def search(name: String, filterExpr: String, filterArgs: Array[AnyRef], cons: SearchControls): NamingEnumeration[SearchResult] = throw new OperationNotSupportedException
  override def getNameInNamespace: String = throw new OperationNotSupportedException
  override def addToEnvironment(propName: String, propVal: scala.Any): AnyRef = throw new OperationNotSupportedException
  override def rename(oldName: Name, newName: Name): Unit = throw new OperationNotSupportedException
  override def rename(oldName: String, newName: String): Unit = throw new OperationNotSupportedException
  override def lookup(name: Name): AnyRef = throw new OperationNotSupportedException
  override def lookup(name: String): AnyRef = throw new OperationNotSupportedException
  override def destroySubcontext(name: Name): Unit = throw new OperationNotSupportedException
  override def destroySubcontext(name: String): Unit = throw new OperationNotSupportedException
  override def composeName(name: Name, prefix: Name): Name = throw new OperationNotSupportedException
  override def composeName(name: String, prefix: String): String = throw new OperationNotSupportedException
  override def createSubcontext(name: Name): Context = throw new OperationNotSupportedException
  override def createSubcontext(name: String): Context = throw new OperationNotSupportedException
  override def unbind(name: Name): Unit = throw new OperationNotSupportedException
  override def unbind(name: String): Unit = throw new OperationNotSupportedException
  override def removeFromEnvironment(propName: String): AnyRef = throw new OperationNotSupportedException
  override def rebind(name: Name, obj: scala.Any): Unit = throw new OperationNotSupportedException
  override def rebind(name: String, obj: scala.Any): Unit = throw new OperationNotSupportedException
  override def getEnvironment: util.Hashtable[_, _] = throw new OperationNotSupportedException
  override def list(name: Name): NamingEnumeration[NameClassPair] = throw new OperationNotSupportedException
  override def list(name: String): NamingEnumeration[NameClassPair] = throw new OperationNotSupportedException
  override def close(): Unit = throw new OperationNotSupportedException
  override def lookupLink(name: Name): AnyRef = throw new OperationNotSupportedException
  override def lookupLink(name: String): AnyRef = throw new OperationNotSupportedException
  override def getNameParser(name: Name): NameParser = throw new OperationNotSupportedException
  override def getNameParser(name: String): NameParser = throw new OperationNotSupportedException
  override def bind(name: Name, obj: scala.Any): Unit = throw new OperationNotSupportedException
  override def bind(name: String, obj: scala.Any): Unit = throw new OperationNotSupportedException
  override def listBindings(name: Name): NamingEnumeration[Binding] = throw new OperationNotSupportedException
  override def listBindings(name: String): NamingEnumeration[Binding] = throw new OperationNotSupportedException
}