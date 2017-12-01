package org.broadinstitute.dsde.rawls.dataaccess.jndi

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{RawlsGroupName, RawlsUserSubjectId}

import scala.util.matching.Regex

/**
  * Created by dvoet on 6/6/17.
  */
trait DirectorySubjectNameSupport {
  private val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())

  val directoryConfig = DirectoryConfig(
    conf.getString("directory.url"),
    conf.getString("directory.user"),
    conf.getString("directory.password"),
    conf.getString("directory.baseDn")
  )

  val peopleOu = s"ou=people,${directoryConfig.baseDn}"
  val groupsOu = s"ou=groups,${directoryConfig.baseDn}"
  val resourcesOu = s"ou=resources,${directoryConfig.baseDn}"

  val policyGroupNamePattern = "([^@]+)@([^@]+)@([^@]+)".r
  def policyGroupName(resourceType: String, resourceId: String, policyName: String) = s"$policyName@$resourceId@$resourceType"
  def isPolicyGroupName(groupName: RawlsGroupName) = groupName.value.contains("@")

  protected def groupDn(groupName: RawlsGroupName) = {
    groupName.value match {
      case policyGroupNamePattern(policyName, resourceId, resourceType) => policyDn(SamResourceTypeName(resourceType), resourceId, policyName)
      case _ => s"cn=${groupName.value},$groupsOu"
    }
  }
  protected def userDn(RawlsUserSubjectId: RawlsUserSubjectId) = s"uid=${RawlsUserSubjectId.value},$peopleOu"
  protected def resourceTypeDn(resourceTypeName: SamResourceTypeName) = s"resourceType=${resourceTypeName.value},$resourcesOu"
  protected def resourceDn(resourceTypeName: SamResourceTypeName, resourceId: String) = s"resourceId=$resourceId,${resourceTypeDn(resourceTypeName)}"
  protected def policyDn(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String): String = s"policy=$policyName,${resourceDn(resourceTypeName, resourceId)}"

  protected def dnToSubject(dn: String): Option[Either[RawlsGroupName,RawlsUserSubjectId]] = {
    val groupMatcher = dnMatcher(Seq("cn"), groupsOu)
    val personMatcher = dnMatcher(Seq("uid"), peopleOu)
    val policyMatcher = dnMatcher(Seq("policy", "resourceId", "resourceType"), resourcesOu)

    dn match {
      case groupMatcher(cn) => Option(Left(RawlsGroupName(cn)))
      case personMatcher(uid) => Option(Right(RawlsUserSubjectId(uid)))
      case policyMatcher(policyName, resourceId, resourceTypeName) => Option(Left(RawlsGroupName(policyGroupName(resourceTypeName, resourceId, policyName))))
      case _ => None
    }
  }

  protected def dnToGroupName(dn:String): Option[RawlsGroupName] = {
    dnToSubject(dn).collect {
      case Left(gn) => gn
    }
  }

  protected def dnToUserSubjectId(dn:String): Option[RawlsUserSubjectId] = {
    dnToSubject(dn).collect {
      case Right(id) => id
    }
  }

  protected def dnMatcher(matchAttributeNames: Seq[String], baseDn: String): Regex = {
    val partStrings = matchAttributeNames.map { attrName => s"$attrName=([^,]+)" }
    partStrings.mkString("(?i)", ",", s",$baseDn").r
  }
}
