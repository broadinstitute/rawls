package org.broadinstitute.dsde.rawls.dataaccess.jndi

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{RawlsGroupName, RawlsUserSubjectId}

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

  protected def groupDn(groupName: RawlsGroupName) = s"cn=${groupName.value},$groupsOu"
  protected def userDn(RawlsUserSubjectId: RawlsUserSubjectId) = s"uid=${RawlsUserSubjectId.value},$peopleOu"
  protected def resourceTypeDn(resourceTypeName: SamResourceTypeName) = s"resourceType=${resourceTypeName.value},$resourcesOu"
  protected def resourceDn(resourceTypeName: SamResourceTypeName, resourceId: String) = s"resourceId=$resourceId,${resourceTypeDn(resourceTypeName)}"
  protected def policyDn(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String): String = s"policy=$policyName,${resourceDn(resourceTypeName, resourceId)}"

  protected def dnToSubject(dn: String): Option[Either[RawlsGroupName,RawlsUserSubjectId]] = {
    val splitDn = dn.split(",")

    splitDn.lift(1) match {
      case Some(ou) => {
        if(ou.equalsIgnoreCase("ou=groups")) Option(Left(RawlsGroupName(splitDn(0).stripPrefix("cn="))))
        else if(ou.equalsIgnoreCase("ou=people")) Option(Right(RawlsUserSubjectId(splitDn(0).stripPrefix("uid="))))
        else None
      }
      case None => throw new RawlsException(s"unexpected dn [$dn]")
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
}
