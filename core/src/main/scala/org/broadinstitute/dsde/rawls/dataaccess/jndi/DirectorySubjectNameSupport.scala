package org.broadinstitute.dsde.rawls.dataaccess.jndi

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsException
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

  protected def groupDn(groupName: RawlsGroupName) = s"cn=${groupName.value},$groupsOu"
  protected def userDn(RawlsUserSubjectId: RawlsUserSubjectId) = s"uid=${RawlsUserSubjectId.value},$peopleOu"

  protected def dnToSubject(dn: String): Either[RawlsGroupName,RawlsUserSubjectId] = {
    val splitDn = dn.split(",")

    splitDn.lift(1) match {
      case Some(ou) => {
        if(ou.equalsIgnoreCase("ou=groups")) Left(RawlsGroupName(splitDn(0).stripPrefix("cn=")))
        else if(ou.equalsIgnoreCase("ou=people")) Right(RawlsUserSubjectId(splitDn(0).stripPrefix("uid=")))
        else throw new RawlsException(s"unexpected dn2 [$dn]")
      }
      case None => throw new RawlsException(s"unexpected dn [$dn]")
    }
  }

  protected def dnToGroupName(dn:String): RawlsGroupName = {
    dnToSubject(dn) match {
      case Left(gn) => gn
      case _ => throw new RawlsException(s"not a group dn [$dn]")
    }
  }

  protected def dnToUserSubjectId(dn:String): RawlsUserSubjectId = {
    dnToSubject(dn) match {
      case Right(id) => id
      case _ => throw new RawlsException(s"not a user dn [$dn]")
    }
  }
}
