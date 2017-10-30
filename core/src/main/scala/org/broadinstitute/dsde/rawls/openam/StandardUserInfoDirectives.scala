package org.broadinstitute.dsde.rawls.openam

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.DataSource
import org.broadinstitute.dsde.rawls.dataaccess.jndi.{DirectoryConfig, JndiDirectoryDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail, RawlsUserRef, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.user.UserService
import shapeless.HNil
import slick.backend.DatabaseConfig
import slick.driver.JdbcDriver
import spray.http.{HttpHeader, OAuth2BearerToken}
import spray.routing
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.{ExecutionContext, Future}

trait StandardUserInfoDirectives extends UserInfoDirectives with JndiDirectoryDAO {

  val userServiceConstructor: UserInfo => UserService

  //  def requireUserInfo(): Directive1[UserInfo] = {
  //    val userInfo= for (accessToken <- accessTokenHeaderDirective;
  //        userEmail <- emailHeaderDirective;
  //        accessTokenExpiresIn <- accessTokenExpiresInHeaderDirective;
  //        userSubjectId <- userSubjectIdDirective;
  //    ) yield UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(accessToken), accessTokenExpiresIn.toLong, RawlsUserSubjectId(userSubjectId))
  //    userInfo.map {
  //      case a  => {
  //        val userService = UserService.constructor(a,"","","","")
  //        userService
  //      }
  //    }
  //  }

  def requireUserInfo: Directive1[UserInfo] = {

    val userInfo = for(
      accessToken <- accessTokenHeaderDirective;
      userEmail <- emailHeaderDirective;
      accessTokenExpiresIn <- accessTokenExpiresInHeaderDirective;
      userSubjectId <- userSubjectIdDirective
    ) yield UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(accessToken), accessTokenExpiresIn.toLong, RawlsUserSubjectId(userSubjectId))

    userInfo flatMap { ui =>
      onSuccess(getWorkbenchUserEmailId(ui.userEmail.toString)).map {
        case Some(resourceType) => UserInfo(resourceType.userEmail, ui.accessToken, ui.accessTokenExpiresIn, resourceType.userSubjectId)
        case None => UserInfo(ui.userEmail, ui.accessToken, ui.accessTokenExpiresIn, ui.userSubjectId)
      }
    }
  }

    private def getWorkbenchUserEmailId(email:String):Future[Option[RawlsUser]] = {
      val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())


      val directoryConfig = DirectoryConfig(
        conf.getString("directory.url"),
        conf.getString("directory.user"),
        conf.getString("directory.password"),
        conf.getString("directory.baseDn")
      )

      val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcDriver]("slick", conf), directoryConfig)

      if (email.endsWith("gserviceaccount.com"))
        slickDataSource.inTransaction(dataccess => dataccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(email.replaceFirst("pet-","").replace("@\\S+","")))))
      else
        Future(None)
    }

  private def accessTokenHeaderDirective: Directive1[String] = headerValueByName("OIDC_access_token")

  private def accessTokenExpiresInHeaderDirective: Directive1[String] = headerValueByName("OIDC_CLAIM_expires_in")

  private def emailHeaderDirective: Directive1[String] = headerValueByName("OIDC_CLAIM_email")

  private def userSubjectIdDirective: Directive1[String] = headerValue(extractUniqueId)

  def extractUniqueId: HttpHeader => Option[String] = {hdr:HttpHeader =>
    hdr.name match {
      case "OIDC_CLAIM_sub" => Some(hdr.value)
      case "OIDC_CLAIM_user_id" => Some(hdr.value)
      case _ => None
    }
  }
}
