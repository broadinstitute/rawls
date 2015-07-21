package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.ExecutionContext

trait StandardOpenAmDirectives extends OpenAmDirectives {
  val rawlsOpenAmClient: RawlsOpenAmClient

  def userInfoFromCookie(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
    implicit val ec = magnet.value
    tokenFromCookie flatMap userInfoFromToken
  }

  private def tokenFromCookie: Directive1[String] = cookie("iPlanetDirectoryPro") map { _.content }

  private def userInfoFromToken(token: String)(implicit ec: ExecutionContext): Directive1[UserInfo] = {
    val userInfoFuture = for {
      id <- rawlsOpenAmClient.lookupIdFromSession(token)
      userInfo <- rawlsOpenAmClient.lookupUserInfo(token, id.id, id.realm)
    } yield UserInfo(userInfo.mail.head, token)
    onSuccess(userInfoFuture)
  }
}
