package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.openam.UserInfoResponseProtocol._
import org.broadinstitute.dsde.vault.common.openam.OpenAMClient
import org.broadinstitute.dsde.vault.common.openam.OpenAMResponse._
import spray.client.pipelining._
import spray.http.{MediaTypes, HttpEntity, Uri}
import spray.httpx.SprayJsonSupport._

import scala.concurrent.Future

/**
 * Wrapper/replacement for vault-common's OpenAMClient that has the features we want
 * (Rawls config, email extraction, etc)
 */
class RawlsOpenAmClient(deploymentUri: String, username: String, password: String, realm: Option[String],
                        authIndexType: Option[String], authIndexValue: Option[String]) {

  def this(config: RawlsOpenAmConfig) {
    this(config.deploymentUri, config.username, config.password, config.realm, config.authIndexType, config.authIndexValue)
  }

  val baseClient = OpenAMClient
  import system.dispatcher
  implicit val system = baseClient.system

  def authenticate: Future[AuthenticateResponse] = {
    baseClient.authenticate(
      deploymentUri,
      username,
      password,
      realm,
      authIndexType,
      authIndexValue
    )
  }

  def lookupIdFromSession(token: String): Future[IdFromSessionResponse] = {
    val uri = Uri(s"${deploymentUri}/json/users").withQuery("_action" -> "idFromSession")
    val pipeline = addToken(token) ~> sendReceive ~> unmarshal[IdFromSessionResponse]
    pipeline(Post(uri, HttpEntity(MediaTypes.`application/json`, """{}""")))
  }

  def lookupUserInfo(token: String, id: String, realm: Option[String]): Future[UserInfoResponse] = {
    val uri = Uri(s"${deploymentUri}/json${realm.getOrElse("")}/users/$id").withQuery("_fields" -> "username,cn,mail")
    val pipeline = addToken(token) ~> sendReceive ~> unmarshal[UserInfoResponse]
    pipeline(Get(uri))
  }

  private def addToken(token: String) = addHeader("iPlanetDirectoryPro", token)
}
