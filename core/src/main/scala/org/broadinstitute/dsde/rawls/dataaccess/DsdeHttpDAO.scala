package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, ResponseEntity, StatusCode}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.HttpClientUtils

import scala.concurrent.{ExecutionContext, Future}

/**
 * Wraps HTTP methods utility in a DAO-like wrapper. Helps interactions with HTTP endpoints within Terra (DSDE being the old name for DSP)
 */
trait DsdeHttpDAO extends LazyLogging {
  implicit protected val system: ActorSystem
  implicit protected val materializer: Materializer
  implicit protected val executionContext: ExecutionContext

  protected def http: HttpExt
  protected def httpClientUtils: HttpClientUtils

  protected def authHeader(userInfo: UserInfo): HttpHeader = authHeader(userInfo.accessToken)
  protected def authHeader(accessToken: OAuth2BearerToken): HttpHeader = Authorization(accessToken)

  protected def executeRequest[T](httpRequest: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] =
    httpClientUtils.executeRequestUnmarshalResponse[T](http, httpRequest)

  protected def executeRequestAsUser[T](
    userInfo: UserInfo
  )(httpRequest: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] =
    httpClientUtils
      .executeRequestUnmarshalResponse[T](http, httpClientUtils.addHeader(httpRequest, authHeader(userInfo)))

  def executeRequestWithToken[T](
    accessToken: OAuth2BearerToken
  )(httpRequest: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] =
    httpClientUtils
      .executeRequestUnmarshalResponse[T](http, httpClientUtils.addHeader(httpRequest, authHeader(accessToken)))

  protected def pipeline[A](userInfo: UserInfo)(implicit um: Unmarshaller[ResponseEntity, A]) =
    executeRequestAsUser[A](userInfo) _

  protected def pipeline[A](implicit um: Unmarshaller[ResponseEntity, A]) = executeRequest[A] _

  protected def when5xx(throwable: Throwable): Boolean = DsdeHttpDAO.when5xx(throwable)

}

object DsdeHttpDAO {

  private def statusCodePredicate(check: StatusCode => Boolean): Throwable => Boolean = {
    case t: RawlsExceptionWithErrorReport => t.errorReport.statusCode.exists(check)
    case _                                => false
  }

  def when5xx: Throwable => Boolean = statusCodePredicate(_.intValue / 100 == 5)
  def whenUnauthorized: Throwable => Boolean = statusCodePredicate(_.intValue == 401)
}
