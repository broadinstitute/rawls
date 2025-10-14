package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.unmarshalling.Unmarshaller
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.joda.time.DateTime

/**
 * Directives to get user information.
 */
trait UserInfoDirectives {
  def requireUserInfo(otelContext: Option[Context] = None): Directive1[UserInfo]

  implicit def dateTimeUnmarshaller: Unmarshaller[String, DateTime] = Unmarshaller.strict(DateTime.parse)

}
