package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import io.opencensus.trace.Span
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}

trait MockUserInfoDirectives extends UserInfoDirectives {
  protected def userInfo = UserInfo(RawlsUserEmail("owner-access"),
                                    OAuth2BearerToken("token"),
                                    123,
                                    RawlsUserSubjectId("123456789876543212345")
  )

  def requireUserInfo(otelContext: Option[Context]): Directive1[UserInfo] =
    provide(userInfo)
}
