package org.broadinstitute.dsde.rawls.model

import org.joda.time.DateTime

/**
 * Created by dvoet on 10/27/15.
 */
case class UserRefreshToken(refreshToken: String)
case class UserRefreshTokenDate(refreshTokenUpdatedDate: DateTime)

object UserJsonSupport extends JsonSupport {
  implicit val UserRefreshTokenFormat = jsonFormat1(UserRefreshToken)
  implicit val UserRefreshTokenDateFormat = jsonFormat1(UserRefreshTokenDate)
}