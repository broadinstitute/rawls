package org.broadinstitute.dsde.rawls.model

import spray.http.OAuth2BearerToken

/**
 * Created by dvoet on 7/21/15.
 */
case class UserInfo(userEmail: String, accessToken: OAuth2BearerToken, accessTokenExpiresIn: Long, userSubjectId: String)
