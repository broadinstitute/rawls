package org.broadinstitute.dsde.rawls.model

import spray.http.HttpCookie

/**
 * Created by dvoet on 7/21/15.
 */
case class UserInfo(userId: String, authCookie: HttpCookie)
