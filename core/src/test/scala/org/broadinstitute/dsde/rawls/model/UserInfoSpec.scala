package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserInfoSpec extends AnyFlatSpec with Matchers {

  "UserInfo.isB2C" should "return false for Google users" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"),
                            OAuth2BearerToken("some-token"),
                            300,
                            RawlsUserSubjectId("193481341723041"),
                            None
    )
    userInfo.isB2C shouldBe false
  }

  it should "return true for B2C users" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"),
                            OAuth2BearerToken("some-token"),
                            300,
                            RawlsUserSubjectId("704ef594-9669-45f4-b605-82b499065a49"),
                            None
    )
    userInfo.isB2C shouldBe true

    val userInfo2 = UserInfo(RawlsUserEmail("fake@email.com"),
                             OAuth2BearerToken("some-token"),
                             300,
                             RawlsUserSubjectId("another-user"),
                             Some(OAuth2BearerToken("some-google-token"))
    )
    userInfo2.isB2C shouldBe true
  }
}
