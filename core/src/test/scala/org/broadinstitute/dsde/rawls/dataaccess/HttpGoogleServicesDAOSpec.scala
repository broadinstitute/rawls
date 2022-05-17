package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO._
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpGoogleServicesDAOSpec extends AnyFlatSpec with Matchers {

  behavior of "handleByOperationIdType"

  def v1Handler(opId: String) = "v1"
  def v2alpha1Handler(opId: String) = "v2alpha1"
  def lifeSciencesHandler(opId: String) = "lifeSciences"
  def defaultHandler(opId: String) = "default"

  val cases: List[(String, String)] = List(
    ("operations/abc", "v1"),
    ("projects/abc/operations/def", "v2alpha1"),
    ("projects/abc/locations/def/operations/ghi", "lifeSciences"),
    ("!!no match!!", "default"),
  )

  cases foreach { case (opId, identification) =>
    it should s"Correctly identify $opId as $identification" in {
      handleByOperationIdType(opId, v1Handler, v2alpha1Handler, lifeSciencesHandler, defaultHandler) should be(identification)
    }
  }

  behavior of "getUserCredential"

  it should "get a credential for a Google user" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"), OAuth2BearerToken("some-token"), 300, RawlsUserSubjectId("193481341723041"), None)
    val cred = getUserCredential(userInfo)
    cred shouldBe defined
    cred.get.getExpiresInSeconds.toInt should (be >= 0 and be <= 300)
  }

  it should "get a credential for a Google user through B2C" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"), OAuth2BearerToken("some-jwt"), 300, RawlsUserSubjectId("704ef594-9669-45f4-b605-82b499065a49"), Some(OAuth2BearerToken("some-token")))
    val cred = getUserCredential(userInfo)
    cred shouldBe defined
    cred.get.getAccessToken shouldBe "some-token"
    cred.get.getExpiresInSeconds.toInt should (be >= 0 and be <= 300)
  }

  it should "not get a credential for an Azure user through B2C" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"), OAuth2BearerToken("some-jwt"), 300, RawlsUserSubjectId("704ef594-9669-45f4-b605-82b499065a49"), None)
    val cred = getUserCredential(userInfo)
    cred shouldBe None
  }
}
