package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{MockSamDAO, TestData}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import spray.http._

import scala.concurrent.ExecutionContext

class PetSASpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO, httpSamDAO: SamDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with StandardUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: RawlsUser = RawlsUser(userInfo))(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO, new MockSamDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  it should "switch to User Account when Accessed from pet SA" in withMinimalTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>
      val petSA = UserInfo(RawlsUserEmail("pet-123456789876543212345@gserviceaccount.com"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876"))
      runAndWait(dataSource.dataAccess.rawlsUserQuery.createUser(testData.userOwner))
      Post("/user") ~> addHeader("OIDC_access_token", petSA.accessToken.value) ~> addHeader("OIDC_CLAIM_expires_in", petSA.accessTokenExpiresIn.toString) ~> addHeader("OIDC_CLAIM_email", petSA.userEmail.value) ~> addHeader("OIDC_CLAIM_user_id", petSA.userSubjectId.value) ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
    }
  }

  val usersTestData = new TestData {
    import driver.api._

    val userOwner = RawlsUser(userInfo)
    val userUser = RawlsUser(UserInfo(RawlsUserEmail("user"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userNoAccess = RawlsUser(UserInfo(RawlsUserEmail("no-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.createUser(userOwner),
        rawlsUserQuery.createUser(userUser),
        rawlsUserQuery.createUser(userNoAccess)
      )
    }
  }
}
