package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RequesterPaysSetupServiceSpec extends FlatSpec with Matchers with MockitoSugar with ScalaFutures {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(1, Seconds)))

  private val userInfo = UserInfo(RawlsUserEmail("user@foo.com"), OAuth2BearerToken("mytoken"), 0, RawlsUserSubjectId("asdfasdfasdf"))

  private def setupServices = {
    val mockBondApiDAO = mock[BondApiDAO]
    val gcsDAO = new MockGoogleServicesDAO("foo")
    val service = new RequesterPaysSetupService(gcsDAO, mockBondApiDAO, "rp/role")
    (mockBondApiDAO, service)
  }

  "getBondProviderServiceAccountEmails" should "get emails" in {
    val expectedEmail = BondServiceAccountEmail("bondSA")
    val (mockBondApiDAO: BondApiDAO, service: RequesterPaysSetupService) = setupServices

    when(mockBondApiDAO.getBondProviders()).thenReturn(Future.successful(List("p1", "p2")))
    when(mockBondApiDAO.getServiceAccountKey("p1", userInfo)).thenReturn(Future.successful(None))
    when(mockBondApiDAO.getServiceAccountKey("p2", userInfo)).thenReturn(Future.successful(Some(BondResponseData(expectedEmail))))

    service.getBondProviderServiceAccountEmails(userInfo).futureValue shouldBe List(expectedEmail)
  }

  it should "get empty list when no providers" in {
    val (mockBondApiDAO: BondApiDAO, service: RequesterPaysSetupService) = setupServices

    when(mockBondApiDAO.getBondProviders()).thenReturn(Future.successful(List.empty))

    service.getBondProviderServiceAccountEmails(userInfo).futureValue shouldBe List.empty
  }

  it should "get empty list when no linked accounts" in {
    val (mockBondApiDAO: BondApiDAO, service: RequesterPaysSetupService) = setupServices

    when(mockBondApiDAO.getBondProviders()).thenReturn(Future.successful(List("p1", "p2")))
    when(mockBondApiDAO.getServiceAccountKey("p1", userInfo)).thenReturn(Future.successful(None))
    when(mockBondApiDAO.getServiceAccountKey("p2", userInfo)).thenReturn(Future.successful(None))

    service.getBondProviderServiceAccountEmails(userInfo).futureValue shouldBe List.empty
  }

  "grantRequesterPaysToLinkedSAs" should "link" in {
    val expectedEmail = BondServiceAccountEmail("bondSA")
    val mockBondApiDAO = mock[BondApiDAO]
    val gcsDAO = new MockGoogleServicesDAO("foo")
    val rpRole = "rp/role"
    val service = new RequesterPaysSetupService(gcsDAO, mockBondApiDAO, rpRole)

    when(mockBondApiDAO.getBondProviders()).thenReturn(Future.successful(List("p1", "p2")))
    when(mockBondApiDAO.getServiceAccountKey("p1", userInfo)).thenReturn(Future.successful(None))
    when(mockBondApiDAO.getServiceAccountKey("p2", userInfo)).thenReturn(Future.successful(Some(BondResponseData(expectedEmail))))

    val projectName = RawlsBillingProjectName("testprojectname")
    service.grantRequesterPaysToLinkedSAs(userInfo, projectName).futureValue shouldBe List(expectedEmail)
    gcsDAO.policies.get(projectName) shouldBe Some(Map(rpRole -> Set("serviceAccount:" + expectedEmail.client_email)))
  }
}
