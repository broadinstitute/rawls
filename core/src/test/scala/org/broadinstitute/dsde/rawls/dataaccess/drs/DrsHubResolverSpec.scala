package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{HttpRequest, ResponseEntity}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.ActorMaterializer
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class DrsHubResolverSpec extends AnyFlatSpecLike with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val mockActorSystem: ActorSystem = ActorSystem("MockDrsHubResolver")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockDrsHubResolver = mock[DrsHubResolver]
  val mockUserInfo: UserInfo = {
    UserInfo(RawlsUserEmail("mr_bean@gmail.com"), OAuth2BearerToken("foo"), 0, RawlsUserSubjectId("abc123"))
  }

    behavior of "DrsHubResolver"

  it should "retry at least once on 5xx response from drshub" in {
    val drsUrl = "drs://jade.datarepo-dev.broadinstitute.org/foo"
    val fakeResponse = DrsHubMinimalResponse(Option(ServiceAccountPayload(Option(ServiceAccountEmail("foo")))))
    val fakeError = RawlsExceptionWithErrorReport(ErrorReport(statusCode = InternalServerError, message = "fake error"))
    when(mockDrsHubResolver.executeRequestWithToken[DrsHubMinimalResponse](any[OAuth2BearerToken])(any[HttpRequest])(any[Unmarshaller[ResponseEntity, DrsHubMinimalResponse]]))
      .thenReturn(Future.failed(fakeError), Future.successful(fakeResponse))

    val actualResultFuture = mockDrsHubResolver.drsServiceAccountEmail(
      drsUrl = drsUrl,
      userInfo = mockUserInfo
    )

    Await.result(actualResultFuture, 1 second)

    verify(mockDrsHubResolver, times(2)).drsServiceAccountEmail(drsUrl, mockUserInfo)
  }
}
