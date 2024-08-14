package org.broadinstitute.dsde.rawls.methods

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.{jobexec, RawlsException, RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.config.{MethodRepoConfig, WDLParserConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{HttpMethodRepoDAO, MockCromwellSwaggerClient}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.CachingWDLParser
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{
  Agora,
  AgoraMethod,
  Dockstore,
  ErrorReport,
  ErrorReportSource,
  MethodConfiguration,
  RawlsRequestContext,
  RawlsUser,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import slick.lifted.Functions.user

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Success

class MethodConfigurationUtilsSpec extends AnyFlatSpec with Matchers with TestDriverComponent {

  implicit val actorSystem: ActorSystem = ActorSystem()

  val mockServer: RemoteServicesMockServer = RemoteServicesMockServer()
  val methodRepoDAO: HttpMethodRepoDAO = mock[HttpMethodRepoDAO]
  val ctx: RawlsRequestContext = RawlsRequestContext(
    UserInfo(testData.userProjectOwner.userEmail, OAuth2BearerToken("foo"), 0, testData.userProjectOwner.userSubjectId)
  )

  val agoraMethodConf: MethodConfiguration = MethodConfiguration("dsde",
                                                                 "no_input",
                                                                 Some("Sample"),
                                                                 None,
                                                                 Map.empty,
                                                                 Map.empty,
                                                                 AgoraMethod("dsde", "no_input", 1)
  )

  behavior of "MethodConfigurationUtils"

  it should "return results when method when found" in {
    when(methodRepoDAO.getMethod(any(), any())).thenReturn(Future.successful(Option(meth1WDL)))

    val future =
      MethodConfigurationUtils.gatherMethodConfigInputs(ctx, methodRepoDAO, agoraMethodConf, methodConfigResolver)
    Await.ready(future, 30.seconds)
    val Success(result) = future.value.get

    // verify that it returns GatherInputsResult
    result shouldBe a[GatherInputsResult]
    result.missingInputs shouldBe Set("meth1.method1.i1")
  }

  it should "return 404 if method is not found" in {
    when(methodRepoDAO.getMethod(any(), any())).thenReturn(Future.successful(None))

    val exception = intercept[RawlsExceptionWithErrorReport](
      Await.result(
        MethodConfigurationUtils.gatherMethodConfigInputs(ctx, methodRepoDAO, agoraMethodConf, methodConfigResolver),
        30.seconds
      )
    )

    // assert that if method is not found it returns 404
    exception shouldBe a[RawlsExceptionWithErrorReport]
    exception.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
    exception.errorReport.message shouldBe "Cannot get agora://dsde/no_input/1 from method repo."
  }

  it should "return 502 when something unexpected happens" in {
    when(methodRepoDAO.getMethod(any(), any()))
      .thenReturn(Future.failed(new RawlsException("exception thrown for testing purposes")))
    when(methodRepoDAO.errorReportSource).thenReturn(ErrorReportSource("agora"))
    when(methodRepoDAO.toErrorReport(any())).thenCallRealMethod()

    val exception = intercept[RawlsExceptionWithErrorReport](
      Await.result(
        MethodConfigurationUtils.gatherMethodConfigInputs(ctx, methodRepoDAO, agoraMethodConf, methodConfigResolver),
        30.seconds
      )
    )

    // assert that when Future fails, a 502 is returned with wrapped exception
    exception shouldBe a[RawlsExceptionWithErrorReport]
    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
    exception.errorReport.message shouldBe "Unable to query the method repo."
    exception.errorReport.causes.head.message shouldBe "exception thrown for testing purposes"
    exception.errorReport.causes.head.source shouldBe "agora"
  }
}
