package org.broadinstitute.dsde.rawls.serviceperimeter

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.google.api.services.accesscontextmanager.v1.model.Operation
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.ServicePerimeterServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, MockGoogleAccessContextManagerDAO}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectNumber, ServicePerimeterName, Workspace}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{RETURNS_SMART_NULLS, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class ServicePerimeterServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with BeforeAndAfterAll with Matchers with ScalatestRouteTest with MockitoTestUtils{
  val defaultConfig = ServicePerimeterServiceConfig(Map(ServicePerimeterName("theGreatBarrier") -> Seq(GoogleProjectNumber("555555"), GoogleProjectNumber("121212")),
    ServicePerimeterName("anotherGoodName") -> Seq(GoogleProjectNumber("777777"), GoogleProjectNumber("343434"))), 1 second, 1 second)

  "ServicePerimeterService" should "attempt to overwrite the service perimeter with correct list of google project numbers" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val googleAccessContextManagerDAO = Mockito.spy(new MockGoogleAccessContextManagerDAO())
    val gcsDAO = Mockito.spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val service = new ServicePerimeterService(dataSource, gcsDAO, defaultConfig)

    val servicePerimeterName: ServicePerimeterName = defaultConfig.staticProjectsInPerimeters.keys.head
    val staticProjectNumbersInPerimeter: Set[String] = defaultConfig.staticProjectsInPerimeters(servicePerimeterName).map(_.value).toSet

    val billingProject1 = testData.testProject1
    val billingProject2 = testData.testProject2
    val billingProjects = Seq(billingProject1, billingProject2)
    val workspacesPerProject = 2

    // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
    // the Billing Projects and therefore in the Perimeter
    val workspacesInPerimeter: Seq[Workspace] = billingProjects.flatMap { bp =>
      runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(bp.copy(servicePerimeter = Option(servicePerimeterName)))))
      val updatedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(bp.projectName)).getOrElse(fail(s"billing project ${bp.projectName} not found"))
      updatedBillingProject.servicePerimeter shouldBe Option(servicePerimeterName)

      (1 to workspacesPerProject).map { n =>
        val workspace = testData.workspace.copy(
          namespace = bp.projectName.value,
          name = s"${bp.projectName.value}Workspace${n}",
          workspaceId = UUID.randomUUID().toString,
          googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString)))
        runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspace))
      }
    }

    Await.result(slickDataSource.inTransaction { dataAccess =>
      service.overwriteGoogleProjectsInPerimeter(servicePerimeterName, dataAccess)
    }, Duration.Inf)

    // Check that we made the call to overwrite the Perimeter exactly once (default) and that the correct perimeter
    // name was specified with the correct list of projects which should include all pre-existing Workspaces within
    // Billing Projects using the same Service Perimeter, all static Google Project Numbers specified by the Config, and
    // the new Google Project Number that we just created
    val existingProjectNumbersInPerimeter = workspacesInPerimeter.map(_.googleProjectNumber.get.value).toSet
    val expectedGoogleProjectNumbers: Set[String] = (existingProjectNumbersInPerimeter ++ staticProjectNumbersInPerimeter)
    val projectNumbersCaptor = captor[Set[String]]
    val servicePerimeterNameCaptor = captor[ServicePerimeterName]

    // verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was called exactly once and capture
    // the arguments passed to it so that we can verify that they were correct
    verify(gcsDAO.accessContextManagerDAO).overwriteProjectsInServicePerimeter(servicePerimeterNameCaptor.capture, projectNumbersCaptor.capture)
    projectNumbersCaptor.getValue should contain theSameElementsAs expectedGoogleProjectNumbers
    servicePerimeterNameCaptor.getValue shouldBe servicePerimeterName
  }

  it should "throw a 500 if the perimeter fails when updating" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val gcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    val acmDAO = mock[AccessContextManagerDAO](RETURNS_SMART_NULLS)
    val operationName = "overwritePerimeterOperation"

    when(gcsDAO.accessContextManagerDAO).thenReturn(acmDAO)
    when(acmDAO.overwriteProjectsInServicePerimeter(any[ServicePerimeterName], any[Set[String]]))
      .thenReturn(Future.successful(new Operation().setName(operationName)))
    when(gcsDAO.pollOperation(OperationId(GoogleApiTypes.AccessContextManagerApi, operationName)))
      .thenReturn(Future.successful(OperationStatus(done = true, Option("I'm bad at overwriting the perimeter"))))

    val service = new ServicePerimeterService(dataSource, gcsDAO, defaultConfig)

    val servicePerimeterName: ServicePerimeterName = defaultConfig.staticProjectsInPerimeters.keys.head
    val billingProject1 = testData.testProject1
    val billingProject2 = testData.testProject2
    val billingProjects = Seq(billingProject1, billingProject2)
    val workspacesPerProject = 2

    // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
    // the Billing Projects and therefore in the Perimeter
    billingProjects.flatMap { bp =>
      runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(bp.copy(servicePerimeter = Option(servicePerimeterName)))))
      val updatedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(bp.projectName)).getOrElse(fail(s"billing project ${bp.projectName} not found"))
      updatedBillingProject.servicePerimeter shouldBe Option(servicePerimeterName)

      (1 to workspacesPerProject).map { n =>
        val workspace = testData.workspace.copy(
          namespace = bp.projectName.value,
          name = s"${bp.projectName.value}Workspace${n}",
          workspaceId = UUID.randomUUID().toString,
          googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString)))
        runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspace))
      }
    }

    val failure = intercept[RawlsExceptionWithErrorReport] {
      Await.result(slickDataSource.inTransaction { dataAccess =>
        service.overwriteGoogleProjectsInPerimeter(servicePerimeterName, dataAccess)
      }, Duration.Inf)
    }
    failure.errorReport.statusCode shouldBe Option(StatusCodes.InternalServerError)
  }
}
