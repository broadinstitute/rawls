package org.broadinstitute.dsde.rawls.spendreporting

import java.util

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.google.api.services.bigquery.model.{GetQueryResultsResponse, Job, JobReference, QueryParameter, TableCell, TableRow}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{SamBillingProjectActions, SamResourceAction, SamResourceTypeNames}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{RETURNS_SMART_NULLS, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._


class SpendReportingServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with BeforeAndAfterAll with Matchers with ScalatestRouteTest with MockitoTestUtils {
  val firstRow: util.List[TableCell] = List(
    new TableCell().setV("0.0"), // cost
    new TableCell().setV("0.0"), // credits
    new TableCell().setV("USD"), // currency
    new TableCell().setV(s"${DateTime.now().toString}") // timestamp
  ).asJava
  val secondRow: util.List[TableCell] = List(
    new TableCell().setV("0.1"), // cost
    new TableCell().setV("0.0"), // credits
    new TableCell().setV("USD"), // currency
    new TableCell().setV(s"${DateTime.now().toString}") // timestamp
  ).asJava
  val defaultTable: util.List[TableRow] = List(
    new TableRow().setF(firstRow),
    new TableRow().setF(secondRow)
  ).asJava

  val defaultServiceProject: GoogleProject = GoogleProject("project")

  // Create Spend Reporting Service with Sam and BQ DAOs that mock happy-path responses and return defaultTable. Override Sam and BQ responses as needed
  def createSpendReportingService(
                                   dataSource: SlickDataSource,
                                   samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                   bqDAO: GoogleBigQueryDAO = mock[GoogleBigQueryDAO](RETURNS_SMART_NULLS),
                                   bqTable: util.List[TableRow] = defaultTable
                                 ): SpendReportingService = {
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo))
      .thenReturn(Future.successful(true))
    when(samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject), any[String], mockitoEq(SamBillingProjectActions.readSpendReport), mockitoEq(userInfo)))
      .thenReturn(Future.successful(true))

    when(bqDAO.startParameterizedQuery(mockitoEq(defaultServiceProject), any[String], any[List[QueryParameter]], any[String]))
      .thenReturn(Future.successful(new JobReference()))
    when(bqDAO.getQueryStatus(any[JobReference]))
      .thenReturn(Future.successful(new Job()))
    when(bqDAO.getQueryResult(any[Job]))
      .thenReturn(Future.successful(new GetQueryResultsResponse().setRows(bqTable)))

    new SpendReportingService(userInfo, dataSource, bqDAO, samDAO, "table", defaultServiceProject)
  }

  "SpendReportingService" should "unmarshal results from Google" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val res = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    val reportingResults = res.getOrElse(fail("results not returned"))
    reportingResults.spendSummary.cost shouldBe "0.10" // sum of costs in defaultTable
  }

  it should "tolerate getting a response from BQ with zero rows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val bqDAO = mock[GoogleBigQueryDAO](RETURNS_SMART_NULLS)
    val service = createSpendReportingService(dataSource, bqDAO = bqDAO)

    when(bqDAO.getQueryResult(any[Job]))
      .thenReturn(Future.successful(new GetQueryResultsResponse()))

    Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf) shouldBe None
  }

  it should "throw an exception when user does not have read_spend_report" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val service = createSpendReportingService(dataSource, samDAO = samDAO)

    when(samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject), any[String], mockitoEq(SamBillingProjectActions.readSpendReport), mockitoEq(userInfo)))
      .thenReturn(Future.successful(false))

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)
  }

  it should "throw an exception when user is not in alpha group" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val service = createSpendReportingService(dataSource, samDAO = samDAO)

    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo))
      .thenReturn(Future.successful(false))

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)
  }

  it should "throw an exception when start date is after end date" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, startDate = DateTime.now(), endDate = DateTime.now().minusDays(1)), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "throw an exception when date range is larger than 90 days" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, startDate = DateTime.now().minusDays(91), endDate = DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }
}
