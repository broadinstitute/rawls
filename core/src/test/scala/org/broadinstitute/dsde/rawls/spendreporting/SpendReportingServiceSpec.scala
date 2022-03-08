package org.broadinstitute.dsde.rawls.spendreporting


import akka.http.scaladsl.model.StatusCodes
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Field, FieldValue, FieldValueList, Schema, StandardSQLTypeName, TableResult}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{MockBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, SamBillingProjectActions, SamResourceAction, SamResourceTypeNames}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{RETURNS_SMART_NULLS, when}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._


class SpendReportingServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with Matchers with MockitoTestUtils {
  val fields: List[Field] = List(
    Field.of("cost", StandardSQLTypeName.STRING),
    Field.of("credits", StandardSQLTypeName.STRING),
    Field.of("currency", StandardSQLTypeName.STRING),
    Field.of("date", StandardSQLTypeName.STRING)
  )
  val firstRow: List[FieldValue] = List(
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // cost
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${DateTime.now().toString}") // date
  )
  val secondRow: List[FieldValue] = List(
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.10111"), // cost
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${DateTime.now().toString}") // date
  )
  val defaultFieldValues: List[FieldValueList] = List(
    FieldValueList.of(firstRow.asJava, fields:_*),
    FieldValueList.of(secondRow.asJava, fields:_*)
  )
  val defaultPage: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, defaultFieldValues.asJava)
  val schema: Schema = Schema.of(fields:_*)
  val defaultTableResult: TableResult = new TableResult(schema, 2, defaultPage)
  val defaultServiceProject: GoogleProject = GoogleProject("project")
  val spendReportingServiceConfig: SpendReportingServiceConfig = SpendReportingServiceConfig("table", 90)

  // Create Spend Reporting Service with Sam and BQ DAOs that mock happy-path responses and return defaultTable. Override Sam and BQ responses as needed
  def createSpendReportingService(
                                   dataSource: SlickDataSource,
                                   samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                   tableResult: TableResult = defaultTableResult
                                 ): SpendReportingService = {
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo))
      .thenReturn(Future.successful(true))
    when(samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject), any[String], mockitoEq(SamBillingProjectActions.readSpendReport), mockitoEq(userInfo)))
      .thenReturn(Future.successful(true))
    val mockServiceFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult))

    new SpendReportingService(userInfo, dataSource, mockServiceFactory.getServiceFromJson("json", defaultServiceProject), samDAO, spendReportingServiceConfig)
  }

  "SpendReportingService" should "unmarshal results from Google" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe "0.10" // sum of costs in defaultTable
  }

  it should "throw an exception when BQ returns zero rows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val emptyPage = new PageImpl[FieldValueList](null, null, List[FieldValueList]().asJava)
    val emptyTableResult = new TableResult(schema, 0, emptyPage)
    val service = createSpendReportingService(dataSource, tableResult = emptyTableResult)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
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

  it should s"throw an exception when date range is larger than ${spendReportingServiceConfig.maxDateRange} days" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, startDate = DateTime.now().minusDays(spendReportingServiceConfig.maxDateRange + 1), endDate = DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "throw an exception if the billing project cannot be found" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(RawlsBillingProjectName("fakeProject"), DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
  }

  it should "throw an exception if the billing project does not have a linked billing account" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)
    val projectName = RawlsBillingProjectName("fakeProject")
    runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, CreationStatuses.Ready, billingAccount = None, None)))

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "throw an exception if BigQuery returns multiple kinds of currencies" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val cadRow = List(
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.10111"),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "CAD"),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${DateTime.now().toString}")
    )

    val internationalPage = new PageImpl[FieldValueList](null, null, (FieldValueList.of(cadRow.asJava, fields:_*) :: defaultFieldValues).asJava)
    val internationalTable = new TableResult(schema, 3, internationalPage)

    val service = createSpendReportingService(dataSource, tableResult = internationalTable)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }
}
