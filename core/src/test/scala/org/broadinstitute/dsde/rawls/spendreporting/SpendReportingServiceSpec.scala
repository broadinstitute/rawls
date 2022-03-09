package org.broadinstitute.dsde.rawls.spendreporting


import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{MockBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
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
import scala.math.BigDecimal.RoundingMode


class SpendReportingServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with Matchers with MockitoTestUtils {
  object SpendReportingTestData {
    object Daily {
      val firstRowCost = 2.4
      val secondRowCost = 0.10111
      val firstRowCostRounded: BigDecimal = BigDecimal(firstRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val secondRowCostRounded: BigDecimal = BigDecimal(secondRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val totalCostRounded: BigDecimal = BigDecimal(firstRowCost + secondRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val firstRowDate = DateTime.now().minusDays(1)
      val secondRowDate = DateTime.now()
      val fields: List[Field]= List(
        Field.of("cost", StandardSQLTypeName.STRING),
        Field.of("credits", StandardSQLTypeName.STRING),
        Field.of("currency", StandardSQLTypeName.STRING),
        Field.of("date", StandardSQLTypeName.STRING)
      )
      val firstRow: List[FieldValue] = List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"$firstRowCost"), // cost
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${firstRowDate.toString}") // date
      )
      val secondRow: List[FieldValue] = List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"$secondRowCost"), // cost
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${secondRowDate.toString}") // date
      )
      val fieldValues: List[FieldValueList] = List(
        FieldValueList.of(firstRow.asJava, fields:_*),
        FieldValueList.of(secondRow.asJava, fields:_*)
      )
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, fieldValues.asJava)
      val schema: Schema = Schema.of(fields:_*)
      val tableResult: TableResult = new TableResult(schema, 2, page)
    }

    object Workspace {
      val workspaceGoogleProject1 = "project1"
      val workspaceGoogleProject2 = "project2"
      val workspace1: Workspace = model.Workspace(testData.billingProject.projectName.value, "workspace1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator", Map.empty, isLocked = false, WorkspaceVersions.V2, GoogleProjectId(workspaceGoogleProject1), None, None, None, None, WorkspaceType.RawlsWorkspace)
      val workspace2: Workspace = model.Workspace(testData.billingProject.projectName.value, "workspace2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator", Map.empty, isLocked = false, WorkspaceVersions.V2, GoogleProjectId(workspaceGoogleProject2), None, None, None, None, WorkspaceType.RawlsWorkspace)

      val firstRowCost = 100.582
      val secondRowCost = 0.10111
      val firstRowCostRounded: BigDecimal = BigDecimal(firstRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val secondRowCostRounded: BigDecimal = BigDecimal(secondRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val totalCostRounded: BigDecimal = BigDecimal(firstRowCost + secondRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val fields: List[Field] = List(
        Field.of("cost", StandardSQLTypeName.STRING),
        Field.of("credits", StandardSQLTypeName.STRING),
        Field.of("currency", StandardSQLTypeName.STRING),
        Field.of("googleProjectId", StandardSQLTypeName.STRING)
      )
      val firstRow: List[FieldValue] = List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"$firstRowCost"), // cost
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, workspaceGoogleProject1) // googleProjectId
      )
      val secondRow: List[FieldValue] = List(
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"$secondRowCost"), // cost
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, workspaceGoogleProject2) // googleProjectId
      )
      val fieldValues: List[FieldValueList] = List(
        FieldValueList.of(firstRow.asJava, fields:_*),
        FieldValueList.of(secondRow.asJava, fields:_*)
      )
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, fieldValues.asJava)
      val schema: Schema = Schema.of(fields:_*)
      val tableResult: TableResult = new TableResult(schema, 2, page)
    }
  }

  val defaultServiceProject: GoogleProject = GoogleProject("project")
  val spendReportingServiceConfig: SpendReportingServiceConfig = SpendReportingServiceConfig("table", 90)

  // Create Spend Reporting Service with Sam and BQ DAOs that mock happy-path responses and return SpendReportingTestData.Workspace.tableResult. Override Sam and BQ responses as needed
  def createSpendReportingService(
                                   dataSource: SlickDataSource,
                                   samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                   tableResult: TableResult = SpendReportingTestData.Workspace.tableResult
                                 ): SpendReportingService = {
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo))
      .thenReturn(Future.successful(true))
    when(samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject), any[String], mockitoEq(SamBillingProjectActions.readSpendReport), mockitoEq(userInfo)))
      .thenReturn(Future.successful(true))
    val mockServiceFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult))

    new SpendReportingService(userInfo, dataSource, mockServiceFactory.getServiceFromJson("json", defaultServiceProject), samDAO, spendReportingServiceConfig)
  }

  "SpendReportingService" should "break down results from Google by day" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.Daily.tableResult)

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Option(SpendReportingAggregationKeys.Daily)), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe SpendReportingTestData.Daily.totalCostRounded.toString
    val dailyAggregation = reportingResults.spendDetails.headOption.getOrElse(fail("daily results not parsed correctly"))
    dailyAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Daily

    dailyAggregation.spendData.map { spendForDay =>
      if (spendForDay.startTime.toLocalDate.equals(SpendReportingTestData.Daily.firstRowDate.toLocalDate)) {
        spendForDay.cost shouldBe SpendReportingTestData.Daily.firstRowCostRounded.toString
      } else if (spendForDay.startTime.toLocalDate.equals(SpendReportingTestData.Daily.secondRowDate.toLocalDate)) {
        spendForDay.cost shouldBe SpendReportingTestData.Daily.secondRowCostRounded.toString
      } else {
        fail(s"unexpected day found in spend results - $spendForDay")
      }
    }
  }

  it should "break down results from Google by workspace" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.Workspace.tableResult)

    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace1))
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace2))

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Option(SpendReportingAggregationKeys.Workspace)), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe SpendReportingTestData.Workspace.totalCostRounded.toString
    val workspaceAggregation = reportingResults.spendDetails.headOption.getOrElse(fail("workspace results not parsed correctly"))

    workspaceAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace

    workspaceAggregation.spendData.map { spendForWorkspace =>
      val workspaceGoogleProject = spendForWorkspace.googleProjectId.getOrElse(fail("workspace results not parsed correctly")).value

      if (workspaceGoogleProject.equals(SpendReportingTestData.Workspace.workspace1.googleProjectId.value)) {
        spendForWorkspace.cost shouldBe SpendReportingTestData.Workspace.firstRowCostRounded.toString
      } else if (workspaceGoogleProject.equals(SpendReportingTestData.Workspace.workspace2.googleProjectId.value)) {
        spendForWorkspace.cost shouldBe SpendReportingTestData.Workspace.secondRowCostRounded.toString
      } else {
        fail(s"unexpected workspace found in spend results - $spendForWorkspace")
      }
    }
    workspaceAggregation.spendData.headOption.getOrElse(fail("workspace results not parsed correctly")).googleProjectId shouldBe defined
    workspaceAggregation.spendData.headOption.getOrElse(fail("workspace results not parsed correctly")).workspace shouldBe defined
  }

  it should "return summary data only if aggregation key is omitted" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.Workspace.tableResult)

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), None), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe SpendReportingTestData.Workspace.totalCostRounded.toString
    reportingResults.spendDetails shouldBe empty
  }

  it should "throw an exception when BQ returns zero rows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val emptyPage = new PageImpl[FieldValueList](null, null, List[FieldValueList]().asJava)
    val emptyTableResult = new TableResult(SpendReportingTestData.Daily.schema, 0, emptyPage)
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

    val internationalPage = new PageImpl[FieldValueList](null, null, (FieldValueList.of(cadRow.asJava, SpendReportingTestData.Daily.fields:_*) :: SpendReportingTestData.Daily.fieldValues).asJava)
    val internationalTable = new TableResult(SpendReportingTestData.Daily.schema, 3, internationalPage)

    val service = createSpendReportingService(dataSource, tableResult = internationalTable)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }

  it should "throw an exception if BigQuery results include an unexpected Google project" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val badRow = List(
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.10111"),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "fakeProject")
    )

    val badPage = new PageImpl[FieldValueList](null, null, (FieldValueList.of(badRow.asJava, SpendReportingTestData.Workspace.fields:_*) :: SpendReportingTestData.Workspace.fieldValues).asJava)
    val badTable = new TableResult(SpendReportingTestData.Workspace.schema, 3, badPage)

    val service = createSpendReportingService(dataSource, tableResult = badTable)
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace1))
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace2))

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Option(SpendReportingAggregationKeys.Workspace)), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }
}
