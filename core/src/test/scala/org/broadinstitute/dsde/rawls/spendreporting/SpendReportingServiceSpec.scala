package org.broadinstitute.dsde.rawls.spendreporting


import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
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


class SpendReportingServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with Matchers with MockitoTestUtils {
  val dailyFields: List[Field] = List(
    Field.of("cost", StandardSQLTypeName.STRING),
    Field.of("credits", StandardSQLTypeName.STRING),
    Field.of("currency", StandardSQLTypeName.STRING),
    Field.of("date", StandardSQLTypeName.STRING)
  )
  val firstDailyRow: List[FieldValue] = List(
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // cost
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${DateTime.now().toString}") // date
  )
  val secondDailyRow: List[FieldValue] = List(
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.10111"), // cost
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, s"${DateTime.now().toString}") // date
  )
  val dailyFieldValues: List[FieldValueList] = List(
    FieldValueList.of(firstDailyRow.asJava, dailyFields:_*),
    FieldValueList.of(secondDailyRow.asJava, dailyFields:_*)
  )
  val dailyPage: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, dailyFieldValues.asJava)
  val dailySchema: Schema = Schema.of(dailyFields:_*)
  val dailyTableResult: TableResult = new TableResult(dailySchema, 2, dailyPage)

  val workspaceGoogleProject1 = "project1"
  val workspaceGoogleProject2 = "project2"
  val workspaceFields: List[Field] = List(
    Field.of("cost", StandardSQLTypeName.STRING),
    Field.of("credits", StandardSQLTypeName.STRING),
    Field.of("currency", StandardSQLTypeName.STRING),
    Field.of("googleProjectId", StandardSQLTypeName.STRING)
  )
  val firstWorkspaceRow: List[FieldValue] = List(
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // cost
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, workspaceGoogleProject1) // googleProjectId
  )
  val secondWorkspaceRow: List[FieldValue] = List(
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.10111"), // cost
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"), // credits
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USD"), // currency
    FieldValue.of(FieldValue.Attribute.PRIMITIVE, workspaceGoogleProject2) // googleProjectId
  )
  val workspaceFieldValues: List[FieldValueList] = List(
    FieldValueList.of(firstWorkspaceRow.asJava, workspaceFields:_*),
    FieldValueList.of(secondWorkspaceRow.asJava, workspaceFields:_*)
  )
  val workspacePage: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, workspaceFieldValues.asJava)
  val workspaceSchema: Schema = Schema.of(workspaceFields:_*)
  val workspaceTableResult: TableResult = new TableResult(workspaceSchema, 2, workspacePage)
  val defaultServiceProject: GoogleProject = GoogleProject("project")
  val spendReportingServiceConfig: SpendReportingServiceConfig = SpendReportingServiceConfig("table", 90)

  // Create Spend Reporting Service with Sam and BQ DAOs that mock happy-path responses and return defaultTable. Override Sam and BQ responses as needed
  def createSpendReportingService(
                                   dataSource: SlickDataSource,
                                   samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
                                   tableResult: TableResult = dailyTableResult
                                 ): SpendReportingService = {
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo))
      .thenReturn(Future.successful(true))
    when(samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject), any[String], mockitoEq(SamBillingProjectActions.readSpendReport), mockitoEq(userInfo)))
      .thenReturn(Future.successful(true))
    val mockServiceFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult))

    new SpendReportingService(userInfo, dataSource, mockServiceFactory.getServiceFromJson("json", defaultServiceProject), samDAO, spendReportingServiceConfig)
  }

  "SpendReportingService" should "break down results from Google by day" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource)

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), SpendReportingAggregationKeys.Daily), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe "0.10" // sum of costs in defaultTable
    reportingResults.spendDetails.headOption.getOrElse(fail("daily results not parsed correctly"))
      .aggregationKey shouldBe SpendReportingAggregationKeys.Daily
  }

  it should "break down results from Google by workspace" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = workspaceTableResult)

    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(Workspace(testData.billingProject.projectName.value, "workspace1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator", Map.empty, isLocked = false, WorkspaceVersions.V2, GoogleProjectId(workspaceGoogleProject1), None, None, None, None, WorkspaceShardStates.Sharded, WorkspaceType.RawlsWorkspace)))
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(Workspace(testData.billingProject.projectName.value, "workspace2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator", Map.empty, isLocked = false, WorkspaceVersions.V2, GoogleProjectId(workspaceGoogleProject2), None, None, None, None, WorkspaceShardStates.Sharded, WorkspaceType.RawlsWorkspace)))

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), SpendReportingAggregationKeys.Workspace), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe "0.10" // sum of costs in defaultTable
    val workspaceAggregation = reportingResults.spendDetails.headOption.getOrElse(fail("workspace results not parsed correctly"))

    workspaceAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace
    workspaceAggregation.spendData.headOption.getOrElse(fail("workspace results not parsed correctly")).googleProjectId shouldBe defined
    workspaceAggregation.spendData.headOption.getOrElse(fail("workspace results not parsed correctly")).workspace shouldBe defined
  }

  it should "throw an exception when BQ returns zero rows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val emptyPage = new PageImpl[FieldValueList](null, null, List[FieldValueList]().asJava)
    val emptyTableResult = new TableResult(dailySchema, 0, emptyPage)
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

    val internationalPage = new PageImpl[FieldValueList](null, null, (FieldValueList.of(cadRow.asJava, dailyFields:_*) :: dailyFieldValues).asJava)
    val internationalTable = new TableResult(dailySchema, 3, internationalPage)

    val service = createSpendReportingService(dataSource, tableResult = internationalTable)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }
}
