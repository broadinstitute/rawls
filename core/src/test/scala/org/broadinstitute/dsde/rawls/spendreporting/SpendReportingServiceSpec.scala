package org.broadinstitute.dsde.rawls.spendreporting


import akka.http.scaladsl.model.StatusCodes
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{MockBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{RETURNS_SMART_NULLS, when}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
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
      val firstRowDate: DateTime = DateTime.now().minusDays(1)
      val secondRowDate: DateTime = DateTime.now()

      val table: List[Map[String, String]] = List(
        Map(
          "cost" -> s"$firstRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "date" -> firstRowDate.toString
        ),
        Map(
          "cost" -> s"$secondRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "date" -> secondRowDate.toString
        )
      )

      val tableResult: TableResult = createTableResult(table)
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

      val table: List[Map[String, String]] = List(
        Map(
          "cost" -> s"$firstRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "googleProjectId" -> workspaceGoogleProject1
        ),
        Map(
          "cost" -> s"$secondRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "googleProjectId" -> workspaceGoogleProject2
        )
      )

      val tableResult: TableResult = createTableResult(table)
    }

    object Category {
      val otherRowCost = 204.1025
      val computeRowCost = 50.20
      val storageRowCost = 2.5
      val otherRowCostRounded: BigDecimal = BigDecimal(otherRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val computeRowCostRounded: BigDecimal = BigDecimal(computeRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val storageRowCostRounded: BigDecimal = BigDecimal(storageRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val totalCostRounded: BigDecimal = BigDecimal(otherRowCost + computeRowCost + storageRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val table: List[Map[String, String]] = List(
        Map(
          "cost" -> s"$otherRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud DNS"
        ),
        Map(
          "cost" -> s"$computeRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Kubernetes Engine"
        ),
        Map(
          "cost" -> s"$storageRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud Storage"
        )
      )
      val tableResult: TableResult = createTableResult(table)
    }

    object SubAggregation {
      val workspaceGoogleProject1 = "project1"
      val workspaceGoogleProject2 = "project2"
      val workspace1: Workspace = model.Workspace(testData.billingProject.projectName.value, "workspace1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator", Map.empty, isLocked = false, WorkspaceVersions.V2, GoogleProjectId(workspaceGoogleProject1), None, None, None, None, WorkspaceType.RawlsWorkspace)
      val workspace2: Workspace = model.Workspace(testData.billingProject.projectName.value, "workspace2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator", Map.empty, isLocked = false, WorkspaceVersions.V2, GoogleProjectId(workspaceGoogleProject2), None, None, None, None, WorkspaceType.RawlsWorkspace)

      val workspace1OtherRowCost = 204.1025
      val workspace1ComputeRowCost = 50.20
      val workspace2StorageRowCost = 2.5
      val workspace2OtherRowCost = 5.10245

      val workspace1OtherRowCostRounded: BigDecimal = BigDecimal(workspace1OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace1ComputeRowCostRounded: BigDecimal = BigDecimal(workspace1ComputeRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2StorageRowCostRounded: BigDecimal = BigDecimal(workspace2StorageRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2OtherRowCostRounded: BigDecimal = BigDecimal(workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val otherTotalCostRounded: BigDecimal = BigDecimal(workspace1OtherRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val storageTotalCostRounded: BigDecimal = workspace2StorageRowCostRounded
      val computeTotalCostRounded: BigDecimal = workspace1ComputeRowCostRounded

      val workspace1TotalCostRounded: BigDecimal = BigDecimal(workspace1OtherRowCost + workspace1ComputeRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2TotalCostRounded: BigDecimal = BigDecimal(workspace2StorageRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val totalCostRounded: BigDecimal = BigDecimal(workspace1OtherRowCost + workspace1ComputeRowCost + workspace2StorageRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val table: List[Map[String, String]] = List(
        Map(
          "cost" -> s"$workspace1OtherRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud DNS",
          "googleProjectId" -> workspaceGoogleProject1
        ),
        Map(
          "cost" -> s"$workspace1ComputeRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Kubernetes Engine",
          "googleProjectId" -> workspaceGoogleProject1
        ),
        Map(
          "cost" -> s"$workspace2StorageRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud Storage",
          "googleProjectId" -> workspaceGoogleProject2
        ),
        Map(
          "cost" -> s"$workspace2OtherRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud Logging",
          "googleProjectId" -> workspaceGoogleProject2
        )
      )

      val tableResult: TableResult = createTableResult(table)
    }
  }

  def createTableResult(values: List[Map[String, String]]): TableResult = {
    val rawSchemas: List[Set[String]] = values.map(_.keySet)
    val rawFields: List[String] = if (rawSchemas.nonEmpty) {
      rawSchemas.reduce { (x, y) =>
        if (x.equals(y)) {
          x
        } else {
          fail(s"inconsistent schema found when comparing rows $x and $y")
        }
      }.toList
    } else {
      List.empty
    }
    val fields: List[Field] = rawFields.map { field =>
      Field.of(field, StandardSQLTypeName.STRING)
    }
    val schema: Schema = Schema.of(fields:_*)

    val fieldValues: List[List[FieldValue]] = values.map { row =>
      row.values.toList.map { value =>
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)
      }
    }
    val fieldValueLists: List[FieldValueList] = fieldValues.map { row =>
      FieldValueList.of(row.asJava, fields:_*)
    }
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, fieldValueLists.asJava)

    new TableResult(schema, fieldValueLists.length, page)
  }

  val defaultServiceProject: GoogleProject = GoogleProject("project")
  val spendReportingServiceConfig: SpendReportingServiceConfig = SpendReportingServiceConfig(
    "fakeTable", "fakeTimePartitionColumn", 90
  )

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

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily))), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe SpendReportingTestData.Daily.totalCostRounded.toString
    val dailyAggregation = reportingResults.spendDetails.headOption.getOrElse(fail("daily results not parsed correctly"))
    dailyAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Daily

    dailyAggregation.spendData.map { spendForDay =>
      if (spendForDay.startTime.getOrElse(fail("daily results not parsed correctly"))
        .toLocalDate.equals(SpendReportingTestData.Daily.firstRowDate.toLocalDate)) {
        spendForDay.cost shouldBe SpendReportingTestData.Daily.firstRowCostRounded.toString
      } else if (spendForDay.startTime.getOrElse(fail("daily results not parsed correctly"))
        .toLocalDate.equals(SpendReportingTestData.Daily.secondRowDate.toLocalDate)) {
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

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))), Duration.Inf)
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

  it should "break down results from Google by Terra spend category" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.Category.tableResult)

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category))), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe SpendReportingTestData.Category.totalCostRounded.toString
    val categoryAggregation = reportingResults.spendDetails.headOption.getOrElse(fail("workspace results not parsed correctly"))

    categoryAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Category

    verifyCategoryAggregation(
      categoryAggregation,
      expectedComputeCost = SpendReportingTestData.Category.computeRowCostRounded,
      expectedStorageCost = SpendReportingTestData.Category.storageRowCostRounded,
      expectedOtherCost = SpendReportingTestData.Category.otherRowCostRounded
    )
  }

  it should "return summary data only if aggregation key is omitted" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.Workspace.tableResult)

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set.empty), Duration.Inf)
    reportingResults.spendSummary.cost shouldBe SpendReportingTestData.Workspace.totalCostRounded.toString
    reportingResults.spendDetails shouldBe empty
  }

  it should "support sub-aggregations" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.SubAggregation.tableResult)

    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace1))
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace2))

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace, Option(SpendReportingAggregationKeys.Category)))), Duration.Inf)
    val topLevelAggregation = reportingResults.spendDetails.headOption.getOrElse(fail("spend results not parsed correctly"))

    withClue("total cost was incorrect"){
      reportingResults.spendSummary.cost shouldBe SpendReportingTestData.SubAggregation.totalCostRounded.toString
    }
    verifyWorkspaceCategorySubAggregation(topLevelAggregation)
  }

  it should "support multiple aggregations" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val service = createSpendReportingService(dataSource, tableResult = SpendReportingTestData.SubAggregation.tableResult)

    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace1))
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace2))

    val reportingResults = Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace, Option(SpendReportingAggregationKeys.Category)), SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category))), Duration.Inf)
    withClue("total cost was incorrect") {
      reportingResults.spendSummary.cost shouldBe SpendReportingTestData.SubAggregation.totalCostRounded.toString
    }

    reportingResults.spendDetails.map {
      case workspaceAggregation@SpendReportingAggregation(SpendReportingAggregationKeys.Workspace, _) => {
        verifyWorkspaceCategorySubAggregation(workspaceAggregation)
      }
      case categoryAggregation@SpendReportingAggregation(SpendReportingAggregationKeys.Category, _) => {
        verifyCategoryAggregation(
          categoryAggregation,
          expectedComputeCost = SpendReportingTestData.SubAggregation.computeTotalCostRounded,
          expectedStorageCost = SpendReportingTestData.SubAggregation.storageTotalCostRounded,
          expectedOtherCost = SpendReportingTestData.SubAggregation.otherTotalCostRounded
        )
      }
      case _ => fail("unexpected aggregation key found")
    }
  }

  private def verifyCategoryAggregation(categoryAggregation: SpendReportingAggregation, expectedComputeCost: BigDecimal, expectedStorageCost: BigDecimal, expectedOtherCost: BigDecimal) = {
    categoryAggregation.spendData.map { spendDataForCategory =>
      val category = spendDataForCategory.category.getOrElse(fail("results not parsed correctly"))

      withClue(s"total $category cost was incorrect") {
        if (category.equals(TerraSpendCategories.Compute)) {
          spendDataForCategory.cost shouldBe expectedComputeCost.toString
        } else if (category.equals(TerraSpendCategories.Storage)) {
          spendDataForCategory.cost shouldBe expectedStorageCost.toString
        } else if (category.equals(TerraSpendCategories.Other)) {
          spendDataForCategory.cost shouldBe expectedOtherCost.toString
        } else {
          fail(s"unexpected category found in spend results - $spendDataForCategory")
        }
      }
    }
  }

  private def verifyWorkspaceCategorySubAggregation(topLevelAggregation: SpendReportingAggregation) = {
    topLevelAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace

    topLevelAggregation.spendData.map { spendData =>
      val workspaceGoogleProject = spendData.googleProjectId.getOrElse(fail("spend results not parsed correctly")).value
      val subAggregation = spendData.subAggregation.getOrElse(fail("spend results not parsed correctly"))
      subAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Category

      if (workspaceGoogleProject.equals(SpendReportingTestData.SubAggregation.workspace1.googleProjectId.value)) {
        withClue(s"cost for ${SpendReportingTestData.SubAggregation.workspace1.toWorkspaceName} was incorrect") {
          spendData.cost shouldBe SpendReportingTestData.SubAggregation.workspace1TotalCostRounded.toString
        }
        subAggregation.spendData.map { subAggregatedSpendData =>
          if (subAggregatedSpendData.category == Option(TerraSpendCategories.Compute)) {
            withClue(s"${subAggregatedSpendData.category} category cost for ${SpendReportingTestData.SubAggregation.workspace1.toWorkspaceName} was incorrect") {
              subAggregatedSpendData.cost shouldBe SpendReportingTestData.SubAggregation.workspace1ComputeRowCostRounded.toString
            }
          } else if (subAggregatedSpendData.category == Option(TerraSpendCategories.Other)) {
            withClue(s"${subAggregatedSpendData.category} category cost for ${SpendReportingTestData.SubAggregation.workspace1.toWorkspaceName} was incorrect") {
              subAggregatedSpendData.cost shouldBe SpendReportingTestData.SubAggregation.workspace1OtherRowCostRounded.toString
            }
          } else {
            fail(s"unexpected category found in spend results - $subAggregatedSpendData")
          }
        }

      } else if (workspaceGoogleProject.equals(SpendReportingTestData.SubAggregation.workspace2.googleProjectId.value)) {
        withClue(s"cost for ${SpendReportingTestData.SubAggregation.workspace2.toWorkspaceName} was incorrect") {
          spendData.cost shouldBe SpendReportingTestData.SubAggregation.workspace2TotalCostRounded.toString
        }
        subAggregation.spendData.map { subAggregatedSpendData =>
          if (subAggregatedSpendData.category == Option(TerraSpendCategories.Storage)) {
            withClue(s"${subAggregatedSpendData.category} category cost for ${SpendReportingTestData.SubAggregation.workspace2.toWorkspaceName} was incorrect") {
              subAggregatedSpendData.cost shouldBe SpendReportingTestData.SubAggregation.workspace2StorageRowCostRounded.toString
            }
          } else if (subAggregatedSpendData.category == Option(TerraSpendCategories.Other)) {
            withClue(s"${subAggregatedSpendData.category} category cost for ${SpendReportingTestData.SubAggregation.workspace2.toWorkspaceName} was incorrect") {
              subAggregatedSpendData.cost shouldBe SpendReportingTestData.SubAggregation.workspace2OtherRowCostRounded.toString
            }
          } else {
            fail(s"unexpected category found in spend results - $subAggregatedSpendData")
          }
        }
      } else {
        fail(s"unexpected workspace found in spend results - $spendData")
      }
    }
  }

  it should "throw an exception when BQ returns zero rows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val emptyTableResult = createTableResult(List[Map[String, String]]())
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
    val cadRow = Map(
      "cost" -> "0.10111",
      "credits" -> "0.0",
      "currency" -> "CAD",
      "date" -> DateTime.now().toString
    )

    val internationalTable = createTableResult(cadRow :: SpendReportingTestData.Daily.table)

    val service = createSpendReportingService(dataSource, tableResult = internationalTable)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now()), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }

  it should "throw an exception if BigQuery results include an unexpected Google project" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val badRow = Map(
      "cost" -> "0.10111",
      "credits" -> "0.0",
      "currency" -> "USD",
      "googleProjectId" -> "fakeProject"
    )

    val badTable = createTableResult(badRow :: SpendReportingTestData.Workspace.table)

    val service = createSpendReportingService(dataSource, tableResult = badTable)
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace1))
    runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(SpendReportingTestData.Workspace.workspace2))

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName, DateTime.now().minusDays(1), DateTime.now(), Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }

  it should "use the custom time partition column name if specified" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val expectedNoCustom =
      s"""
         | SELECT
         |  SUM(cost) as cost,
         |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
         |  currency , project.id as googleProjectId, DATE(_PARTITIONTIME) as date
         | FROM `fakeTable`
         | WHERE billing_account_id = @billingAccountId
         | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
         | AND project.id in UNNEST(@projects)
         | GROUP BY currency , googleProjectId, date
         |""".stripMargin
    assertResult(expectedNoCustom) {
      val service = createSpendReportingService(dataSource)
      service.getQuery(
        Set(SpendReportingAggregationKeys.Workspace, SpendReportingAggregationKeys.Daily), "fakeTable", None
      )
    }

    val expectedCustom =
      s"""
         | SELECT
         |  SUM(cost) as cost,
         |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
         |  currency , project.id as googleProjectId, DATE(custom_time_partition) as date
         | FROM `fakeTable`
         | WHERE billing_account_id = @billingAccountId
         | AND custom_time_partition BETWEEN @startDate AND @endDate
         | AND project.id in UNNEST(@projects)
         | GROUP BY currency , googleProjectId, date
         |""".stripMargin
    assertResult(expectedCustom) {
      val service = createSpendReportingService(dataSource)
      service.getQuery(
        Set(SpendReportingAggregationKeys.Workspace, SpendReportingAggregationKeys.Daily), "fakeTable",
        Some("custom_time_partition")
      )
    }
  }
}
