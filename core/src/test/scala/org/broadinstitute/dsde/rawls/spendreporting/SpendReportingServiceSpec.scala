package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.model.StatusCodes
import cats.effect.{IO, Resource}
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{MockBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{model, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

class SpendReportingServiceSpec extends AnyFlatSpecLike with TestDriverComponent with Matchers with MockitoTestUtils {

  object TestData {
    val workspaceGoogleProject1 = "project1"
    val workspaceGoogleProject2 = "project2"
    val workspace1: Workspace = workspace("workspace1", GoogleProjectId(workspaceGoogleProject1))
    val workspace2: Workspace = workspace("workspace2", GoogleProjectId(workspaceGoogleProject2))

    val googleProjectsToWorkspaceNames: Map[GoogleProject, WorkspaceName] = Map(
      GoogleProject(workspaceGoogleProject1) -> workspace1.toWorkspaceName,
      GoogleProject(workspaceGoogleProject2) -> workspace2.toWorkspaceName
    )

    def workspace(
      name: String,
      googleProjectId: GoogleProjectId,
      namespace: String = testData.billingProject.projectName.value,
      workspaceId: String = UUID.randomUUID().toString,
      bucketName: String = "bucketName",
      workflowCollectionName: Option[String] = None,
      attributes: AttributeMap = Map.empty,
      googleProjectNumber: Option[GoogleProjectNumber] = None,
      currentBillingAccountOnGoogleProject: Option[RawlsBillingAccountName] = None,
      billingAccountErrorMessage: Option[String] = None
    ): Workspace = model.Workspace(
      namespace,
      name,
      workspaceId,
      bucketName,
      workflowCollectionName,
      DateTime.now,
      DateTime.now,
      "creator",
      attributes,
      isLocked = false,
      WorkspaceVersions.V2,
      googleProjectId,
      googleProjectNumber,
      currentBillingAccountOnGoogleProject,
      billingAccountErrorMessage,
      None,
      WorkspaceType.RawlsWorkspace
    )

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
      val totalCostRounded: BigDecimal =
        BigDecimal(otherRowCost + computeRowCost + storageRowCost).setScale(2, RoundingMode.HALF_EVEN)

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

      val workspace1OtherRowCost = 204.1025
      val workspace1ComputeRowCost = 50.20
      val workspace2StorageRowCost = 2.5
      val workspace2OtherRowCost = 5.10245

      val workspace1OtherRowCostRounded: BigDecimal =
        BigDecimal(workspace1OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace1ComputeRowCostRounded: BigDecimal =
        BigDecimal(workspace1ComputeRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2StorageRowCostRounded: BigDecimal =
        BigDecimal(workspace2StorageRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2OtherRowCostRounded: BigDecimal =
        BigDecimal(workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val otherTotalCostRounded: BigDecimal =
        BigDecimal(workspace1OtherRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val storageTotalCostRounded: BigDecimal = workspace2StorageRowCostRounded
      val computeTotalCostRounded: BigDecimal = workspace1ComputeRowCostRounded

      val workspace1TotalCostRounded: BigDecimal =
        BigDecimal(workspace1OtherRowCost + workspace1ComputeRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2TotalCostRounded: BigDecimal =
        BigDecimal(workspace2StorageRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val totalCostRounded: BigDecimal = BigDecimal(
        workspace1OtherRowCost + workspace1ComputeRowCost + workspace2StorageRowCost + workspace2OtherRowCost
      ).setScale(2, RoundingMode.HALF_EVEN)

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

  def createTableResult(data: List[Map[String, String]]): TableResult = {
    val fields = data.flatMap(_.keySet).distinct.map(field => Field.of(field, StandardSQLTypeName.STRING))
    val values: List[FieldValueList] = data.map { row =>
      val rowValues = row.values.toList.map { value =>
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)
      }
      FieldValueList.of(rowValues.asJava, fields: _*)
    }
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, values.asJava)
    new TableResult(Schema.of(fields: _*), values.length, page)
  }

  val defaultServiceProject: GoogleProject = GoogleProject("project")
  val spendReportingServiceConfig: SpendReportingServiceConfig = SpendReportingServiceConfig(
    "fakeTable",
    "fakeTimePartitionColumn",
    90
  )

  // Create Spend Reporting Service with Sam and BQ DAOs that mock happy-path responses and return SpendReportingTestData.Workspace.tableResult. Override Sam and BQ responses as needed
  def createSpendReportingService(
    dataSource: SlickDataSource,
    samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
    tableResult: TableResult = TestData.Workspace.tableResult
  ): SpendReportingService = {
    when(
      samDAO.userHasAction(SamResourceTypeNames.managedGroup,
                           "Alpha_Spend_Report_Users",
                           SamResourceAction("use"),
                           userInfo
      )
    )
      .thenReturn(Future.successful(true))
    when(
      samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject),
                           any[String],
                           mockitoEq(SamBillingProjectActions.readSpendReport),
                           mockitoEq(userInfo)
      )
    )
      .thenReturn(Future.successful(true))
    val mockServiceFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult))

    new SpendReportingService(testContext,
                              dataSource,
                              mockServiceFactory.getServiceFromJson("json", defaultServiceProject),
                              samDAO,
                              spendReportingServiceConfig
    )
  }

  "SpendReportingService" should "break down results from Google by day" in {
    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.Daily.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      Map(),
      Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily))
    )
    reportingResults.spendSummary.cost shouldBe TestData.Daily.totalCostRounded.toString
    reportingResults.spendDetails.head.aggregationKey shouldBe SpendReportingAggregationKeys.Daily
    reportingResults.spendDetails.head.spendData.foreach { spendForDay =>
      spendForDay.startTime match {
        case Some(date) if date.toLocalDate.equals(TestData.Daily.firstRowDate.toLocalDate) =>
          spendForDay.cost shouldBe TestData.Daily.firstRowCostRounded.toString
        case Some(date) if date.toLocalDate.equals(TestData.Daily.secondRowDate.toLocalDate) =>
          spendForDay.cost shouldBe TestData.Daily.secondRowCostRounded.toString
        case _ => fail(s"unexpected day found in spend results - $spendForDay")
      }
    }
  }

  it should "break down results from Google by workspace" in {
    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.Workspace.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      TestData.googleProjectsToWorkspaceNames,
      Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))
    )

    reportingResults.spendSummary.cost shouldBe TestData.Workspace.totalCostRounded.toString
    val workspaceAggregation =
      reportingResults.spendDetails.headOption.getOrElse(fail("workspace results not parsed correctly"))

    workspaceAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace

    workspaceAggregation.spendData.map { spendForWorkspace =>
      val workspaceGoogleProject =
        spendForWorkspace.googleProjectId.getOrElse(fail("workspace results not parsed correctly")).value

      if (workspaceGoogleProject.equals(TestData.workspace1.googleProjectId.value)) {
        spendForWorkspace.cost shouldBe TestData.Workspace.firstRowCostRounded.toString
      } else if (workspaceGoogleProject.equals(TestData.workspace2.googleProjectId.value)) {
        spendForWorkspace.cost shouldBe TestData.Workspace.secondRowCostRounded.toString
      } else {
        fail(s"unexpected workspace found in spend results - $spendForWorkspace")
      }
    }
    workspaceAggregation.spendData.headOption
      .getOrElse(fail("workspace results not parsed correctly"))
      .googleProjectId shouldBe defined
    workspaceAggregation.spendData.headOption
      .getOrElse(fail("workspace results not parsed correctly"))
      .workspace shouldBe defined
  }

  it should "break down results from Google by Terra spend category" in {

    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.Category.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      Map(),
      Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category))
    )

    reportingResults.spendSummary.cost shouldBe TestData.Category.totalCostRounded.toString
    val categoryAggregation = reportingResults.spendDetails.headOption.get

    categoryAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Category

    verifyCategoryAggregation(
      categoryAggregation,
      expectedComputeCost = TestData.Category.computeRowCostRounded,
      expectedStorageCost = TestData.Category.storageRowCostRounded,
      expectedOtherCost = TestData.Category.otherRowCostRounded
    )
  }

  it should "should return only summary data if aggregation keys are omitted" in {
    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.Workspace.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      Map(),
      Set.empty
    )
    reportingResults.spendSummary.cost shouldBe TestData.Workspace.totalCostRounded.toString
    reportingResults.spendDetails shouldBe empty
  }

  it should "support sub-aggregations" in {
    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.SubAggregation.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      TestData.googleProjectsToWorkspaceNames,
      Set(
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace,
                                            Option(SpendReportingAggregationKeys.Category)
        )
      )
    )
    withClue("total cost was incorrect") {
      reportingResults.spendSummary.cost shouldBe TestData.SubAggregation.totalCostRounded.toString
    }
    verifyWorkspaceCategorySubAggregation(reportingResults.spendDetails.headOption.get)
  }

  it should "support multiple aggregations" in {
    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.SubAggregation.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      Map(
        GoogleProject(TestData.workspace1.googleProjectId.value) -> TestData.workspace1.toWorkspaceName,
        GoogleProject(TestData.workspace2.googleProjectId.value) -> TestData.workspace2.toWorkspaceName
      ),
      Set(
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace,
                                            Option(SpendReportingAggregationKeys.Category)
        ),
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category)
      )
    )

    withClue("total cost was incorrect") {
      reportingResults.spendSummary.cost shouldBe TestData.SubAggregation.totalCostRounded.toString
    }

    reportingResults.spendDetails.map {
      case workspaceAggregation @ SpendReportingAggregation(SpendReportingAggregationKeys.Workspace, _) =>
        verifyWorkspaceCategorySubAggregation(workspaceAggregation)
      case categoryAggregation @ SpendReportingAggregation(SpendReportingAggregationKeys.Category, _) =>
        verifyCategoryAggregation(
          categoryAggregation,
          expectedComputeCost = TestData.SubAggregation.computeTotalCostRounded,
          expectedStorageCost = TestData.SubAggregation.storageTotalCostRounded,
          expectedOtherCost = TestData.SubAggregation.otherTotalCostRounded
        )
      case _ => fail("unexpected aggregation key found")
    }
  }

  private def verifyCategoryAggregation(categoryAggregation: SpendReportingAggregation,
                                        expectedComputeCost: BigDecimal,
                                        expectedStorageCost: BigDecimal,
                                        expectedOtherCost: BigDecimal
  ) =
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

  private def verifyWorkspaceCategorySubAggregation(topLevelAggregation: SpendReportingAggregation) = {
    topLevelAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace

    topLevelAggregation.spendData.map { spendData =>
      val workspaceGoogleProject = spendData.googleProjectId.getOrElse(fail("spend results not parsed correctly")).value
      val subAggregation = spendData.subAggregation.getOrElse(fail("spend results not parsed correctly"))
      subAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Category

      if (workspaceGoogleProject.equals(TestData.workspace1.googleProjectId.value)) {
        withClue(s"cost for ${TestData.workspace1.toWorkspaceName} was incorrect") {
          spendData.cost shouldBe TestData.SubAggregation.workspace1TotalCostRounded.toString
        }
        subAggregation.spendData.map { subAggregatedSpendData =>
          if (subAggregatedSpendData.category == Option(TerraSpendCategories.Compute)) {
            withClue(
              s"${subAggregatedSpendData.category} category cost for ${TestData.workspace1.toWorkspaceName} was incorrect"
            ) {
              subAggregatedSpendData.cost shouldBe TestData.SubAggregation.workspace1ComputeRowCostRounded.toString
            }
          } else if (subAggregatedSpendData.category == Option(TerraSpendCategories.Other)) {
            withClue(
              s"${subAggregatedSpendData.category} category cost for ${TestData.workspace1.toWorkspaceName} was incorrect"
            ) {
              subAggregatedSpendData.cost shouldBe TestData.SubAggregation.workspace1OtherRowCostRounded.toString
            }
          } else {
            fail(s"unexpected category found in spend results - $subAggregatedSpendData")
          }
        }

      } else if (workspaceGoogleProject.equals(TestData.workspace2.googleProjectId.value)) {
        withClue(s"cost for ${TestData.workspace2.toWorkspaceName} was incorrect") {
          spendData.cost shouldBe TestData.SubAggregation.workspace2TotalCostRounded.toString
        }
        subAggregation.spendData.map { subAggregatedSpendData =>
          if (subAggregatedSpendData.category == Option(TerraSpendCategories.Storage)) {
            withClue(
              s"${subAggregatedSpendData.category} category cost for ${TestData.workspace2.toWorkspaceName} was incorrect"
            ) {
              subAggregatedSpendData.cost shouldBe TestData.SubAggregation.workspace2StorageRowCostRounded.toString
            }
          } else if (subAggregatedSpendData.category == Option(TerraSpendCategories.Other)) {
            withClue(
              s"${subAggregatedSpendData.category} category cost for ${TestData.workspace2.toWorkspaceName} was incorrect"
            ) {
              subAggregatedSpendData.cost shouldBe TestData.SubAggregation.workspace2OtherRowCostRounded.toString
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
      Await.result(service.getSpendForBillingProject(testData.billingProject.projectName,
                                                     DateTime.now().minusDays(1),
                                                     DateTime.now()
                   ),
                   Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
  }

  it should "throw an exception when user does not have read_spend_report" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    when(
      samDAO.userHasAction(
        mockitoEq(SamResourceTypeNames.billingProject),
        any(),
        mockitoEq(SamBillingProjectActions.readSpendReport),
        mockitoEq(userInfo)
      )
    ).thenReturn(Future.successful(false))
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      samDAO,
      spendReportingServiceConfig
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.getSpendForBillingProject(
          testData.billingProject.projectName,
          DateTime.now().minusDays(1),
          DateTime.now()
        ),
        Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)
  }

  it should "throw an exception when user is not in alpha group" in {
    val samDAO = mock[SamDAO]
    when(
      samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.managedGroup),
        ArgumentMatchers.eq("Alpha_Spend_Report_Users"),
        ArgumentMatchers.eq(SamResourceAction("use")),
        ArgumentMatchers.eq(testContext.userInfo)
      )
    ).thenReturn(Future.successful(false))

    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      samDAO,
      spendReportingServiceConfig
    )
    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.requireAlphaUser()(fail("action was run without an exception being thrown")), Duration.Inf)
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)
  }

  it should "throw an exception when start date is after end date" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[SamDAO],
      spendReportingServiceConfig
    )
    val startDate = DateTime.now()
    val endDate = DateTime.now().minusDays(1)
    val e = intercept[RawlsExceptionWithErrorReport](service.validateReportParameters(startDate, endDate))
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should s"throw an exception when date range is larger than ${spendReportingServiceConfig.maxDateRange} days" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[SamDAO],
      spendReportingServiceConfig
    )
    val startDate = DateTime.now().minusDays(spendReportingServiceConfig.maxDateRange + 1)
    val endDate = DateTime.now()
    val e = intercept[RawlsExceptionWithErrorReport](service.validateReportParameters(startDate, endDate))
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not throw an exception when validating max start and end date range" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[SamDAO],
      spendReportingServiceConfig
    )
    val startDate = DateTime.now().minusDays(spendReportingServiceConfig.maxDateRange)
    val endDate = DateTime.now()
    service.validateReportParameters(startDate, endDate)
  }

  it should "throw an exception if the billing project cannot be found" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      val service = createSpendReportingService(dataSource)

      val e = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          service.getSpendForBillingProject(
            RawlsBillingProjectName("fakeProject"),
            DateTime.now().minusDays(1),
            DateTime.now()
          ),
          Duration.Inf
        )
      }
      e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
  }

  it should "throw an exception if the billing project does not have a linked billing account" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      val service = createSpendReportingService(dataSource)
      val projectName = RawlsBillingProjectName("fakeProject")
      runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery.create(
          RawlsBillingProject(projectName, CreationStatuses.Ready, billingAccount = None, None)
        )
      )

      val e = intercept[RawlsExceptionWithErrorReport] {
        Await.result(service.getSpendForBillingProject(projectName, DateTime.now().minusDays(1), DateTime.now()),
                     Duration.Inf
        )
      }
      e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "throw an exception if BigQuery returns multiple kinds of currencies" in {
    val table = createTableResult(
      List(
        Map(
          "cost" -> "0.10111",
          "credits" -> "0.0",
          "currency" -> "CAD",
          "date" -> DateTime.now().toString
        ),
        Map(
          "cost" -> "0.10111",
          "credits" -> "0.0",
          "currency" -> "USD",
          "date" -> DateTime.now().toString
        )
      )
    ).getValues.asScala.toList

    val e = intercept[RawlsExceptionWithErrorReport] {
      SpendReportingService.extractSpendReportingResults(
        table,
        DateTime.now().minusDays(1),
        DateTime.now(),
        Map(),
        Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily))
      )
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }

  it should "throw an exception if BigQuery results include an unexpected Google project" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      val badRow = Map(
        "cost" -> "0.10111",
        "credits" -> "0.0",
        "currency" -> "USD",
        "googleProjectId" -> "fakeProject"
      )

      val badTable = createTableResult(badRow :: TestData.Workspace.table)

      val service = createSpendReportingService(dataSource, tableResult = badTable)
      runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(TestData.workspace1))
      runAndWait(dataSource.dataAccess.workspaceQuery.createOrUpdate(TestData.workspace2))

      val e = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          service.getSpendForBillingProject(
            testData.billingProject.projectName,
            DateTime.now().minusDays(1),
            DateTime.now(),
            Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))
          ),
          Duration.Inf
        )
      }
      e.errorReport.statusCode shouldBe Option(StatusCodes.BadGateway)
  }

  it should "use the default time partition name if a custom name is not specified" in {
    val expectedQuery =
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
    assertResult(expectedQuery) {
      val service = new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
        mock[SamDAO],
        spendReportingServiceConfig
      )
      service.getQuery(
        Set(SpendReportingAggregationKeys.Workspace, SpendReportingAggregationKeys.Daily),
        "fakeTable",
        None
      )
    }
  }

  it should "use the custom time partition column name if specified" in {
    val expectedQuery =
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
    assertResult(expectedQuery) {
      val service = new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
        mock[SamDAO],
        spendReportingServiceConfig
      )
      service.getQuery(
        Set(SpendReportingAggregationKeys.Workspace, SpendReportingAggregationKeys.Daily),
        "fakeTable",
        Some("custom_time_partition")
      )
    }
  }

}
