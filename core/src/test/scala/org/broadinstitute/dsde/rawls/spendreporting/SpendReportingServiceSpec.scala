package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.SpendReportingAggregation.AggregationKeyEnum
import bio.terra.profile.model.SpendReportingForDateRange.CategoryEnum
import bio.terra.profile.model.{
  CloudPlatform => BpmCloudPlatform,
  ProfileModel,
  SpendReport => SpendReportBPM,
  SpendReportingAggregation => SpendReportingAggregationBPM,
  SpendReportingForDateRange => SpendReportingForDateRangeBPM
}
import cats.effect.{IO, Resource}
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import org.broadinstitute.dsde.rawls.billing.{
  BillingProfileManagerDAO,
  BillingRepository,
  BpmAzureSpendReportApiException
}
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{SpendReportingAggregationKeys, _}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.rawls.{model, RawlsException, RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.workbench.client.sam.model.FilteredFlatResource
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Mockito}
import org.scalatest.RecoverMethods.recoverToExceptionIf
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.{Date, UUID}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

class SpendReportingServiceSpec extends AnyFlatSpecLike with Matchers with MockitoTestUtils with SprayJsonSupport {

  implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo = UserInfo(RawlsUserEmail("owner-access"),
                                    OAuth2BearerToken("token"),
                                    123,
                                    RawlsUserSubjectId("123456789876543212345")
  )
  val wsName: WorkspaceName = WorkspaceName("myNamespace", "myWorkspace")

  val billingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("fakeBillingAcct")

  val billingProject: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                RawlsBillingProjectName(wsName.namespace),
                                                                CreationStatuses.Ready,
                                                                Option(billingAccountName),
                                                                None
  )

  val mockWorkspaceServiceConstructor: RawlsRequestContext => WorkspaceService = {
    lazy val mockWorkspaceService: WorkspaceService = mock[WorkspaceService]
    _ => mockWorkspaceService
  }

  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)
  object TestData {
    val workspace1: Workspace = workspace("workspace1", GoogleProjectId("project1"))
    val workspace2: Workspace = workspace("workspace2", GoogleProjectId("project2"))

    val googleProjectsToWorkspaceNames: Map[GoogleProjectId, WorkspaceName] = Map(
      workspace1.googleProjectId -> workspace1.toWorkspaceName,
      workspace2.googleProjectId -> workspace2.toWorkspaceName
    )

    def workspace(
      name: String,
      googleProjectId: GoogleProjectId,
      version: WorkspaceVersions.WorkspaceVersion = WorkspaceVersions.V2,
      namespace: String = RawlsBillingProjectName(wsName.namespace).value,
      workspaceId: String = UUID.randomUUID().toString,
      bucketName: String = "bucketName",
      workflowCollectionName: Option[String] = None,
      attributes: AttributeMap = Map.empty,
      googleProjectNumber: Option[GoogleProjectNumber] = None,
      currentBillingAccountOnGoogleProject: Option[RawlsBillingAccountName] = None,
      errorMessage: Option[String] = None
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
      workspaceVersion = version,
      googleProjectId,
      googleProjectNumber,
      currentBillingAccountOnGoogleProject,
      errorMessage,
      None,
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
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
          "googleProjectId" -> workspace1.googleProjectId.value
        ),
        Map(
          "cost" -> s"$secondRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "googleProjectId" -> workspace2.googleProjectId.value
        )
      )

      val tableResult: TableResult = createTableResult(table)
    }

    object Category {
      val otherRowCost = 204.1025
      val computeRowCost = 50.20
      val storageRowCost = 2.5
      val otherRowCostRounded: String = BigDecimal(otherRowCost).setScale(2, RoundingMode.HALF_EVEN).toString
      val computeRowCostRounded: String = BigDecimal(computeRowCost).setScale(2, RoundingMode.HALF_EVEN).toString
      val storageRowCostRounded: String = BigDecimal(storageRowCost).setScale(2, RoundingMode.HALF_EVEN).toString
      val totalCostRounded: String =
        BigDecimal(otherRowCost + computeRowCost + storageRowCost).setScale(2, RoundingMode.HALF_EVEN).toString

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

      val otherTotalCostRounded: String =
        BigDecimal(workspace1OtherRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN).toString()
      val storageTotalCostRounded: String = workspace2StorageRowCostRounded.toString
      val computeTotalCostRounded: String = workspace1ComputeRowCostRounded.toString

      val workspace1TotalCostRounded: BigDecimal =
        BigDecimal(workspace1OtherRowCost + workspace1ComputeRowCost).setScale(2, RoundingMode.HALF_EVEN)
      val workspace2TotalCostRounded: BigDecimal =
        BigDecimal(workspace2StorageRowCost + workspace2OtherRowCost).setScale(2, RoundingMode.HALF_EVEN)

      val totalCostRounded: String = BigDecimal(
        workspace1OtherRowCost + workspace1ComputeRowCost + workspace2StorageRowCost + workspace2OtherRowCost
      ).setScale(2, RoundingMode.HALF_EVEN).toString()

      val table: List[Map[String, String]] = List(
        Map(
          "cost" -> s"$workspace1OtherRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud DNS",
          "googleProjectId" -> workspace1.googleProjectId.value
        ),
        Map(
          "cost" -> s"$workspace1ComputeRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Kubernetes Engine",
          "googleProjectId" -> workspace1.googleProjectId.value
        ),
        Map(
          "cost" -> s"$workspace2StorageRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud Storage",
          "googleProjectId" -> workspace2.googleProjectId.value
        ),
        Map(
          "cost" -> s"$workspace2OtherRowCost",
          "credits" -> "0.0",
          "currency" -> "USD",
          "service" -> "Cloud Logging",
          "googleProjectId" -> workspace2.googleProjectId.value
        )
      )

      val tableResult: TableResult = createTableResult(table)
    }

    object BpmSpendReport {
      def spendData(from: DateTime,
                    to: DateTime,
                    currency: String,
                    costData: Map[String, BigDecimal]
      ): SpendReportBPM = {
        // create spend reporting items based on costData
        val spendDataList = costData
          .map(costKvp =>
            new SpendReportingForDateRangeBPM()
              .cost(costKvp._2.toString())
              .credits("0") /*credits is always 0 in case of Azure*/
              .category(CategoryEnum.fromValue(costKvp._1))
              .currency(currency)
              .startTime(from.toString(ISODateTimeFormat.date()))
              .endTime(to.toString(ISODateTimeFormat.date()))
          )
          .asJavaCollection
          .stream()
          .toList
        val spendReportingAggregation =
          new SpendReportingAggregationBPM()
            .aggregationKey(AggregationKeyEnum.CATEGORY)
            .spendData(spendDataList)
        val spendSummary = new SpendReportingForDateRangeBPM()
          .cost(costData.values.sum.toString())
          .credits("0")
          .currency("USD")
          .startTime(from.toString(ISODateTimeFormat.date()))
          .endTime(to.toString(ISODateTimeFormat.date()))

        new SpendReportBPM()
          .spendDetails(java.util.List.of(spendReportingAggregation))
          .spendSummary(spendSummary)
      }
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
    90,
    "test.rawls"
  )

  def mockBigQuery(
    data: List[Map[String, String]],
    stats: JobStatistics.QueryStatistics = mock[JobStatistics.QueryStatistics](RETURNS_SMART_NULLS)
  ): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    val bigQueryService = mock[GoogleBigQueryService[IO]](RETURNS_SMART_NULLS)
    val job = mock[Job]
    when(job.getQueryResults(any())).thenReturn(createTableResult(data))
    when(job.getStatistics).thenReturn(stats)
    when(job.waitFor()).thenReturn(job)
    when(bigQueryService.runJob(any(), any())).thenReturn(IO(job))
    Resource.pure[IO, GoogleBigQueryService[IO]](bigQueryService)
  }

  "SpendReportingService.extractSpendReportingResults" should "break down results from Google by day" in {
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

    reportingResults.spendSummary.cost shouldBe TestData.Category.totalCostRounded
    val categoryAggregation = reportingResults.spendDetails.headOption.get
    categoryAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Category
    verifyCategoryAggregation(
      categoryAggregation,
      expectedCompute = TestData.Category.computeRowCostRounded,
      expectedStorage = TestData.Category.storageRowCostRounded,
      expectedOther = TestData.Category.otherRowCostRounded
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

    reportingResults.spendSummary.cost shouldBe TestData.SubAggregation.totalCostRounded
    verifyWorkspaceCategorySubAggregation(reportingResults.spendDetails.headOption.get)
  }

  it should "support multiple aggregations" in {
    val reportingResults = SpendReportingService.extractSpendReportingResults(
      TestData.SubAggregation.tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      Map(
        TestData.workspace1.googleProjectId -> TestData.workspace1.toWorkspaceName,
        TestData.workspace2.googleProjectId -> TestData.workspace2.toWorkspaceName
      ),
      Set(
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace,
                                            Option(SpendReportingAggregationKeys.Category)
        ),
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category)
      )
    )

    reportingResults.spendSummary.cost shouldBe TestData.SubAggregation.totalCostRounded

    reportingResults.spendDetails.map {
      case workspaceAggregation @ SpendReportingAggregation(SpendReportingAggregationKeys.Workspace, _) =>
        verifyWorkspaceCategorySubAggregation(workspaceAggregation)
      case categoryAggregation @ SpendReportingAggregation(SpendReportingAggregationKeys.Category, _) =>
        verifyCategoryAggregation(
          categoryAggregation,
          expectedCompute = TestData.SubAggregation.computeTotalCostRounded,
          expectedStorage = TestData.SubAggregation.storageTotalCostRounded,
          expectedOther = TestData.SubAggregation.otherTotalCostRounded
        )
      case _ => fail("unexpected aggregation key found")
    }
  }

  def verifyCategoryAggregation(
    aggregation: SpendReportingAggregation,
    expectedCompute: String,
    expectedStorage: String,
    expectedOther: String
  ): Unit =
    aggregation.spendData.foreach { spendDataForCategory =>
      val category = spendDataForCategory.category.getOrElse(fail("results not parsed correctly"))

      withClue(s"total $category cost was incorrect") {
        if (category.equals(TerraSpendCategories.Compute)) {
          spendDataForCategory.cost shouldBe expectedCompute
        } else if (category.equals(TerraSpendCategories.Storage)) {
          spendDataForCategory.cost shouldBe expectedStorage
        } else if (category.equals(TerraSpendCategories.Other)) {
          spendDataForCategory.cost shouldBe expectedOther
        } else {
          fail(s"unexpected category found in spend results - $spendDataForCategory")
        }
      }
    }

  def verifyWorkspaceCategorySubAggregation(topLevelAggregation: SpendReportingAggregation): Unit = {
    topLevelAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace

    topLevelAggregation.spendData.foreach { spendData =>
      val workspaceGoogleProject = spendData.googleProjectId.get.value
      val subAggregation = spendData.subAggregation.get
      subAggregation.aggregationKey shouldBe SpendReportingAggregationKeys.Category

      if (workspaceGoogleProject.equals(TestData.workspace1.googleProjectId.value)) {
        spendData.cost shouldBe TestData.SubAggregation.workspace1TotalCostRounded.toString
        subAggregation.spendData.foreach { data =>
          data.category match {
            case Some(TerraSpendCategories.Compute) =>
              data.cost shouldBe TestData.SubAggregation.workspace1ComputeRowCostRounded.toString
            case Some(TerraSpendCategories.Other) =>
              data.cost shouldBe TestData.SubAggregation.workspace1OtherRowCostRounded.toString
            case _ => fail(s"unexpected category found in spend results - $data")
          }
        }

      } else if (workspaceGoogleProject.equals(TestData.workspace2.googleProjectId.value)) {
        spendData.cost shouldBe TestData.SubAggregation.workspace2TotalCostRounded.toString
        subAggregation.spendData.foreach { data =>
          data.category match {
            case Some(TerraSpendCategories.Storage) =>
              data.cost shouldBe TestData.SubAggregation.workspace2StorageRowCostRounded.toString
            case Some(TerraSpendCategories.Other) =>
              data.cost shouldBe TestData.SubAggregation.workspace2OtherRowCostRounded.toString
            case Some(_) => fail(s"unexpected category found in spend results - $data")
            case None    => fail(s"no category found in spend results - $data")
          }
        }
      } else {
        fail(s"unexpected workspace found in spend results - $spendData")
      }
    }
  }

  it should "throw an exception if the query result contains multiple kinds of currencies" in {
    val table = createTableResult(
      List(
        Map("cost" -> "0.10111", "credits" -> "0.0", "currency" -> "CAD", "date" -> DateTime.now().toString),
        Map("cost" -> "0.10111", "credits" -> "0.0", "currency" -> "USD", "date" -> DateTime.now().toString)
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
    e.errorReport.statusCode shouldBe Option(StatusCodes.InternalServerError)
  }

  "extractCrossBillingProjectSpendReportingResults" should "break down results by workspace and category" in {
    val credits1 = 0.0
    val credits2 = 3.012
    val credits3 = 1.12345
    val storageCostWs1 = 100.582
    val otherCostWs1 = 0.10111
    val totalCostWs1 = storageCostWs1 + otherCostWs1
    val storageCostRoundedWs1: BigDecimal = BigDecimal(storageCostWs1).setScale(2, RoundingMode.HALF_EVEN)
    val otherCostRoundedWs1: BigDecimal = BigDecimal(otherCostWs1).setScale(2, RoundingMode.HALF_EVEN)
    val totalCostRoundedWs1: BigDecimal = BigDecimal(totalCostWs1).setScale(2, RoundingMode.HALF_EVEN)
    val storageCostWs2 = 20.145
    val computeCostWs2 = 150.4033
    val totalCostWs2 = storageCostWs2 + computeCostWs2
    val storageCostRoundedWs2: BigDecimal = BigDecimal(storageCostWs2).setScale(2, RoundingMode.HALF_EVEN)
    val computeCostRoundedWs2: BigDecimal = BigDecimal(computeCostWs2).setScale(2, RoundingMode.HALF_EVEN)
    val totalCostRoundedWs2: BigDecimal =
      BigDecimal(totalCostWs2).setScale(2, RoundingMode.HALF_EVEN)

    val computeCostWs3 = 1111.222
    val otherCostWs3 = 0.02
    val totalCostWs3 = otherCostWs3 + computeCostWs3
    val computeCostRoundedWs3: BigDecimal = BigDecimal(computeCostWs3).setScale(2, RoundingMode.HALF_EVEN)
    val otherCostRoundedWs3: BigDecimal = BigDecimal(otherCostWs3).setScale(2, RoundingMode.HALF_EVEN)
    val totalCostRoundedWs3: BigDecimal = BigDecimal(totalCostWs3).setScale(2, RoundingMode.HALF_EVEN)

    val table: List[Map[String, String]] = List(
      Map(
        "storage_cost" -> s"$storageCostWs1",
        "compute_cost" -> "0.0",
        "other_cost" -> s"$otherCostWs1",
        "total_cost" -> s"$totalCostWs1",
        "currency" -> "USD",
        "project_id" -> "workspace1ProjectId",
        "project_name" -> "terra-billing-project1",
        "storage_credits" -> s"$credits1",
        "compute_credits" -> "0.0",
        "other_credits" -> "0.0"
      ),
      Map(
        "storage_cost" -> s"$storageCostWs2",
        "compute_cost" -> s"$computeCostWs2",
        "other_cost" -> "0.0",
        "total_cost" -> s"$totalCostWs2",
        "project_id" -> "workspace2ProjectId",
        "project_name" -> "terra-billing-project1",
        "currency" -> "USD",
        "compute_credits" -> s"$credits2",
        "storage_credits" -> "0.0",
        "other_credits" -> "0.0"
      ),
      Map(
        "storage_cost" -> "0.0",
        "compute_cost" -> s"$computeCostWs3",
        "other_cost" -> s"$otherCostWs3",
        "project_id" -> "workspace3ProjectId",
        "project_name" -> "terra-billing-project2",
        "total_cost" -> s"$totalCostWs3",
        "currency" -> "USD",
        "other_credits" -> s"$credits3",
        "compute_credits" -> "0.0",
        "storage_credits" -> "0.0"
      )
    )

    val tableResult: TableResult = createTableResult(table)

    val reportingResults = SpendReportingService.extractCrossBillingProjectSpendReportingResults(
      tableResult.getValues.asScala.toList,
      DateTime.now().minusDays(1),
      DateTime.now(),
      Map(
        GoogleProjectId("workspace1ProjectId") -> WorkspaceName("workspace1", "namespace1"),
        GoogleProjectId("workspace2ProjectId") -> WorkspaceName("workspace2", "namespace1"),
        GoogleProjectId("workspace3ProjectId") -> WorkspaceName("workspace3", "namespace2")
      )
    )
    val spendDetails = reportingResults.spendDetails
    // We have 3 workspaces
    spendDetails.length shouldBe 3

    // Workspace 1
    spendDetails.head.aggregationKey shouldBe SpendReportingAggregationKeys.Workspace
    val ws1SpendData = spendDetails.head.spendData
    ws1SpendData.length shouldBe 1
    verifyWorkspaceSpendData(ws1SpendData.head,
                             totalCostRoundedWs1,
                             BigDecimal(0.00).setScale(2, RoundingMode.HALF_EVEN),
                             storageCostRoundedWs1,
                             otherCostRoundedWs1
    )

    // Workspace 2
    spendDetails(1).aggregationKey shouldBe SpendReportingAggregationKeys.Workspace
    val ws2SpendData = spendDetails(1).spendData
    ws2SpendData.length shouldBe 1
    verifyWorkspaceSpendData(ws2SpendData.head,
                             totalCostRoundedWs2,
                             computeCostRoundedWs2,
                             storageCostRoundedWs2,
                             BigDecimal(0.00).setScale(2, RoundingMode.HALF_EVEN)
    )

    // Workspace 3
    spendDetails(2).aggregationKey shouldBe SpendReportingAggregationKeys.Workspace
    val ws3SpendData = spendDetails(2).spendData
    ws3SpendData.length shouldBe 1
    verifyWorkspaceSpendData(ws3SpendData.head,
                             totalCostRoundedWs3,
                             computeCostRoundedWs3,
                             BigDecimal(0.00).setScale(2, RoundingMode.HALF_EVEN),
                             otherCostRoundedWs3
    )

  }
  def verifyWorkspaceSpendData(actualSpendData: SpendReportingForDateRange,
                               expectedTotal: BigDecimal,
                               expectedCompute: BigDecimal,
                               expectedStorage: BigDecimal,
                               expectedOther: BigDecimal
  ): Unit = {
    actualSpendData.cost shouldBe expectedTotal.toString
    val aggSub = actualSpendData.subAggregation.get
    aggSub.aggregationKey shouldBe SpendReportingAggregationKeys.Category
    verifyCategoricalSpendData(aggSub.spendData, expectedCompute, expectedStorage, expectedOther)
  }

  def verifyCategoricalSpendData(actualSpendData: Seq[SpendReportingForDateRange],
                                 expectedCompute: BigDecimal,
                                 expectedStorage: BigDecimal,
                                 expectedOther: BigDecimal
  ): Unit = {
    actualSpendData.length shouldBe 3
    actualSpendData.foreach { spendData =>
      spendData.category match {
        case Some(TerraSpendCategories.Other) =>
          spendData.cost shouldBe expectedOther.toString
        case Some(TerraSpendCategories.Compute) =>
          spendData.cost shouldBe expectedCompute.toString
        case Some(TerraSpendCategories.Storage) =>
          spendData.cost shouldBe expectedStorage.toString
        case _ => fail("Unexpected category")
      }
    }
  }

  "getSpendForGCPBillingProject" should "throw an exception when BQ returns zero rows" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    when(billingRepository.getBillingProject(any())).thenReturn(Future.successful(Option.apply(billingProject)))

    val bigQueryService = mockBigQuery(List[Map[String, String]]())

    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        bigQueryService,
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )
    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    doReturn(Future.successful(billingProjectSpendExport)).when(service).getSpendExportConfiguration(any())
    doReturn(Future.successful(TestData.googleProjectsToWorkspaceNames))
      .when(service)
      .getSpendReportableWorkspaceGoogleProjects(any())

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.getSpendForGCPBillingProject(
          RawlsBillingProjectName(""),
          DateTime.now().minusDays(1),
          DateTime.now(),
          Set.empty
        ),
        Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
  }

  it should "throw an exception if the billing project cannot be found" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    val dataSource = mock[SlickDataSource]
    when(dataSource.inTransaction[Option[BillingProjectSpendExport]](any(), any())).thenReturn(Future.successful(None))
    val service = spy(
      new SpendReportingService(
        testContext,
        dataSource,
        Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendExportConfiguration(RawlsBillingProjectName("fakeProject")), Duration.Inf)
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
    e.errorReport.message shouldBe s"billing project fakeProject not found"
  }

  it should "throw an exception if the billing project does not have a linked billing account" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]

    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    val dataSource = mock[SlickDataSource]
    when(dataSource.inTransaction[Option[BillingProjectSpendExport]](any(), any()))
      .thenReturn(Future.failed(new RawlsException()))
    val service = new SpendReportingService(
      testContext,
      dataSource,
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      billingRepository,
      bpmDAO,
      samDAO,
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val projectName = RawlsBillingProjectName("fakeProject")

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getSpendExportConfiguration(projectName), Duration.Inf)
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
    e.errorReport.message shouldBe s"billing account not found on billing project ${projectName.value}"
  }

  it should "throw an exception if BigQuery results include an unexpected Google project" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]

    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    when(billingRepository.getBillingProject(any())).thenReturn(Future.successful(Option.apply(billingProject)))
    val badRow = Map(
      "cost" -> "0.10111",
      "credits" -> "0.0",
      "currency" -> "USD",
      "googleProjectId" -> "fakeProject"
    )

    val bigQueryService = mockBigQuery(badRow :: TestData.Workspace.table)
    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        bigQueryService,
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )
    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    doReturn(Future.successful(billingProjectSpendExport)).when(service).getSpendExportConfiguration(any())
    doReturn(Future.successful(TestData.googleProjectsToWorkspaceNames))
      .when(service)
      .getSpendReportableWorkspaceGoogleProjects(any())

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.getSpendForGCPBillingProject(
          RawlsBillingProjectName(""),
          DateTime.now().minusDays(1),
          DateTime.now(),
          Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))
        ),
        Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.InternalServerError)
  }

  "getSpendForBillingProject" should "get the spend report from BPM for Azure billing projects" in {
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val price1 = BigDecimal("10.22")
    val price2 = BigDecimal("50.74")
    val currency = "USD"

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    val billingProfileId = UUID.randomUUID()
    val projectName = RawlsBillingProjectName(wsName.namespace)
    val azureBillingProject = RawlsBillingProject(
      UUID.randomUUID(),
      projectName,
      CreationStatuses.Ready,
      Option(billingAccountName),
      None,
      billingProfileId = Option.apply(billingProfileId.toString)
    )
    when(billingRepository.getBillingProject(mockitoEq(projectName)))
      .thenReturn(Future.successful(Option.apply(azureBillingProject)))

    val spendReport =
      TestData.BpmSpendReport.spendData(from, to, currency, Map("Compute" -> price1, "Storage" -> price2))
    when(bpmDAO.getAzureSpendReport(any(), any(), any(), any()))
      .thenReturn(spendReport)
    when(bpmDAO.getBillingProfile(mockitoEq(billingProfileId), any()))
      .thenReturn(Option(new ProfileModel().id(billingProfileId).cloudPlatform(BpmCloudPlatform.AZURE)))

    val billingProfileIdCapture: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val startDateCapture: ArgumentCaptor[Date] = ArgumentCaptor.forClass(classOf[Date])
    val endDateCapture: ArgumentCaptor[Date] = ArgumentCaptor.forClass(classOf[Date])
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      billingRepository,
      bpmDAO,
      samDAO,
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )

    val result = Await.result(
      service.getSpendForBillingProject(azureBillingProject.projectName, from, to, Set.empty),
      Duration.Inf
    )

    result.spendSummary.credits shouldBe "0"
    result.spendSummary.cost shouldBe Seq(price1, price2).sum.toString()
    result.spendSummary.currency shouldBe "USD"
    result.spendSummary.startTime.get.toString(ISODateTimeFormat.date()) shouldBe from.toString(
      ISODateTimeFormat.date()
    )
    result.spendSummary.endTime.get.toString(ISODateTimeFormat.date()) shouldBe to.toString(ISODateTimeFormat.date())

    verify(bpmDAO, Mockito.times(1))
      .getAzureSpendReport(billingProfileIdCapture.capture(),
                           startDateCapture.capture(),
                           endDateCapture.capture(),
                           any()
      )

    billingProfileIdCapture.getValue shouldBe billingProfileId
    startDateCapture.getValue shouldBe from.toDate
    endDateCapture.getValue shouldBe to.toDate
  }

  it should "not get the spend report from BPM for Google billing projects with a billing profile" in {
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)
    val billingProfileId = UUID.randomUUID()
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getBillingProfile(mockitoEq(billingProfileId), any()))
      .thenReturn(Option(new ProfileModel().id(billingProfileId).cloudPlatform(BpmCloudPlatform.GCP)))

    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    when(billingRepository.getBillingProject(mockitoEq(billingProject.projectName)))
      .thenReturn(Future.successful(Option(billingProject.copy(billingProfileId = Option(billingProfileId.toString)))))

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))

    val bigQueryService = mockBigQuery(TestData.Workspace.table)
    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        bigQueryService,
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )
    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    doReturn(Future.successful(billingProjectSpendExport)).when(service).getSpendExportConfiguration(any())
    doReturn(Future.successful(TestData.googleProjectsToWorkspaceNames))
      .when(service)
      .getSpendForGCPBillingProject(any(), any(), any(), any())

    Await.result(
      service.getSpendForBillingProject(
        billingProject.projectName,
        from,
        to,
        Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))
      ),
      Duration.Inf
    )

    verify(bpmDAO, Mockito.times(0)).getAzureSpendReport(any(), any(), any(), any())
    verify(service, Mockito.times(1)).getSpendForGCPBillingProject(
      mockitoEq(billingProject.projectName),
      mockitoEq(from),
      mockitoEq(to),
      mockitoEq(Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace)))
    )
  }

  it should "not get the spend report from BPM for Google billing projects without a billing profile" in {
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    when(billingRepository.getBillingProject(mockitoEq(billingProject.projectName)))
      .thenReturn(Future.successful(Option(billingProject.copy(billingProfileId = None))))

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))

    val bigQueryService = mockBigQuery(TestData.Workspace.table)
    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        bigQueryService,
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )
    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    doReturn(Future.successful(billingProjectSpendExport)).when(service).getSpendExportConfiguration(any())
    doReturn(Future.successful(TestData.googleProjectsToWorkspaceNames))
      .when(service)
      .getSpendForGCPBillingProject(any(), any(), any(), any())

    Await.result(
      service.getSpendForBillingProject(
        billingProject.projectName,
        from,
        to,
        Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace))
      ),
      Duration.Inf
    )

    verify(bpmDAO, Mockito.times(0)).getBillingProfile(any(), any())
    verify(bpmDAO, Mockito.times(0)).getAzureSpendReport(any(), any(), any(), any())
    verify(service, Mockito.times(1)).getSpendForGCPBillingProject(
      mockitoEq(billingProject.projectName),
      mockitoEq(from),
      mockitoEq(to),
      mockitoEq(Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace)))
    )
  }

  it should "handle/rethrow ApiException from BPM client" in {
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    when(bpmDAO.getBillingProfile(any(), any()))
      .thenReturn(Option(new ProfileModel().id(UUID.randomUUID()).cloudPlatform(BpmCloudPlatform.AZURE)))
    val errorMessage = "something went wrong"
    doThrow(new BpmAzureSpendReportApiException(StatusCodes.BadRequest.intValue, errorMessage))
      .when(bpmDAO)
      .getAzureSpendReport(any(), any(), any(), any())

    val billingProfileId = UUID.randomUUID()
    val projectName = RawlsBillingProjectName(wsName.namespace)
    val azureBillingProject = RawlsBillingProject(
      UUID.randomUUID(),
      projectName,
      CreationStatuses.Ready,
      Option(billingAccountName),
      None,
      billingProfileId = Option.apply(billingProfileId.toString)
    )
    when(billingRepository.getBillingProject(mockitoEq(projectName)))
      .thenReturn(Future.successful(Option.apply(azureBillingProject)))

    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      billingRepository,
      bpmDAO,
      samDAO,
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.getSpendForBillingProject(azureBillingProject.projectName, from, to, Set.empty),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
    e.errorReport.message shouldBe errorMessage
  }

  "validateReportParameters" should "not throw an exception when validating max start and end date range" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val startDate = DateTime.now().minusDays(spendReportingServiceConfig.maxDateRange)
    val endDate = DateTime.now()
    service.validateReportParameters(startDate, endDate)
  }

  it should "throw an exception when start date is after end date" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val startDate = DateTime.now()
    val endDate = DateTime.now().minusDays(1)
    val e = intercept[RawlsExceptionWithErrorReport](service.validateReportParameters(startDate, endDate))
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "throw an exception when date range is larger than the max date range" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val startDate = DateTime.now().minusDays(spendReportingServiceConfig.maxDateRange + 1)
    val endDate = DateTime.now()
    val e = intercept[RawlsExceptionWithErrorReport](service.validateReportParameters(startDate, endDate))
    e.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  "getQuery" should "use the constant _PARTITIONTIME as the time partition name for non-broad tables" in {
    val expectedQuery =
      s"""
         | SELECT
         |  SUM(cost) as cost,
         |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
         |  currency , project.id as googleProjectId, DATE(_PARTITIONTIME) as date
         | FROM `NonBroadTable`
         | WHERE billing_account_id = @billingAccountId
         | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
         | AND project.id in UNNEST(@projects)
         | GROUP BY currency , googleProjectId, date
         |""".stripMargin

    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val result = service.getQuery(
      Set(
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace),
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily)
      ),
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), Some("NonBroadTable"))
    )
    result shouldBe expectedQuery
  }

  it should "use the configured default time partition name for the broad table" in {
    val expectedQuery =
      s"""
         | SELECT
         |  SUM(cost) as cost,
         |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
         |  currency , project.id as googleProjectId, DATE(fakeTimePartitionColumn) as date
         | FROM `fakeTable`
         | WHERE billing_account_id = @billingAccountId
         | AND fakeTimePartitionColumn BETWEEN @startDate AND @endDate
         | AND project.id in UNNEST(@projects)
         | GROUP BY currency , googleProjectId, date
         |""".stripMargin

    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val result = service.getQuery(
      Set(
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace),
        SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily)
      ),
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    )
    result shouldBe expectedQuery
  }

  "getAllUserWorkspaceQuery" should "generate a query for a billingProject with its workspace projects" in {

    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject1"),
                                RawlsBillingAccountName("billingAccount1"),
                                Some("billing1_bq_project.billing1_dataset.billing1_table")
      )
    val billingProjectSpendExport2 =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject2"),
                                RawlsBillingAccountName("billingAccount2"),
                                Some("billing1_bq_project.billing1_dataset.billing2_table")
      )

    val workspaces = Map(
      RawlsBillingAccountName("billingAccount1") -> Seq(
        GoogleProjectId("workspace2ProjectId"),
        GoogleProjectId("workspace1ProjectId")
      ),
      RawlsBillingAccountName("billingAccount2") -> Seq(
        GoogleProjectId("workspace3ProjectId"),
        GoogleProjectId("workspace4ProjectId")
      )
    )

    val expectedQuery =
      s"""|WITH spend_categories AS (
          |  SELECT
          |    project.id AS project_id,
          |    currency,
          |    SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
          |    CASE
          |      WHEN service.description IN ('Cloud Storage') THEN 'Storage'
          |      WHEN service.description IN ('Compute Engine', 'Google Kubernetes Engine') THEN 'Compute'
          |      ELSE 'Other'
          |    END AS spend_category,
          |    SUM(CAST(cost AS FLOAT64)) AS category_cost
          |  FROM
          |    billing1_bq_project.billing1_dataset.billing1_table
          |  where
          |    _PARTITIONTIME BETWEEN @startDate AND @endDate
          |    and ( (billing_account_id = 'billingAccount1' and project.id in ('workspace2ProjectId', 'workspace1ProjectId'))
          |    or (billing_account_id = 'billingAccount2' and project.id in ('workspace3ProjectId', 'workspace4ProjectId')) )
          |  GROUP BY
          |    project_id,
          |    spend_category,
          |    currency
          |)
          |SELECT
          |  project_id,
          |  SUM(category_cost) AS total_cost,
          |  SUM(CASE WHEN spend_category = 'Storage' THEN category_cost ELSE 0 END) AS storage_cost,
          |  SUM(CASE WHEN spend_category = 'Compute' THEN category_cost ELSE 0 END) AS compute_cost,
          |  SUM(CASE WHEN spend_category = 'Other' THEN category_cost ELSE 0 END) AS other_cost,
          |  currency,
          |  SUM(CASE WHEN spend_category = 'Storage' THEN credits ELSE 0 END) AS storage_credits,
          |  SUM(CASE WHEN spend_category = 'Compute' THEN credits ELSE 0 END) AS compute_credits,
          |  SUM(CASE WHEN spend_category = 'Other' THEN credits ELSE 0 END) AS other_credits,
          |FROM
          |  spend_categories
          |GROUP BY
          |  project_id,
          |  currency
          |ORDER BY
          |  total_cost DESC
          |limit 5 offset 5
          |""".stripMargin

    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )
    val result = service.getAllUserWorkspaceQuery(
      billingProjectSpendExport.spendExportTable.get,
      workspaces,
      5,
      5
    )

    // It's easier and more reliable to do this than tweak line changes in the query or expected query
    def normalizeWhitespace(str: String): String = str.replaceAll("\\s+", " ").trim
    normalizeWhitespace(result) shouldEqual normalizeWhitespace(expectedQuery)

  }

  "getBillingWithSpendPermission" should "return spendExportTables for workspaces" in {

    val dataSource = mock[SlickDataSource]

    val billingProject1SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject1"),
                                RawlsBillingAccountName("billingAccount1"),
                                Some("billing_bq_project.billing_dataset.billing_table")
      )

    val billingProject2SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject2"),
                                RawlsBillingAccountName("billingAccount2"),
                                Some("billing_bq_project.billing_dataset.billing_table")
      )

    val billingProject3SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject3"),
                                RawlsBillingAccountName("billingAccount3"),
                                None
      )
    val samDAO = mock[SamDAO]

    doReturn(
      Future.successful(
        Seq(
          new FilteredFlatResource(
          ).resourceId(
            "workspace1Billing1"
          ),
          new FilteredFlatResource(
          ).resourceId(
            "workspace2Billing1"
          ),
          new FilteredFlatResource(
          ).resourceId(
            "workspace1Billing2"
          ),
          new FilteredFlatResource(
          ).resourceId(
            "workspace1Billing3"
          )
        )
      )
    )
      .when(samDAO)
      .listResourcesWithActions(mockitoEq(SamResourceTypeNames.workspace), any(), any())

    val workspace1Billing1 =
      TestData.workspace("workspace1Billing1",
                         GoogleProjectId("workspace1ProjectId"),
                         WorkspaceVersions.V1,
                         "billingProject1"
      )
    val workspace2Billing1 =
      TestData.workspace("workspace2Billing1",
                         GoogleProjectId("workspace2ProjectId"),
                         WorkspaceVersions.V2,
                         "billingProject1"
      )
    val workspace1Billing2 =
      TestData.workspace("workspace1Billing2",
                         GoogleProjectId("workspace3ProjectId"),
                         WorkspaceVersions.V2,
                         "billingProject2"
      )
    val workspace1Billing3 =
      TestData.workspace("workspace1Billing3",
                         GoogleProjectId("workspace4ProjectId"),
                         WorkspaceVersions.V2,
                         "billingProject3"
      )

    val workspaces = Map(
      RawlsBillingProjectName("billingProject1") -> Seq(workspace1Billing1, workspace2Billing1),
      RawlsBillingProjectName("billingProject2") -> Seq(workspace1Billing2),
      RawlsBillingProjectName("billingProject3") -> Seq(workspace1Billing3)
    )
    val mockWorkspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    when(mockWorkspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(Future.successful(workspaces))
    val mockWorkspaceServiceConstructor: RawlsRequestContext => WorkspaceService = { _ =>
      mockWorkspaceService
    }

    val service = spy(
      new SpendReportingService(
        testContext,
        dataSource,
        Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
        mock[BillingRepository],
        mock[BillingProfileManagerDAO],
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )

    doReturn(Future.successful(Seq(billingProject1SpendExport, billingProject2SpendExport, billingProject3SpendExport)))
      .when(service)
      .getSpendExportConfigurations(
        any()
      )

    val result = Await.result(
      service.getBillingWithSpendPermission(testContext),
      Duration.Inf
    )

    result shouldBe Map(
      billingProject1SpendExport -> Seq(workspace1Billing1, workspace2Billing1),
      billingProject2SpendExport -> Seq(workspace1Billing2),
      billingProject3SpendExport -> Seq(workspace1Billing3)
    )

  }

  "getSpendForAllWorkspaces" should "handle no owned workspaces" in {
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    when(samDAO.listResourcesWithActions(any(), any(), any())).thenReturn(Future.successful(List.empty))

    val mockWorkspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    when(mockWorkspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(Future.successful(Map.empty))
    val mockWorkspaceServiceConstructor: RawlsRequestContext => WorkspaceService = { _ =>
      mockWorkspaceService
    }

    val bigQueryService = mockBigQuery(List[Map[String, String]]())

    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        bigQueryService,
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )

    val exceptionFuture = recoverToExceptionIf[RawlsExceptionWithErrorReport] {
      service.getSpendForAllWorkspaces(from, to, 100, 0)
    }
    exceptionFuture.map { e =>
      e.errorReport.statusCode shouldBe Option(StatusCodes.InternalServerError)
      e.errorReport.message.contains("no workspaces") shouldBe true
    }
  }

  "getSpendForAllWorkspaces" should "get the spend report from multiple billing projects" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    // Billing projects
    val billingProfileId1 = UUID.randomUUID()
    val projectName1 = RawlsBillingProjectName("billingProject1")
    val billingAccount1 = RawlsBillingAccountName("billingAcct1")
    val billingProject1 = RawlsBillingProject(
      UUID.randomUUID(),
      projectName1,
      CreationStatuses.Ready,
      Option(billingAccount1),
      None,
      billingProfileId = Option.apply(billingProfileId1.toString)
    )
    val billingProfileId2 = UUID.randomUUID()
    val projectName2 = RawlsBillingProjectName("billingProject2")
    val billingAccount2 = RawlsBillingAccountName("billingAcct2")
    val billingProject2 = RawlsBillingProject(
      UUID.randomUUID(),
      projectName2,
      CreationStatuses.Ready,
      Option(billingAccount2),
      None,
      billingProfileId = Option.apply(billingProfileId2.toString)
    )

    // Billing project spend exports
    val billingProject1SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject1"),
                                RawlsBillingAccountName("billingAccount1"),
                                Some("billing1_bq_project.billing1_dataset.billing1_table")
      )

    val billingProject2SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject2"),
                                RawlsBillingAccountName("billingAccount2"),
                                None
      )

    // Workspaces
    val workspace1Billing1 =
      TestData.workspace("workspace1Billing1",
                         GoogleProjectId("workspace1ProjectId"),
                         WorkspaceVersions.V1,
                         "billingProject1"
      )
    val workspace2Billing2 =
      TestData.workspace("workspace2Billing2",
                         GoogleProjectId("workspace2ProjectId"),
                         WorkspaceVersions.V2,
                         "billingProject2"
      )

    val mockWorkspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
    when(mockWorkspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(
        Future.successful(
          Map(billingProject1.projectName -> List(workspace1Billing1),
              billingProject2.projectName -> List(workspace2Billing2)
          )
        )
      )

    val mockWorkspaceServiceConstructor: RawlsRequestContext => WorkspaceService = { _ =>
      mockWorkspaceService
    }

    when(billingRepository.getBillingProject(mockitoEq(projectName1)))
      .thenReturn(Future.successful(Option.apply(billingProject1)))
    when(billingRepository.getBillingProject(mockitoEq(projectName2)))
      .thenReturn(Future.successful(Option.apply(billingProject2)))

    when(samDAO.listResourcesWithActions(any(), any(), any())).thenReturn(
      Future.successful(
        List(
          new FilteredFlatResource().resourceId(UUID.randomUUID().toString),
          new FilteredFlatResource().resourceId(UUID.randomUUID().toString)
        )
      )
    )

    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val price1 = BigDecimal("10.22")
    val price2 = BigDecimal("50.74")
    val zero = BigDecimal("0.00")
    val total = price1 + price2

    val table1: List[Map[String, String]] = List(
      Map(
        "storage_cost" -> s"$price1",
        "compute_cost" -> s"$zero",
        "other_cost" -> s"$price2",
        "total_cost" -> s"$total",
        "currency" -> "USD",
        "project_id" -> "workspace1ProjectId",
        "project_name" -> "terra-billing-project1",
        "storage_credits" -> s"$zero",
        "compute_credits" -> s"$zero",
        "other_credits" -> s"$zero"
      )
    )
    val table2: List[Map[String, String]] = List(
      Map(
        "storage_cost" -> s"$price2",
        "compute_cost" -> s"$zero",
        "other_cost" -> s"$zero",
        "total_cost" -> s"$price2",
        "project_id" -> "workspace2ProjectId",
        "project_name" -> "terra-billing-project1",
        "currency" -> "USD",
        "storage_credits" -> s"$zero",
        "compute_credits" -> s"$zero",
        "other_credits" -> s"$zero"
      )
    )

    val bigQueryService = mock[GoogleBigQueryService[IO]](RETURNS_SMART_NULLS)
    val job1 = mock[Job]
    when(job1.getQueryResults(any())).thenReturn(createTableResult(table1))
    when(job1.getStatistics).thenReturn(mock[JobStatistics.QueryStatistics](RETURNS_SMART_NULLS))
    when(job1.waitFor()).thenReturn(job1)
    val job2 = mock[Job]
    when(job2.getQueryResults(any())).thenReturn(createTableResult(table2))
    when(job2.getStatistics).thenReturn(mock[JobStatistics.QueryStatistics](RETURNS_SMART_NULLS))
    when(job2.waitFor()).thenReturn(job2)
    when(bigQueryService.runJob(any(), any())).thenReturn(IO(job1), IO(job2))

    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        Resource.pure[IO, GoogleBigQueryService[IO]](bigQueryService),
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )
    doReturn(Future.successful(Seq(billingProject1SpendExport, billingProject2SpendExport)))
      .when(service)
      .getSpendExportConfigurations(
        any()
      )

    val result = Await.result(
      service.getSpendForAllWorkspaces(from, to, 100, 0),
      Duration.Inf
    )

    result.get.spendDetails.length shouldBe 2

    val spendSummary = result.get.spendSummary

    spendSummary.credits shouldBe zero.toString()
    spendSummary.cost shouldBe (total + price2).toString()
    spendSummary.currency shouldBe "USD"
    spendSummary.startTime.get.toString(ISODateTimeFormat.date()) shouldBe from.toString(
      ISODateTimeFormat.date()
    )
    spendSummary.endTime.get.toString(ISODateTimeFormat.date()) shouldBe to.toString(ISODateTimeFormat.date())

  }

  "getSpendForAllWorkspaces" should "handle errors from bigquery gracefully" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    // Billing projects
    val billingProfileId1 = UUID.randomUUID()
    val projectName1 = RawlsBillingProjectName("billingProject1")
    val billingAccount1 = RawlsBillingAccountName("billingAcct1")
    val billingProject1 = RawlsBillingProject(
      UUID.randomUUID(),
      projectName1,
      CreationStatuses.Ready,
      Option(billingAccount1),
      None,
      billingProfileId = Option.apply(billingProfileId1.toString)
    )
    val billingProfileId2 = UUID.randomUUID()
    val projectName2 = RawlsBillingProjectName("billingProject2")
    val billingAccount2 = RawlsBillingAccountName("billingAcct2")
    val billingProject2 = RawlsBillingProject(
      UUID.randomUUID(),
      projectName2,
      CreationStatuses.Ready,
      Option(billingAccount2),
      None,
      billingProfileId = Option.apply(billingProfileId2.toString)
    )

    // Billing project spend exports
    val billingProject1SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject1"),
                                RawlsBillingAccountName("billingAccount1"),
                                Some("billing1_bq_project.billing1_dataset.billing1_table")
      )

    val billingProject2SpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName("billingProject2"),
                                RawlsBillingAccountName("billingAccount2"),
                                None
      )

    // Workspaces
    val workspace1Billing1 =
      TestData.workspace("workspace1Billing1",
                         GoogleProjectId("workspace1ProjectId"),
                         WorkspaceVersions.V1,
                         "billingProject1"
      )
    val workspace2Billing2 =
      TestData.workspace("workspace2Billing2",
                         GoogleProjectId("workspace2ProjectId"),
                         WorkspaceVersions.V2,
                         "billingProject2"
      )

    val mockWorkspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
    when(mockWorkspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(
        Future.successful(
          Map(billingProject1.projectName -> List(workspace1Billing1),
              billingProject2.projectName -> List(workspace2Billing2)
          )
        )
      )

    val mockWorkspaceServiceConstructor: RawlsRequestContext => WorkspaceService = { _ =>
      mockWorkspaceService
    }

    when(billingRepository.getBillingProject(mockitoEq(projectName1)))
      .thenReturn(Future.successful(Option.apply(billingProject1)))
    when(billingRepository.getBillingProject(mockitoEq(projectName2)))
      .thenReturn(Future.successful(Option.apply(billingProject2)))

    when(samDAO.listResourcesWithActions(any(), any(), any())).thenReturn(
      Future.successful(
        List(
          new FilteredFlatResource().resourceId(UUID.randomUUID().toString),
          new FilteredFlatResource().resourceId(UUID.randomUUID().toString)
        )
      )
    )

    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val price1 = BigDecimal("10.22")
    val price2 = BigDecimal("50.74")
    val zero = BigDecimal("0.00")
    val total = price1 + price2

    val table: List[Map[String, String]] = List(
      Map(
        "storage_cost" -> s"$price1",
        "compute_cost" -> s"$zero",
        "other_cost" -> s"$price2",
        "total_cost" -> s"$total",
        "currency" -> "USD",
        "project_id" -> "workspace1ProjectId",
        "project_name" -> "terra-billing-project1",
        "storage_credits" -> s"$zero",
        "compute_credits" -> s"$zero",
        "other_credits" -> s"$zero"
      )
    )

    val bigQueryService = mock[GoogleBigQueryService[IO]](RETURNS_SMART_NULLS)
    val job = mock[Job]
    when(job.getQueryResults(any())).thenReturn(createTableResult(table))
    when(job.getStatistics).thenReturn(mock[JobStatistics.QueryStatistics](RETURNS_SMART_NULLS))
    when(job.waitFor()).thenReturn(job)
    when(bigQueryService.runJob(any(), any()))
      .thenReturn(IO(job))
      .thenAnswer(_ => IO.raiseError(new RuntimeException("BigQuery has errored")))

    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        Resource.pure[IO, GoogleBigQueryService[IO]](bigQueryService),
        billingRepository,
        bpmDAO,
        samDAO,
        spendReportingServiceConfig,
        mockWorkspaceServiceConstructor
      )
    )
    doReturn(Future.successful(Seq(billingProject1SpendExport, billingProject2SpendExport)))
      .when(service)
      .getSpendExportConfigurations(
        any()
      )

    val result = Await.result(
      service.getSpendForAllWorkspaces(from, to, 100, 0),
      Duration.Inf
    )

    result.get.spendDetails.length shouldBe 1

    val spendSummary = result.get.spendSummary

    spendSummary.credits shouldBe zero.toString()
    spendSummary.cost shouldBe total.toString()
    spendSummary.currency shouldBe "USD"
    spendSummary.startTime.get.toString(ISODateTimeFormat.date()) shouldBe from.toString(
      ISODateTimeFormat.date()
    )
    spendSummary.endTime.get.toString(ISODateTimeFormat.date()) shouldBe to.toString(ISODateTimeFormat.date())

  }

  "getSpendReportableWorkspaceGoogleProjects" should "return an empty map when there are no valid workspace IDs" in {
    val mockSamDAO = mock[SamDAO]
    val mockWorkspaceServiceConstructor = mock[RawlsRequestContext => WorkspaceService]
    val mockWorkspaceService = mock[WorkspaceService]
    val mockContext = mock[RawlsRequestContext]

    val spendReportingService = new SpendReportingService(
      mockContext,
      mock[SlickDataSource],
      mock[cats.effect.Resource[IO, GoogleBigQueryService[IO]]],
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mockSamDAO,
      mock[SpendReportingServiceConfig],
      mockWorkspaceServiceConstructor
    )

    when(mockSamDAO.listResourcesWithActions(any(), any(), any()))
      .thenReturn(Future.successful(Seq.empty))
    when(mockWorkspaceServiceConstructor.apply(any()))
      .thenReturn(mockWorkspaceService)
    when(mockWorkspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(Future.successful(Map.empty))

    spendReportingService.getSpendReportableWorkspaceGoogleProjects(mockContext).map { result =>
      result shouldBe empty
    }
  }

  it should "throw an exception when the user does not have read_spend_report action" in {
    val mockSamDAO = mock[SamDAO]
    val mockWorkspaceServiceConstructor = mock[RawlsRequestContext => WorkspaceService]
    val mockWorkspaceService = mock[WorkspaceService]
    val mockDataSource = mock[SlickDataSource]
    val spendReportingServiceConfig = SpendReportingServiceConfig(
      "fakeTable",
      "fakeTimePartitionColumn",
      90,
      "test.rawls"
    )

    when(mockDataSource.inTransaction[Option[BillingProjectSpendExport]](any(), any()))
      .thenReturn(
        Future.successful(
          Some(
            BillingProjectSpendExport(RawlsBillingProjectName("test-project"),
                                      RawlsBillingAccountName("test-account"),
                                      None
            )
          )
        )
      )
    when(mockSamDAO.listResourcesWithActions(any(), any(), any()))
      .thenReturn(Future.successful(Seq.empty))
    when(mockWorkspaceServiceConstructor.apply(any()))
      .thenReturn(mockWorkspaceService)
    when(mockWorkspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(Future.successful(Map.empty))

    val spendReportingService = new SpendReportingService(
      testContext,
      mockDataSource,
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig,
      mockWorkspaceServiceConstructor
    )

    recoverToExceptionIf[RawlsExceptionWithErrorReport] {
      spendReportingService.getSpendForGCPBillingProject(RawlsBillingProjectName("test-project"),
                                                         DateTime.now().minusDays(7),
                                                         DateTime.now(),
                                                         Set.empty[SpendReportingAggregationKeyWithSub]
      )
    } map { ex =>
      ex.errorReport.statusCode shouldBe StatusCodes.NotFound
      ex.errorReport.message should include("no spend data found for billing project")
    }
  }

  it should "return a map of workspaces grouped by billing project" in {
    val samDAO = mock[SamDAO]
    val workspaceService = mock[WorkspaceService]
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      samDAO,
      spendReportingServiceConfig,
      _ => workspaceService
    )

    val workspace = TestData.workspace1

    when(samDAO.listResourcesWithActions(any(), any(), any()))
      .thenReturn(Future.successful(List(new FilteredFlatResource().resourceId(workspace.workspaceId))))
    when(workspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(Future.successful(Map(RawlsBillingProjectName("test-project") -> Seq(workspace))))

    val result = Await.result(service.getSpendReportableWorkspaceGoogleProjects(testContext), Duration.Inf)
    result should contain key RawlsBillingProjectName("test-project")
    result(RawlsBillingProjectName("test-project")) should contain(workspace)
  }

  it should "return a map of Google project IDs to workspace names when user has read_spend_report action" in {
    val samDAO = mock[SamDAO]
    val workspaceService = mock[WorkspaceService]
    val service = spy(
      new SpendReportingService(
        testContext,
        mock[SlickDataSource],
        Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
        mock[BillingRepository],
        mock[BillingProfileManagerDAO],
        samDAO,
        spendReportingServiceConfig,
        _ => workspaceService
      )
    )

    val workspace = TestData.workspace1

    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    when(samDAO.listResourcesWithActions(any(), any(), any()))
      .thenReturn(Future.successful(List(new FilteredFlatResource().resourceId(workspace.workspaceId))))
    when(workspaceService.getGCPWorkspacesByBillingProjects(any()))
      .thenReturn(Future.successful(Map(RawlsBillingProjectName("test-project") -> Seq(workspace))))

    val result = Await.result(service.getSpendReportableWorkspaceGoogleProjects(testContext), Duration.Inf)
    result should contain key RawlsBillingProjectName("test-project")
    result(RawlsBillingProjectName("test-project")) should contain(workspace)
  }
}
