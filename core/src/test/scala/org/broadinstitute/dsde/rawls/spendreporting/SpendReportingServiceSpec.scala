package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.SpendReportingAggregation.AggregationKeyEnum
import bio.terra.profile.model.{SpendReport => SpendReportBPM}
import bio.terra.profile.model.{SpendReportingAggregation => SpendReportingAggregationBPM}
import bio.terra.profile.model.{SpendReportingForDateRange => SpendReportingForDateRangeBPM}
import cats.effect.{IO, Resource}
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{model, RawlsException, RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito.{doReturn, never, spy, verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.{Date, UUID}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

class SpendReportingServiceSpec extends AnyFlatSpecLike with Matchers with MockitoTestUtils {

  implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo = UserInfo(RawlsUserEmail("owner-access"),
                                    OAuth2BearerToken("token"),
                                    123,
                                    RawlsUserSubjectId("123456789876543212345")
  )
  val wsName: WorkspaceName = WorkspaceName("myNamespace", "myWorkspace")

  val billingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("fakeBillingAcct")

  val billingProject: RawlsBillingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace),
                                                                CreationStatuses.Ready,
                                                                Option(billingAccountName),
                                                                None
  )

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
      def spendData(from: DateTime, to: DateTime, currency: String, costData: Seq[BigDecimal]): SpendReportBPM = {
        // create spend reporting items based on costData
        val spendDataList = costData
          .map(cost =>
            new SpendReportingForDateRangeBPM()
              .cost(cost.toString())
              .credits("0") /*credits is always 0 in case of Azure*/
              .currency(currency)
              .startTime(from.toString(ISODateTimeFormat.date()))
              .endTime(to.toString(ISODateTimeFormat.date()))
          )
          .asJava
        val spendReportingAggregation =
          new SpendReportingAggregationBPM()
            .aggregationKey(AggregationKeyEnum.CATEGORY)
            .spendData(spendDataList)
        val spendSummary = new SpendReportingForDateRangeBPM()
          .cost(costData.sum.toString())
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
        spendReportingServiceConfig
      )
    )
    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    doReturn(Future.successful(billingProjectSpendExport)).when(service).getSpendExportConfiguration(any())
    doReturn(Future.successful(TestData.googleProjectsToWorkspaceNames)).when(service).getWorkspaceGoogleProjects(any())

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

  it should "throw an exception when user does not have read_spend_report" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    when(
      samDAO.userHasAction(mockitoEq(SamResourceTypeNames.billingProject),
                           any(),
                           mockitoEq(SamBillingProjectActions.readSpendReport),
                           mockitoEq(testContext)
      )
    ).thenReturn(Future.successful(false))
    when(billingRepository.getBillingProject(any())).thenReturn(Future.successful(Option.apply(billingProject)))
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      billingRepository,
      bpmDAO,
      samDAO,
      spendReportingServiceConfig
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.getSpendForGCPBillingProject(
          billingProject.projectName,
          DateTime.now().minusDays(1),
          DateTime.now(),
          Set.empty
        ),
        Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)
  }

  it should "throw an exception if the billing project cannot be found" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future.successful(true))
    val dataSource = mock[SlickDataSource]
    when(dataSource.inTransaction[Option[BillingProjectSpendExport]](any(), any())).thenReturn(Future.successful(None))
    val service = new SpendReportingService(
      testContext,
      dataSource,
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      billingRepository,
      bpmDAO,
      samDAO,
      spendReportingServiceConfig
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
      spendReportingServiceConfig
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
        spendReportingServiceConfig
      )
    )
    val billingProjectSpendExport =
      BillingProjectSpendExport(RawlsBillingProjectName(""), RawlsBillingAccountName(""), None)
    doReturn(Future.successful(billingProjectSpendExport)).when(service).getSpendExportConfiguration(any())
    doReturn(Future.successful(TestData.googleProjectsToWorkspaceNames)).when(service).getWorkspaceGoogleProjects(any())

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

  "getSpendForBillingProject" should "call BPM in case of Azure billing project" in {
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val price1 = BigDecimal("10.22")
    val price2 = BigDecimal("50.74")
    val currency = "USD"

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

    val spendReport = TestData.BpmSpendReport.spendData(from, to, currency, Seq(price1, price2))
    when(bpmDAO.getAzureSpendReport(any(), any(), any(), any())(mockitoEq(executionContext)))
      .thenReturn(Future.apply(spendReport))

    val billingProfileId = UUID.randomUUID()
    val projectName = RawlsBillingProjectName(wsName.namespace)
    val azureBillingProject = RawlsBillingProject(
      projectName,
      CreationStatuses.Ready,
      Option(billingAccountName),
      None,
      billingProfileId = Option.apply(billingProfileId.toString)
    )
    when(billingRepository.getBillingProject(mockitoEq(projectName)))
      .thenReturn(Future.successful(Option.apply(azureBillingProject)))

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
      spendReportingServiceConfig
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
      )(mockitoEq(executionContext))

    billingProfileIdCapture.getValue shouldBe billingProfileId
    startDateCapture.getValue shouldBe from.toDate
    endDateCapture.getValue shouldBe to.toDate
  }

  // "it" should "throw exception or be resilient and empty result" {}

  "validateReportParameters" should "not throw an exception when validating max start and end date range" in {
    val service = new SpendReportingService(
      testContext,
      mock[SlickDataSource],
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig
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
      spendReportingServiceConfig
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
      spendReportingServiceConfig
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
      spendReportingServiceConfig
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
      spendReportingServiceConfig
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

  "getWorkspaceGoogleProjects" should "only map v2 workspaces to project names" in {
    val v1Workspace = TestData.workspace("v1name", GoogleProjectId("v1ProjectId"), WorkspaceVersions.V1)
    val v2Workspace = TestData.workspace("v2name", GoogleProjectId("v2ProjectId"), WorkspaceVersions.V2)
    val workspaces = Seq(
      v1Workspace,
      v2Workspace
    )

    val dataSource = mock[SlickDataSource]
    when(dataSource.inTransaction[Seq[Workspace]](any(), any())).thenReturn(Future.successful(workspaces))
    val service = new SpendReportingService(
      testContext,
      dataSource,
      Resource.pure[IO, GoogleBigQueryService[IO]](mock[GoogleBigQueryService[IO]]),
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      spendReportingServiceConfig
    )

    val result = Await.result(service.getWorkspaceGoogleProjects(RawlsBillingProjectName("")), Duration.Inf)

    result shouldBe Map(GoogleProjectId("v2ProjectId") -> v2Workspace.toWorkspaceName)
  }

}
