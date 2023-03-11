package org.broadinstitute.dsde.rawls.spendreporting

import java.util.{Currency, UUID}
import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.cloud.bigquery.{JobStatistics, Option => _, _}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{Counter, Histogram}
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, HitRatioGauge, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.{SpendReportingAggregationKeyWithSub, _}
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService._
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

object SpendReportingService {
  def constructor(
    dataSource: SlickDataSource,
    bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
    userService: UserService,
    bpmDao: BillingProfileManagerDAO,
    samDAO: SamDAO,
    spendReportingServiceConfig: SpendReportingServiceConfig
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): SpendReportingService =
    new SpendReportingService(ctx,
                              dataSource,
                              bigQueryService,
                              userService,
                              bpmDao,
                              samDAO,
                              spendReportingServiceConfig
    )

  val SpendReportingMetrics = "spendReporting"
  val BigQueryKey = "bigQuery"
  val BigQueryCacheMetric = "cache"
  val BigQueryBytesProcessedMetric = "processed"

  def extractSpendReportingResults(
    allRows: List[FieldValueList],
    start: DateTime,
    end: DateTime,
    names: Map[GoogleProjectId, WorkspaceName],
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): SpendReportingResults = {

    val currency = allRows.map(_.get("currency").getStringValue).distinct match {
      case head :: List() => Currency.getInstance(head)
      case head :: tail =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.InternalServerError,
          s"Inconsistent currencies found while aggregating spend data: $head and ${tail.head} cannot be combined"
        )
      case List() => throw RawlsExceptionWithErrorReport(StatusCodes.NotFound, "No currencies found for spend data")
    }

    def sum(rows: List[FieldValueList], field: String): String = rows
      .map(row => BigDecimal(row.get(field).getDoubleValue))
      .sum
      .setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      .toString()

    def aggregateRows(
      rows: List[FieldValueList],
      aggregation: SpendReportingAggregationKeyWithSub
    ): SpendReportingAggregation = {
      val groupedRows = rows.groupBy(r =>
        aggregation.key match {
          case SpendReportingAggregationKeys.Category =>
            TerraSpendCategories.categorize(r.get(aggregation.key.bigQueryAlias).getStringValue).toString
          case _ => r.get(aggregation.key.bigQueryAlias).getStringValue
        }
      )

      val aggregatedRows = groupedRows.map { case (rowKey, aggregationRows) =>
        val (category, timeRange, projectId, workspaceName) = aggregation.key match {
          case SpendReportingAggregationKeys.Category =>
            (Some(TerraSpendCategories.withName(rowKey)), None, None, None)
          case SpendReportingAggregationKeys.Daily =>
            val startDate = DateTime.parse(rowKey)
            val endDate = start.plusDays(1).minusMillis(1)
            (None, Some((startDate, endDate)), None, None)
          case SpendReportingAggregationKeys.Workspace =>
            val workspaceName = names.getOrElse(
              GoogleProjectId(rowKey),
              throw RawlsExceptionWithErrorReport(
                StatusCodes.InternalServerError,
                s"unexpected project $rowKey returned by BigQuery"
              )
            )
            (None, None, Some(GoogleProject(rowKey)), Some(workspaceName))
        }

        SpendReportingForDateRange(
          sum(aggregationRows, "cost"),
          sum(aggregationRows, "credits"),
          currency.getCurrencyCode,
          startTime = timeRange.map(_._1),
          endTime = timeRange.map(_._2),
          workspace = workspaceName,
          googleProjectId = projectId,
          category = category,
          subAggregation = aggregation.subAggregationKey.map(SpendReportingAggregationKeyWithSub(_, None)).map {
            aggregateRows(aggregationRows, _)
          }
        )
      }.toList
      SpendReportingAggregation(aggregation.key, aggregatedRows)
    }

    val summary = SpendReportingForDateRange(
      sum(allRows, "cost"),
      sum(allRows, "credits"),
      currency.getCurrencyCode,
      Option(start),
      Option(end)
    )

    SpendReportingResults(aggregations.map(aggregateRows(allRows, _)).toList, summary)
  }

}

class SpendReportingService(
  ctx: RawlsRequestContext,
  dataSource: SlickDataSource,
  bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
  userService: UserService,
  bpmDao: BillingProfileManagerDAO,
  samDAO: SamDAO,
  spendReportingServiceConfig: SpendReportingServiceConfig
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging
    with RawlsInstrumented {

  /**
    * Base name for all metrics. This will be prepended to all generated metric names.
    * Example: dev.firecloud.rawls
    */
  override val workbenchMetricBaseName: String = spendReportingServiceConfig.workbenchMetricBaseName

  def spendReportingMetrics: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(GoogleInstrumented.GoogleServiceMetricKey, SpendReportingMetrics)

  def cacheCounter(accessType: String): Counter =
    spendReportingMetrics.expand(BigQueryKey, BigQueryCacheMetric).asCounter(accessType)

  def cacheHitRate(): HitRatioGauge =
    spendReportingMetrics.expand(BigQueryKey, BigQueryCacheMetric).asRatio[HitRatioGauge]("hitRate") {
      new HitRatioGauge(
        cacheCounter("hits"),
        cacheCounter("calls")
      )
    }

  def bytesProcessedCounter: Histogram =
    spendReportingMetrics.expand(BigQueryKey, BigQueryBytesProcessedMetric).asHistogram("bytes")

  private def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(
    op: => Future[T]
  ): Future[T] =
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, ctx).flatMap {
      case true => op
      case false =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.Forbidden,
          s"${ctx.userInfo.userEmail.value} cannot perform ${action.value} on project ${projectName.value}"
        )
    }

  private def toISODateString(dt: DateTime): String = dt.toString(ISODateTimeFormat.date())

  def getSpendExportConfiguration(project: RawlsBillingProjectName): Future[BillingProjectSpendExport] = dataSource
    .inTransaction(_.rawlsBillingProjectQuery.getBillingProjectSpendConfiguration(project))
    .recover { case _: RawlsException =>
      throw RawlsExceptionWithErrorReport(
        StatusCodes.BadRequest,
        s"billing account not found on billing project ${project.value}"
      )
    }
    .map {
      _.getOrElse(
        throw RawlsExceptionWithErrorReport(StatusCodes.NotFound, s"billing project ${project.value} not found")
      )
    }

  def getWorkspaceGoogleProjects(projectName: RawlsBillingProjectName): Future[Map[GoogleProjectId, WorkspaceName]] =
    dataSource.inTransaction(_.workspaceQuery.listWithBillingProject(projectName)).map {
      _.collect {
        case w if w.workspaceVersion == WorkspaceVersions.V2 => w.googleProjectId -> w.toWorkspaceName
      }.toMap
    }

  def validateReportParameters(startDate: DateTime, endDate: DateTime): Unit = if (startDate.isAfter(endDate)) {
    throw RawlsExceptionWithErrorReport(
      StatusCodes.BadRequest,
      s"start date ${toISODateString(startDate)} must be before end date ${toISODateString(endDate)}"
    )
  } else if (Days.daysBetween(startDate, endDate).getDays > spendReportingServiceConfig.maxDateRange) {
    throw RawlsExceptionWithErrorReport(
      StatusCodes.BadRequest,
      s"provided dates exceed maximum report date range of ${spendReportingServiceConfig.maxDateRange} days"
    )
  }

  def getQuery(aggregations: Set[SpendReportingAggregationKeyWithSub], config: BillingProjectSpendExport): String = {
    // Unbox potentially many SpendReportingAggregationKeyWithSubs for query,
    // all of which have optional subAggregationKeys and convert to Set[SpendReportingAggregationKey]
    val queryKeys = aggregations.flatMap(a => Set(Option(a.key), a.subAggregationKey).flatten)
    val tableName = config.spendExportTable.getOrElse(spendReportingServiceConfig.defaultTableName)
    val timePartitionColumn: String = {
      val isBroadTable = tableName == spendReportingServiceConfig.defaultTableName
      // The Broad table uses a view with a different column name.
      if (isBroadTable) spendReportingServiceConfig.defaultTimePartitionColumn else "_PARTITIONTIME"
    }
    s"""
       | SELECT
       |  SUM(cost) as cost,
       |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
       |  currency ${queryKeys.map(_.bigQueryAliasClause()).mkString}
       | FROM `$tableName`
       | WHERE billing_account_id = @billingAccountId
       | AND $timePartitionColumn BETWEEN @startDate AND @endDate
       | AND project.id in UNNEST(@projects)
       | GROUP BY currency ${queryKeys.map(_.bigQueryGroupByClause()).mkString}
       |""".stripMargin
      .replace("REPLACE_TIME_PARTITION_COLUMN", timePartitionColumn)
  }

  def setUpQuery(
    query: String,
    exportConf: BillingProjectSpendExport,
    start: DateTime,
    end: DateTime,
    projectNames: Map[GoogleProjectId, WorkspaceName]
  ): JobInfo = {
    def queryParam(value: String): QueryParameterValue =
      QueryParameterValue.newBuilder().setType(StandardSQLTypeName.STRING).setValue(value).build()

    val projectNamesParam: QueryParameterValue =
      QueryParameterValue
        .newBuilder()
        .setType(StandardSQLTypeName.ARRAY)
        .setArrayType(StandardSQLTypeName.STRING)
        .setArrayValues(projectNames.keySet.map(name => queryParam(name.value)).toList.asJava)
        .build()

    val queryConfig = QueryJobConfiguration
      .newBuilder(query)
      .addNamedParameter("billingAccountId", queryParam(exportConf.billingAccountId.withoutPrefix()))
      .addNamedParameter("startDate", queryParam(toISODateString(start)))
      .addNamedParameter("endDate", queryParam(toISODateString(end)))
      .addNamedParameter("projects", projectNamesParam)
      .build()

    JobInfo.newBuilder(queryConfig).build()
  }

  def logSpendQueryStats(stats: JobStatistics.QueryStatistics): Unit = {
    if (stats.getCacheHit) cacheHitRate().hit() else cacheHitRate().miss()
    bytesProcessedCounter += stats.getEstimatedBytesProcessed
  }

  def getSpendForGCPBillingProject(
    project: RawlsBillingProjectName,
    start: DateTime,
    end: DateTime,
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] = {
    validateReportParameters(start, end)
    requireProjectAction(project, SamBillingProjectActions.readSpendReport) {
      for {
        spendExportConf <- getSpendExportConfiguration(project)
        projectNames <- getWorkspaceGoogleProjects(project)

        query = getQuery(aggregations, spendExportConf)
        queryJob = setUpQuery(query, spendExportConf, start, end, projectNames)

        job: Job <- bigQueryService.use(_.runJob(queryJob)).unsafeToFuture().map(_.waitFor())
        _ = logSpendQueryStats(job.getStatistics[JobStatistics.QueryStatistics])
        result = job.getQueryResults()
      } yield result.getValues.asScala.toList match {
        case Nil =>
          throw RawlsExceptionWithErrorReport(
            StatusCodes.NotFound,
            s"no spend data found for billing project ${project.value} between dates ${toISODateString(start)} and ${toISODateString(end)}"
          )
        case rows => extractSpendReportingResults(rows, start, end, projectNames, aggregations)
      }
    }
  }

  // wrap GCP and Azure functionality into different methods. Azure spend reporting feature is
  // implemented in BPM and has also some param validation step, SAM action validation and configuration.
  def getSpendForBillingProject(
    projectId: String,
    start: DateTime,
    end: DateTime,
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] =
    for {
      billingProject <- userService.getBillingProject(RawlsBillingProjectName(projectId))

      report <- getReportData(billingProject.get, projectId, start, end, aggregations)

      result = report
    } yield result

  private def getReportData(billingProject: RawlsBillingProjectResponse,
                            /*cloudPlatform: String,*/
                            projectId: String,
                            start: DateTime,
                            end: DateTime,
                            aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] =
    if (billingProject.cloudPlatform.equalsIgnoreCase(CloudPlatform.GCP.toString)) {
      getSpendForGCPBillingProject(RawlsBillingProjectName(projectId), start, end, aggregations)
    } else {
      getSpendForAzureBillingProject(billingProject.billingProfileId, start, end)
    }

  private def getReportData2(billingProject: RawlsBillingProject,
                             projectId: String,
                             start: DateTime,
                             end: DateTime,
                             aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] = {
    val cloudPlatform = CloudPlatform.apply()
  }

  private def getSpendForAzureBillingProject(
    billingProfileId: UUID,
    start: DateTime,
    end: DateTime
  ): Future[SpendReportingResults] =
    for {
      spendReport <- bpmDao.getAzureSpendReport(billingProfileId, start.toDate, end.toDate, ctx)

      result = SpendReportingResults.apply(spendReport)
    } yield result
}
