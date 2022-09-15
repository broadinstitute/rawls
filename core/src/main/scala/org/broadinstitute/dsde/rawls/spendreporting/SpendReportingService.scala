package org.broadinstitute.dsde.rawls.spendreporting

import java.util.Currency
import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.cloud.bigquery.{JobStatistics, Option => _, _}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{Counter, Histogram}
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.SpendReportingAggregationKeys.SpendReportingAggregationKey
import org.broadinstitute.dsde.rawls.model.{SpendReportingAggregationKeyWithSub, _}
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService._
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
    samDAO: SamDAO,
    spendReportingServiceConfig: SpendReportingServiceConfig
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): SpendReportingService =
    new SpendReportingService(ctx, dataSource, bigQueryService, samDAO, spendReportingServiceConfig)

  val SpendReportingMetrics = "spendReporting"
  val BigQueryKey = "bigQuery"
  val BigQueryCacheMetric = "cache"
  val BigQueryBytesProcessedMetric = "processed"
  type BillingToWorkspaceNames = Map[String, WorkspaceName]

  def extractSpendReportingResults(
    allRows: List[FieldValueList],
    start: DateTime,
    end: DateTime,
    workspaceProjectsToNames: Map[String, WorkspaceName],
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): SpendReportingResults = {

    val currency = allRows.map(_.get("currency").getStringValue).distinct match {
      case head :: List() => Currency.getInstance(head)
      case head :: tail =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.BadGateway, // todo: Probably the wrong status code
          s"Inconsistent currencies found while aggregating spend data: $head and ${tail.head} cannot be combined"
        )
      case List() => throw RawlsExceptionWithErrorReport(StatusCodes.NotFound, "No currencies found for spend data")

    }

    def sum(rows: List[FieldValueList], field: String): String = rows
      .map(row => BigDecimal(row.get(field).getDoubleValue))
      .sum
      .setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      .toString()

    type SubKey = Option[SpendReportingAggregationKey]

    def byDate(rows: List[FieldValueList], subKey: SubKey): List[SpendReportingForDateRange] = rows
      .groupBy(row => DateTime.parse(row.get("date").getStringValue))
      .map { case (startTime, rowsForStartTime) =>
        SpendReportingForDateRange(
          sum(rowsForStartTime, "cost"),
          sum(rowsForStartTime, "credits"),
          currency.getCurrencyCode,
          Option(startTime),
          endTime = Option(startTime.plusDays(1).minusMillis(1)),
          subAggregation = subKey.map(key => aggregate(rowsForStartTime, SpendReportingAggregationKeyWithSub(key)))
        )
      }
      .toList

    def byWorkspace(rows: List[FieldValueList], subKey: SubKey): List[SpendReportingForDateRange] = rows
      .groupBy(row => row.get("googleProjectId").getStringValue)
      .map { case (googleProjectId, projectRows) =>
        val workspaceName = workspaceProjectsToNames.getOrElse(
          googleProjectId,
          throw RawlsExceptionWithErrorReport(
            StatusCodes.BadGateway,
            s"unexpected project ${googleProjectId} returned by BigQuery"
          )
        )
        SpendReportingForDateRange(
          sum(projectRows, "cost"),
          sum(projectRows, "credits"),
          currency.getCurrencyCode,
          workspace = Option(workspaceName),
          googleProjectId = Option(GoogleProject(googleProjectId)),
          subAggregation = subKey.map(key => aggregate(projectRows, SpendReportingAggregationKeyWithSub(key)))
        )
      }
      .toList

    def byCategory(rows: List[FieldValueList], subKey: SubKey): List[SpendReportingForDateRange] = rows
      .groupBy(row => TerraSpendCategories.categorize(row.get("service").getStringValue))
      .map { case (category, categoryRows) =>
        SpendReportingForDateRange(
          sum(categoryRows, "cost"),
          sum(categoryRows, "credits"),
          currency.getCurrencyCode,
          category = Option(category),
          subAggregation = subKey.map(key => aggregate(categoryRows, SpendReportingAggregationKeyWithSub(key)))
        )
      }
      .toList

    def aggregate(rows: List[FieldValueList], key: SpendReportingAggregationKeyWithSub): SpendReportingAggregation = {
      val spend = key match {
        case SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category, sub)  => byCategory(rows, sub)
        case SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace, sub) => byWorkspace(rows, sub)
        case SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily, sub)     => byDate(rows, sub)
      }
      SpendReportingAggregation(key.key, spend)
    }

    val summary = SpendReportingForDateRange(
      sum(allRows, "cost"),
      sum(allRows, "credits"),
      currency.getCurrencyCode,
      Option(start),
      Option(end)
    )
    SpendReportingResults(aggregations.map(aggregate(allRows, _)).toList, summary)
  }

}

class SpendReportingService(
  ctx: RawlsRequestContext,
  dataSource: SlickDataSource,
  bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
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

  def cacheCounter(stats: JobStatistics.QueryStatistics): Counter =
    spendReportingMetrics.expand(BigQueryKey, BigQueryCacheMetric).asCounter(if (stats.getCacheHit) "hit" else "miss")

  def bytesProcessedCounter: Histogram =
    spendReportingMetrics.expand(BigQueryKey, BigQueryBytesProcessedMetric).asHistogram("bytes")

  private def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(
    op: => Future[T]
  ): Future[T] =
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, ctx.userInfo).flatMap {
      case true => op
      case false =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.Forbidden,
          s"${ctx.userInfo.userEmail.value} cannot perform ${action.value} on project ${projectName.value}"
        )
    }

  def requireAlphaUser[T]()(op: => Future[T]): Future[T] = samDAO
    .userHasAction(
      SamResourceTypeNames.managedGroup,
      "Alpha_Spend_Report_Users",
      SamResourceAction("use"),
      ctx.userInfo
    )
    .flatMap {
      case true => op
      case false =>
        throw RawlsExceptionWithErrorReport(StatusCodes.Forbidden, "This API is not live yet.")
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

  def getWorkspaceGoogleProjects(projectName: RawlsBillingProjectName): Future[BillingToWorkspaceNames] =
    dataSource.inTransaction(_.workspaceQuery.listWithBillingProject(projectName)).map {
      _.collect {
        case w if w.workspaceVersion == WorkspaceVersions.V2 => w.googleProjectId.value -> w.toWorkspaceName
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
    projectNames: BillingToWorkspaceNames
  ): JobInfo = {
    def queryParam(value: String): QueryParameterValue =
      QueryParameterValue.newBuilder().setType(StandardSQLTypeName.STRING).setValue(value).build()

    val projectNamesParam: QueryParameterValue =
      QueryParameterValue
        .newBuilder()
        .setType(StandardSQLTypeName.ARRAY)
        .setArrayType(StandardSQLTypeName.STRING)
        .setArrayValues(projectNames.keySet.map(name => queryParam(name)).toList.asJava)
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
    cacheCounter(stats).inc()
    bytesProcessedCounter += stats.getEstimatedBytesProcessed
  }

  def getSpendForBillingProject(
    project: RawlsBillingProjectName,
    start: DateTime,
    end: DateTime,
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] = {
    validateReportParameters(start, end)
    requireAlphaUser() {
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
  }

}
