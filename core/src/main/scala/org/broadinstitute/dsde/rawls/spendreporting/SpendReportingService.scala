package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.cloud.bigquery.{FieldValueList, JobStatistics, Option => _, _}
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{Counter, Histogram}
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceSpendReportRecord
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, HitRatioGauge, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.SpendReportingAggregationKeys.Category
import org.broadinstitute.dsde.rawls.model.{SpendReportingAggregationKeyWithSub, SpendReportingAggregationKeys, _}
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService._
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceFutureWithParent
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Days}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode
import scala.util.Try

object SpendReportingService {
  def constructor(
    dataSource: SlickDataSource,
    bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
    billingRepository: BillingRepository,
    samDAO: SamDAO,
    spendReportingServiceConfig: SpendReportingServiceConfig,
    workspaceServiceConstructor: RawlsRequestContext => WorkspaceService,
    workspaceSpendReportRepository: WorkspaceSpendReportRepository
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): SpendReportingService =
    new SpendReportingService(
      ctx,
      dataSource,
      bigQueryService,
      billingRepository: BillingRepository,
      samDAO,
      spendReportingServiceConfig,
      workspaceServiceConstructor,
      workspaceSpendReportRepository
    )

  val SpendReportingMetrics = "spendReporting"
  val BigQueryKey = "bigQuery"
  val BigQueryCacheMetric = "cache"
  val BigQueryBytesProcessedMetric = "processed"
  val RawlsKey = "rawls"
  val RawlsCacheMetric = "cache"

  def extractSpendReportingResults(
    allRows: List[FieldValueList],
    start: DateTime,
    end: DateTime,
    names: Map[GoogleProjectId, WorkspaceName],
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): SpendReportingResults = {

    val currency = SpendReportUtils.getCurrency(allRows.map(_.get("currency").getStringValue))

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

  def extractCrossBillingProjectSpendReportingResults(
    allRows: List[FieldValueList],
    start: DateTime,
    end: DateTime,
    names: Map[GoogleProjectId, WorkspaceName]
  ): SpendReportingResults = {

    // Using vars because they will get updated as we process the rows
    var total = BigDecimal(0.0)
    var total_credits = BigDecimal(0.0)

    // TODO: We may want to allow multiple currencies someday
    val currency = SpendReportUtils.getCurrency(allRows.map(_.get("currency").getStringValue))
    val all = allRows.map { row =>
      val currencyString = row.get("currency").getStringValue
      val projectId = row.get("project_id").getStringValue
      val spendReportingForDateRange = WorkspaceSpendReportRecord.toSpendReportAggregation(
        currencyString,
        projectId,
        Some(start),
        Some(end),
        row.get("storage_cost").getDoubleValue.toFloat,
        row.get("storage_credits").getDoubleValue.toFloat,
        row.get("compute_cost").getDoubleValue.toFloat,
        row.get("compute_credits").getDoubleValue.toFloat,
        row.get("other_cost").getDoubleValue.toFloat,
        row.get("other_credits").getDoubleValue.toFloat,
        names
      )
      val total_cost = BigDecimal(spendReportingForDateRange.cost)
      val credits = BigDecimal(spendReportingForDateRange.credits)
      total = total + total_cost
      total_credits = total_credits + credits
      SpendReportingAggregation(SpendReportingAggregationKeys.Workspace, List(spendReportingForDateRange))
    }

    val summary = SpendReportingForDateRange(
      total.toString,
      total_credits.toString,
      currency.toString,
      Option(start),
      Option(end)
    )
    SpendReportingResults(all, summary)
  }

}

class SpendReportingService(
  ctx: RawlsRequestContext,
  dataSource: SlickDataSource,
  bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
  billingRepository: BillingRepository,
  samDAO: SamDAO,
  spendReportingServiceConfig: SpendReportingServiceConfig,
  workspaceServiceConstructor: RawlsRequestContext => WorkspaceService,
  workspaceSpendReportRepository: WorkspaceSpendReportRepository
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

  def rawlsCacheCounter(accessType: String): Counter =
    spendReportingMetrics.expand(RawlsKey, RawlsCacheMetric).asCounter(accessType)

  def rawlsCacheHitRate(): HitRatioGauge =
    spendReportingMetrics.expand(RawlsKey, RawlsCacheMetric).asRatio[HitRatioGauge]("hitRate") {
      new HitRatioGauge(
        rawlsCacheCounter("hits"),
        rawlsCacheCounter("calls")
      )
    }

  def bytesProcessedCounter: Histogram =
    spendReportingMetrics.expand(BigQueryKey, BigQueryBytesProcessedMetric).asHistogram("bytes")

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

  def getSpendExportConfigurations(projects: Seq[RawlsBillingProjectName]): Future[Seq[BillingProjectSpendExport]] =
    dataSource
      .inTransaction(_.rawlsBillingProjectQuery.getBillingProjectsSpendConfiguration(projects))
      .recover { case ex: RawlsException =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.BadRequest,
          ex.getMessage
        )
      }
      .map { exportOptions =>
        exportOptions.collect { case Some(export) => export }

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

  // single-project report: query
  def getQuery(aggregations: Set[SpendReportingAggregationKeyWithSub], config: BillingProjectSpendExport): String = {
    // Unbox potentially many SpendReportingAggregationKeyWithSubs for query,
    // all of which have optional subAggregationKeys and convert to Set[SpendReportingAggregationKey]
    val queryKeys = aggregations.flatMap(a => Set(Option(a.key), a.subAggregationKey).flatten)
    val tableName = config.spendExportTable.getOrElse(spendReportingServiceConfig.defaultTableName)
    val timePartitionColumn: String = getTimePartitionColumn(tableName)

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
       |""".stripMargin.replace("REPLACE_TIME_PARTITION_COLUMN", timePartitionColumn)
  }

  private def getTimePartitionColumn(tableName: String): String = {
    val isBroadTable = tableName == spendReportingServiceConfig.defaultTableName
    // The Broad table uses a view with a different column name.
    if (isBroadTable) spendReportingServiceConfig.defaultTimePartitionColumn else "_PARTITIONTIME"
  }

  // consolidated report: query
  def getAllUserWorkspaceQuery(
    spendExportTable: String,
    billingProjectsByAccount: Map[RawlsBillingAccountName, Seq[GoogleProjectId]],
    pageSize: Int,
    offset: Int
  ): String = {
    val baseQuery = s"""
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
                       |    _BILLING_ACCOUNT_TABLE
                       |  where
                       |    _PARTITIONTIME BETWEEN @startDate AND @endDate and
                       |    (_PROJECT_CLAUSE)
                       |  GROUP BY
                       |    project_id,
                       |    spend_category,
                       |    currency""".stripMargin.trim

    val timePartitionColumn: String = getTimePartitionColumn(spendExportTable)
    val bpSubQuery =
      baseQuery
        .replace("_PARTITIONTIME", timePartitionColumn)
        .replace("_BILLING_ACCOUNT_TABLE", spendExportTable)
        .replace("_PROJECT_CLAUSE", billingProjectsByAccountClause(billingProjectsByAccount))

    s"""WITH spend_categories AS (
       |$bpSubQuery
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
       |limit $pageSize offset $offset
       |""".stripMargin.trim
  }

  def billingProjectsByAccountClause(
    billingProjectsByAccount: Map[RawlsBillingAccountName, Seq[GoogleProjectId]]
  ): String =
    billingProjectsByAccount
      .map { case (billingAccount, projects) =>
        val quotedProjects = projects.map(p => s"'${p.value}'")
        s" (billing_account_id = '${billingAccount.withoutPrefix()}' and project.id in ${quotedProjects.mkString("(", ", ", ")")}) "
      }
      .mkString(" or ")

  // single-project report: query parameters
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

  def setUpAllUserWorkspaceQuery(
    query: String,
    start: DateTime,
    end: DateTime
  ): JobInfo = {
    def queryParam(value: String): QueryParameterValue =
      QueryParameterValue.newBuilder().setType(StandardSQLTypeName.STRING).setValue(value).build()

    val queryConfig = QueryJobConfiguration
      .newBuilder(query)
      .addNamedParameter("startDate", queryParam(toISODateString(start)))
      .addNamedParameter("endDate", queryParam(toISODateString(end)))
      .build()

    JobInfo.newBuilder(queryConfig).build()
  }

  def logSpendQueryStats(stats: JobStatistics.QueryStatistics): Unit = {
    if (stats.getCacheHit) cacheHitRate().hit() else cacheHitRate().miss()
    bytesProcessedCounter += stats.getEstimatedBytesProcessed
  }

  case class CachedSpendReportingResults(isCacheValid: Boolean, results: Option[SpendReportingResults])

  /**
    * Query the spend report cache and inspect the cache results to see if they are valid.
    * @param workspaceNamesByProjectId workspaces/project for which to query
    * @param start beginning of report timeframe
    * @param end end of report timeframe
    * @return potentially-valid cache results
    */
  def getCachedSpendReportData(workspaceNamesByProjectId: Map[GoogleProjectId, WorkspaceName],
                               start: DateTime,
                               end: DateTime
  ): Future[CachedSpendReportingResults] = {
    // set up
    val startLocalDateTime = SpendReportUtils.convertJodaToJava(Some(start))
    val endLocalDateTime = SpendReportUtils.convertJodaToJava(Some(end))
    val projectIds = workspaceNamesByProjectId.keySet.map(_.value)

    // query the cache for possible results
    val cachedResultsFuture =
      workspaceSpendReportRepository.getWorkspaceSpendReports(projectIds, startLocalDateTime, endLocalDateTime)

    cachedResultsFuture.map { cachedResult =>
      // we consider cached results valid if they return one row for each
      // requested google project
      if (cachedResult.size == projectIds.size) {
        // we may have cached that some projects have no data in this report;
        // handle those cases
        val hasDataAvailable = cachedResult.filter(cached => cached.isDataAvailable)
        if (hasDataAvailable.nonEmpty) {
          val spendReportingResults =
            WorkspaceSpendReportRecord.toSpendReportingResults(hasDataAvailable, start, end, workspaceNamesByProjectId)
          CachedSpendReportingResults(isCacheValid = true, Option(spendReportingResults))
        } else {
          CachedSpendReportingResults(isCacheValid = true, None)
        }
      } else {
        // cached results are not valid for these projects/this timeframe
        CachedSpendReportingResults(isCacheValid = false, None)
      }
    }
  }

  // shared between single-project and consolidated reports
  def getSpendReportableWorkspaceGoogleProjects(
    childContext: RawlsRequestContext
  ): Future[Map[RawlsBillingProjectName, Seq[Workspace]]] =
    samDAO
      .listResourcesWithActions(
        SamResourceTypeNames.workspace,
        SamWorkspaceActions.readSpendReport,
        childContext
      )
      .flatMap { ownerWorkspaces =>
        val validWorkspaceIds = ownerWorkspaces
          .map(_.getResourceId)
          .filter(resourceId => Try(UUID.fromString(resourceId)).isSuccess) // filter out non-UUIDs
          .toList
        workspaceServiceConstructor(childContext).getGCPWorkspacesByBillingProjects(validWorkspaceIds)
      }

  /* as of this writing we only support Workspace~Category for caching */
  @VisibleForTesting
  def aggregationsAreCacheable(aggregations: Set[SpendReportingAggregationKeyWithSub]): Boolean =
    Set(SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace, Option(Category)))
      .equals(aggregations)

  /* reusable method to retrieve per-project spend report from BQ */
  private def getSpendForGCPBillingProjectFromBigQuery(
    aggregations: Set[SpendReportingAggregationKeyWithSub],
    spendExportConf: BillingProjectSpendExport,
    start: DateTime,
    end: DateTime,
    projectNames: Map[GoogleProjectId, WorkspaceName],
    childContext: RawlsRequestContext
  ): Future[Option[SpendReportingResults]] = {
    val query = getQuery(aggregations, spendExportConf)
    val queryJob = setUpQuery(query, spendExportConf, start, end, projectNames)
    runBigQueryJob(queryJob, childContext).map { result =>
      result.getValues.asScala.toList match {
        case Nil  => None
        case rows => Option(extractSpendReportingResults(rows, start, end, projectNames, aggregations))
      }
    }
  }

  // single-project report: step 3
  def getSpendForGCPBillingProject(
    project: RawlsBillingProjectName,
    start: DateTime,
    end: DateTime,
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] = traceFutureWithParent("getSpendForGCPBillingProject", ctx) { childContext =>
    validateReportParameters(start, end)
    for {
      // retrieve spend export configuration for this BP
      // and the Google projects/workspaces in this BP
      spendExportConf <- getSpendExportConfiguration(project)
      projectNames <- getSpendReportableWorkspaceGoogleProjects(ctx).map { workspacesByProject =>
        workspacesByProject
          .getOrElse(project, Seq.empty)
          .map { workspace =>
            workspace.googleProjectId -> workspace.toWorkspaceName
          }
          .toMap
      }
      _ = if (projectNames.isEmpty) {
        throw RawlsExceptionWithErrorReport(
          StatusCodes.NotFound,
          s"no spend data found for billing project ${project.value} between dates ${toISODateString(start)} and ${toISODateString(end)}"
        )
      }

      // is this spend report cacheable?
      isCacheable = aggregationsAreCacheable(aggregations)

      queryResults: Option[SpendReportingResults] <-
        if (isCacheable) {
          // look for a cached spend report
          getCachedSpendReportData(projectNames, start, end) flatMap { cachedResults =>
            if (cachedResults.isCacheValid) {
              // cached spend report found; use it
              rawlsCacheHitRate().hit()
              Future.successful(cachedResults.results)
            } else {
              // cached spend report not found; go to BigQuery for results
              rawlsCacheHitRate().miss()
              getSpendForGCPBillingProjectFromBigQuery(aggregations,
                                                       spendExportConf,
                                                       start,
                                                       end,
                                                       projectNames,
                                                       childContext
              ) map { bqResults =>
                // save the BigQuery results back to cache and return
                // the writeback to cache is fire-and-forget
                insertRecordsWithMissingSpendData(bqResults, projectNames, start, end)
                bqResults
              }
            }
          }
        } else {
          // this spend report is not cacheable; bypass the cache
          // and ask BigQuery directly
          getSpendForGCPBillingProjectFromBigQuery(aggregations,
                                                   spendExportConf,
                                                   start,
                                                   end,
                                                   projectNames,
                                                   childContext
          )
        }

      spendResults = queryResults match {
        case Some(results) => results
        case None =>
          throw RawlsExceptionWithErrorReport(
            StatusCodes.NotFound,
            s"no spend data found for billing project ${project.value} between dates ${toISODateString(start)} and ${toISODateString(end)}"
          )
      }
    } yield spendResults

  }

  // single-project report: entry point
  def getSpendForBillingProject(
    project: RawlsBillingProjectName,
    start: DateTime,
    end: DateTime,
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] =
    for {
      billingProject <- billingRepository.getBillingProject(project)

      report <- getReportData(billingProject.get, project, start, end, aggregations)
    } yield report

  // single-project report: step 2
  private def getReportData(billingProject: RawlsBillingProject,
                            project: RawlsBillingProjectName,
                            start: DateTime,
                            end: DateTime,
                            aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): Future[SpendReportingResults] = getSpendForGCPBillingProject(project, start, end, aggregations)

  // consolidated report: entry point
  def getSpendForAllWorkspaces(
    start: DateTime,
    end: DateTime,
    pageSize: Int,
    offset: Int
  ): Future[Option[SpendReportingResults]] =
    traceFutureWithParent("getSpendForAllWorkspaces", ctx) { childContext =>
      validateReportParameters(start, end)
      for {
        // get spend-reportable workspaces, grouped by their spend export config
        // Map[BillingProjectSpendExport, Seq[Workspace]]
        billingMap <- getBillingWithSpendPermission(childContext)
        _ = if (billingMap.isEmpty) {
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.NotFound, "No workspaces eligible for spend report")
          )
        }

        // find the distinct export table names in our billingMap
        distinctTableNames: Set[Option[String]] = billingMap.keys.map(_.spendExportTable).toSet
        // for each distinct export table name, generate a query.
        results <- Future.sequence(distinctTableNames.map { spendExportTable =>
          // find the subset of the billingMap that we'll use in this query
          val billingMapForQuery = billingMap.filter(_._1.spendExportTable == spendExportTable)
          // use default export table if none is specified
          val tableNameForQuery = spendExportTable.getOrElse(spendReportingServiceConfig.defaultTableName)
          // find the (billingAccount, billingProjects) pairs for this query
          val billingProjectsByAccount: Map[RawlsBillingAccountName, Seq[GoogleProjectId]] =
            billingMapForQuery
              .groupMap(_._1.billingAccountId)(_._2)
              .view
              .mapValues(_.flatten.map(_.googleProjectId).toSeq)
              .toMap

          // and finally, generate a map of GoogleProjectId->WorkspaceName for extracting the spend results
          val workspaceNamesByProjectId: Map[GoogleProjectId, WorkspaceName] = billingMapForQuery.values.flatten
            .map(ws => ws.googleProjectId -> ws.toWorkspaceName)
            .toMap

          // look for results in cache
          val spendResults = getCachedSpendReportData(workspaceNamesByProjectId, start, end).flatMap { cachedResults =>
            if (cachedResults.isCacheValid) {
              // we have valid cache results; use them
              rawlsCacheHitRate().hit()
              Future.successful(cachedResults.results)
            } else {
              // cached results are invalid; ask BigQuery for results
              rawlsCacheHitRate().miss()
              val query = getAllUserWorkspaceQuery(tableNameForQuery, billingProjectsByAccount, pageSize, offset)
              val queryJob = setUpAllUserWorkspaceQuery(query, start, end)
              val queryResults = runBigQueryJob(queryJob, childContext)
                .map { result =>
                  result.getValues.asScala.toList match {
                    case Nil => None
                    case rows =>
                      val crossBillingResults =
                        extractCrossBillingProjectSpendReportingResults(rows, start, end, workspaceNamesByProjectId)
                      workspaceSpendReportRepository.insertSpendReportResults(crossBillingResults)
                      Some(crossBillingResults)
                  }
                }
                .recoverWith { case ex: Throwable =>
                  logger.warn(s"Error fetching results from BigQuery: ${ex.getMessage}")
                  Future.successful(None)
                }
              // write BigQuery results back to cache
              queryResults.map { res =>
                insertRecordsWithMissingSpendData(res, workspaceNamesByProjectId, start, end)
              }
              queryResults
            }
          }
          spendResults
        })
        combinedResults = results.flatten.reduceOption((acc, res) => acc + res)
      } yield combinedResults
    }

  def insertRecordsWithMissingSpendData(result: Option[SpendReportingResults],
                                        projectNames: Map[GoogleProjectId, WorkspaceName],
                                        start: DateTime,
                                        end: DateTime
  ): Set[Future[Long]] = {
    val googleProjectsWithReports: Set[GoogleProjectId] =
      if (result.nonEmpty) {
        result.get.spendDetails
          .flatMap(_.spendData)
          .flatMap(_.googleProjectId)
          .map(p => GoogleProjectId(p.value))
          .toSet
      } else {
        Set.empty
      }
    // find all supplied project names which are not in the SpendReportingResults projects
    val missingProjectIds: Set[GoogleProjectId] = projectNames.keySet diff googleProjectsWithReports
    val startLocalDateTime = SpendReportUtils.convertJodaToJava(Some(start))
    val endLocalDateTime = SpendReportUtils.convertJodaToJava(Some(end))
    val insertedRecords = missingProjectIds.map { id =>
      val spendReport = WorkspaceSpendReport.newEmptySpendReport(
        id.toString(),
        startLocalDateTime,
        endLocalDateTime,
        "USD"
      )
      workspaceSpendReportRepository.insertWorkspaceSpendReport(spendReport).recoverWith { case ex: Throwable =>
        logger.warn(s"Error inserting workspace spend report: ${ex.getMessage}")
        Future.successful(0L)
      }
    }
    insertedRecords
  }

  def runBigQueryJob(queryJob: JobInfo, ctx: RawlsRequestContext): Future[TableResult] =
    traceFutureWithParent("runBigQueryJob", ctx) { childContext =>
      for {
        job: Job <- bigQueryService.use(_.runJob(queryJob)).unsafeToFuture().map(_.waitFor())
        _ = logSpendQueryStats(job.getStatistics[JobStatistics.QueryStatistics])
        result = job.getQueryResults()
      } yield result
    }

  def getBillingWithSpendPermission(
    parentContext: RawlsRequestContext
  ): Future[Map[BillingProjectSpendExport, Seq[Workspace]]] =
    traceFutureWithParent("getBillingWithSpendPermission", parentContext) { childContext =>
      for {
        // Retrieve the workspaces for which the user is allowed to report on spend;
        // these are grouped by billing project
        workspacesByBillingProjectName <- getSpendReportableWorkspaceGoogleProjects(childContext)
        // Retrieve the spend-export configs for each of these billing projects;
        // this verifies the billing projects exist in the DB and are GCP
        spendConfigs <-
          if (workspacesByBillingProjectName.isEmpty) {
            Future.successful(Seq.empty[BillingProjectSpendExport])
          } else {
            getSpendExportConfigurations(workspacesByBillingProjectName.keys.toList)
          }

        // Match the workspaces to their spend configs. Filter out any spend configs which
        // have no workspaces (this is not expected to happen)
        workspacesBySpendConfig = spendConfigs
          .map { config =>
            config -> workspacesByBillingProjectName.getOrElse(config.billingProjectName, Seq.empty[Workspace])
          }
          .toMap
          .filter(_._2.nonEmpty)

      } yield workspacesBySpendConfig
    }
}
