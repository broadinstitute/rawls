package org.broadinstitute.dsde.rawls.spendreporting

import java.util.Currency
import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.cloud.bigquery.{Option => _, _}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.SpendReportingAggregationKeys.SpendReportingAggregationKey
import org.broadinstitute.dsde.rawls.model.{SpendReportingAggregationKeyWithSub, _}
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

  def extractSpendReportingResults(
    allRows: List[FieldValueList],
    start: DateTime,
    end: DateTime,
    workspaceProjectsToNames: Map[GoogleProject, WorkspaceName],
    aggregations: Set[SpendReportingAggregationKeyWithSub]
  ): SpendReportingResults = {

    val currency = allRows.map(_.get("currency").getStringValue).distinct match {
      case head :: List() => Currency.getInstance(head)
      case head :: tail =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(
            StatusCodes.BadGateway, // todo: Probably the wrong status code
            s"Inconsistent currencies found while aggregating spend data: $head and ${tail.head} cannot be combined"
          )
        )
      case List() =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound, "No currencies found while aggregating spend data")
        )
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
      .groupBy(row => GoogleProject(row.get("googleProjectId").getStringValue))
      .map { case (googleProjectId, projectRows) =>
        val workspaceName = workspaceProjectsToNames.getOrElse(
          googleProjectId,
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadGateway, s"unexpected project ${googleProjectId.value} returned by BigQuery")
          )
        )
        SpendReportingForDateRange(
          sum(projectRows, "cost"),
          sum(projectRows, "credits"),
          currency.getCurrencyCode,
          workspace = Option(workspaceName),
          googleProjectId = Option(googleProjectId),
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
      val SpendReportingAggregationKeyWithSub(aggregationKey, subKey) = key
      val spend = aggregationKey match {
        case SpendReportingAggregationKeys.Category  => byCategory(rows, subKey)
        case SpendReportingAggregationKeys.Workspace => byWorkspace(rows, subKey)
        case SpendReportingAggregationKeys.Daily     => byDate(rows, subKey)
      }
      SpendReportingAggregation(aggregationKey, spend)
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

  private def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(
    op: => Future[T]
  ): Future[T] =
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, ctx.userInfo).flatMap {
      case true => op
      case false =>
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(
            StatusCodes.Forbidden,
            s"${ctx.userInfo.userEmail.value} cannot perform ${action.value} on project ${projectName.value}"
          )
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
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "This API is not live yet."))
    }

  private def dateTimeToISODateString(dt: DateTime): String = dt.toString(ISODateTimeFormat.date())

  def getSpendExportConfiguration(billingProjectName: RawlsBillingProjectName): Future[BillingProjectSpendExport] =
    dataSource
      .inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.getBillingProjectSpendConfiguration(billingProjectName)
      }
      .recover { case _: RawlsException =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest,
                      s"billing account not found on billing project ${billingProjectName.value}"
          )
        )
      }
      .map(
        _.getOrElse(
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.NotFound, s"billing project ${billingProjectName.value} not found")
          )
        )
      )

  def getWorkspaceGoogleProjects(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[GoogleProject, WorkspaceName]] = dataSource
    .inTransaction { dataAccess =>
      dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
    }
    .map { workspaces =>
      workspaces.collect {
        case workspace if workspace.workspaceVersion == WorkspaceVersions.V2 =>
          GoogleProject(workspace.googleProjectId.value) -> workspace.toWorkspaceName
      }.toMap
    }

  def validateReportParameters(startDate: DateTime, endDate: DateTime): Unit = if (startDate.isAfter(endDate)) {
    throw new RawlsExceptionWithErrorReport(
      ErrorReport(
        StatusCodes.BadRequest,
        s"start date ${dateTimeToISODateString(startDate)} must be before end date ${dateTimeToISODateString(endDate)}"
      )
    )
  } else if (Days.daysBetween(startDate, endDate).getDays > spendReportingServiceConfig.maxDateRange) {
    throw new RawlsExceptionWithErrorReport(
      ErrorReport(
        StatusCodes.BadRequest,
        s"provided dates exceed maximum report date range of ${spendReportingServiceConfig.maxDateRange} days"
      )
    )
  }

  private def stringQueryParameterValue(parameterValue: String): QueryParameterValue =
    QueryParameterValue
      .newBuilder()
      .setType(StandardSQLTypeName.STRING)
      .setValue(parameterValue)
      .build()

  private def stringArrayQueryParameterValue(parameterValues: List[String]): QueryParameterValue = {
    val queryParameterArrayValues = parameterValues.map { parameterValue =>
      QueryParameterValue
        .newBuilder()
        .setType(StandardSQLTypeName.STRING)
        .setValue(parameterValue)
        .build()
    }.asJava

    QueryParameterValue
      .newBuilder()
      .setType(StandardSQLTypeName.ARRAY)
      .setArrayType(StandardSQLTypeName.STRING)
      .setArrayValues(queryParameterArrayValues)
      .build()
  }

  def getQuery(aggregationKeys: Set[SpendReportingAggregationKey],
               tableName: String,
               customTimePartitionColumn: Option[String]
  ): String = {
    // The Broad table uses a view with a different column name.
    val timePartitionColumn = customTimePartitionColumn.getOrElse("_PARTITIONTIME")
    val queryClause = s"""
                         | SELECT
                         |  SUM(cost) as cost,
                         |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
                         |  currency ${aggregationKeys.map(_.bigQueryAliasClause()).mkString}
                         | FROM `$tableName`
                         | WHERE billing_account_id = @billingAccountId
                         | AND $timePartitionColumn BETWEEN @startDate AND @endDate
                         | AND project.id in UNNEST(@projects)
                         | GROUP BY currency ${aggregationKeys.map(_.bigQueryGroupByClause()).mkString}
                         |""".stripMargin
    queryClause.replace("REPLACE_TIME_PARTITION_COLUMN", timePartitionColumn)
  }

  def getSpendForBillingProject(
    billingProjectName: RawlsBillingProjectName,
    startDate: DateTime,
    endDate: DateTime,
    aggregationKeyParameters: Set[SpendReportingAggregationKeyWithSub] = Set.empty
  ): Future[SpendReportingResults] = {
    validateReportParameters(startDate, endDate)
    requireAlphaUser() {
      requireProjectAction(billingProjectName, SamBillingProjectActions.readSpendReport) {
        for {
          spendExportConf <- getSpendExportConfiguration(billingProjectName)
          workspaceProjectsToNames <- getWorkspaceGoogleProjects(billingProjectName)

          // Unbox potentially many SpendReportingAggregationKeyWithSubs, all of which have optional subAggregationKeys and convert to Set[SpendReportingAggregationKey]
          aggregationKeys = aggregationKeyParameters.flatMap(maybeKeys =>
            Set(Option(maybeKeys.key), maybeKeys.subAggregationKey).flatten
          )

          spendReportTableName = spendExportConf.spendExportTable.getOrElse(
            spendReportingServiceConfig.defaultTableName
          )
          isBroadTable = spendReportTableName == spendReportingServiceConfig.defaultTableName
          timePartitionColumn = if (isBroadTable) Some(spendReportingServiceConfig.defaultTimePartitionColumn) else None
          query = getQuery(aggregationKeys, spendReportTableName, timePartitionColumn)

          queryJobConfiguration = QueryJobConfiguration
            .newBuilder(query)
            .addNamedParameter("billingAccountId",
                               stringQueryParameterValue(spendExportConf.billingAccountId.withoutPrefix())
            )
            .addNamedParameter("startDate", stringQueryParameterValue(dateTimeToISODateString(startDate)))
            .addNamedParameter("endDate", stringQueryParameterValue(dateTimeToISODateString(endDate)))
            .addNamedParameter("projects",
                               stringArrayQueryParameterValue(workspaceProjectsToNames.keySet.map(_.value).toList)
            )
            .build()

          result <- bigQueryService.use(_.query(queryJobConfiguration)).unsafeToFuture()
        } yield result.getValues.asScala.toList match {
          case Nil =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(
                StatusCodes.NotFound,
                s"no spend data found for billing project ${billingProjectName.value} between dates ${dateTimeToISODateString(startDate)} and ${dateTimeToISODateString(endDate)}"
              )
            )
          case rows => SpendReportingService
            .extractSpendReportingResults(rows, startDate, endDate, workspaceProjectsToNames, aggregationKeyParameters)
        }
      }
    }
  }
}
