package org.broadinstitute.dsde.rawls.spendreporting

import java.util.Currency

import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.cloud.bigquery.{Option => _, _}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.SpendReportingAggregationKeys.SpendReportingAggregationKey
import org.broadinstitute.dsde.rawls.model.TerraSpendCategories.TerraSpendCategory
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

object SpendReportingService {
  def constructor(dataSource: SlickDataSource,
                  bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
                  samDAO: SamDAO,
                  spendReportingServiceConfig: SpendReportingServiceConfig
  )(userInfo: UserInfo)(implicit executionContext: ExecutionContext): SpendReportingService =
    new SpendReportingService(userInfo, dataSource, bigQueryService, samDAO, spendReportingServiceConfig)
}

class SpendReportingService(userInfo: UserInfo,
                            dataSource: SlickDataSource,
                            bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]],
                            samDAO: SamDAO,
                            spendReportingServiceConfig: SpendReportingServiceConfig
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging {
  private def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(
    op: => Future[T]
  ): Future[T] =
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false =>
        Future.failed(
          new RawlsExceptionWithErrorReport(
            errorReport =
              ErrorReport(StatusCodes.Forbidden,
                          s"${userInfo.userEmail.value} cannot perform ${action.value} on project ${projectName.value}"
              )
          )
        )
    }

  private def requireAlphaUser[T]()(op: => Future[T]): Future[T] =
    samDAO
      .userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo)
      .flatMap {
        case true => op
        case false =>
          Future.failed(
            new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "This API is not live yet."))
          )
      }

  def extractSpendReportingResults(rows: List[FieldValueList],
                                   startTime: DateTime,
                                   endTime: DateTime,
                                   workspaceProjectsToNames: Map[GoogleProject, WorkspaceName],
                                   aggregationKeys: Set[SpendReportingAggregationKeyWithSub]
  ): SpendReportingResults = {
    val currency = getCurrency(rows)
    val spendAggregations = aggregationKeys.map { aggregationKey =>
      extractSpendAggregation(rows, currency, aggregationKey, workspaceProjectsToNames)
    }
    val spendSummary = extractSpendSummary(rows, currency, startTime, endTime)

    SpendReportingResults(spendAggregations.toList, spendSummary)
  }

  /**
    * Ensure that BigQuery results only include one type of currency and return that currency.
    */
  private def getCurrency(rows: List[FieldValueList]): Currency = {
    val currencies = rows.map(_.get("currency").getStringValue)

    Currency.getInstance(currencies.reduce { (x, y) =>
      if (x.equals(y)) {
        x
      } else {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadGateway,
                      s"Inconsistent currencies found while aggregating spend data: $x and $y cannot be combined"
          )
        )
      }
    })
  }

  private def extractSpendAggregation(rows: List[FieldValueList],
                                      currency: Currency,
                                      aggregationKey: SpendReportingAggregationKeyWithSub,
                                      workspaceProjectsToNames: Map[GoogleProject, WorkspaceName] = Map.empty
  ): SpendReportingAggregation =
    aggregationKey match {
      case SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category, subAggregationKey) =>
        extractCategorySpendAggregation(rows, currency, subAggregationKey, workspaceProjectsToNames)
      case SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace, subAggregationKey) =>
        extractWorkspaceSpendAggregation(rows, currency, subAggregationKey, workspaceProjectsToNames)
      case SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Daily, subAggregationKey) =>
        extractDailySpendAggregation(rows, currency, subAggregationKey, workspaceProjectsToNames)
    }

  private def extractSpendSummary(rows: List[FieldValueList],
                                  currency: Currency,
                                  startTime: DateTime,
                                  endTime: DateTime
  ): SpendReportingForDateRange = {
    val (cost, credits) = sumCostsAndCredits(rows, currency)

    SpendReportingForDateRange(
      cost.toString(),
      credits.toString(),
      currency.getCurrencyCode,
      Option(startTime),
      Option(endTime)
    )
  }

  private def sumCostsAndCredits(rows: List[FieldValueList], currency: Currency): (BigDecimal, BigDecimal) =
    (
      rows
        .map(row => BigDecimal(row.get("cost").getDoubleValue))
        .sum
        .setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN),
      rows
        .map(row => BigDecimal(row.get("credits").getDoubleValue))
        .sum
        .setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
    )

  private def extractWorkspaceSpendAggregation(rows: List[FieldValueList],
                                               currency: Currency,
                                               subAggregationKey: Option[SpendReportingAggregationKey] = None,
                                               workspaceProjectsToNames: Map[GoogleProject, WorkspaceName]
  ): SpendReportingAggregation = {
    val spendByGoogleProjectId: Map[GoogleProject, List[FieldValueList]] =
      rows.groupBy(row => GoogleProject(row.get("googleProjectId").getStringValue))
    val workspaceSpend = spendByGoogleProjectId.map { case (googleProjectId, rowsForGoogleProjectId) =>
      val (cost, credits) = sumCostsAndCredits(rowsForGoogleProjectId, currency)
      val subAggregation = subAggregationKey.map { key =>
        extractSpendAggregation(rowsForGoogleProjectId,
                                currency,
                                aggregationKey = SpendReportingAggregationKeyWithSub(key),
                                workspaceProjectsToNames = workspaceProjectsToNames
        )
      }
      val workspaceName = workspaceProjectsToNames.getOrElse(
        googleProjectId,
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadGateway, s"unexpected project ${googleProjectId.value} returned by BigQuery")
        )
      )
      SpendReportingForDateRange(
        cost.toString(),
        credits.toString(),
        currency.getCurrencyCode,
        workspace = Option(workspaceName),
        googleProjectId = Option(googleProjectId),
        subAggregation = subAggregation
      )
    }.toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Workspace,
      workspaceSpend
    )
  }

  private def extractCategorySpendAggregation(rows: List[FieldValueList],
                                              currency: Currency,
                                              subAggregationKey: Option[SpendReportingAggregationKey] = None,
                                              workspaceProjectsToNames: Map[GoogleProject, WorkspaceName] = Map.empty
  ): SpendReportingAggregation = {
    val spendByCategory: Map[TerraSpendCategory, List[FieldValueList]] =
      rows.groupBy(row => TerraSpendCategories.categorize(row.get("service").getStringValue))
    val categorySpend = spendByCategory.map { case (category, rowsForCategory) =>
      val (cost, credits) = sumCostsAndCredits(rowsForCategory, currency)
      val subAggregation = subAggregationKey.map { key =>
        extractSpendAggregation(rowsForCategory,
                                currency,
                                aggregationKey = SpendReportingAggregationKeyWithSub(key),
                                workspaceProjectsToNames = workspaceProjectsToNames
        )
      }
      SpendReportingForDateRange(
        cost.toString,
        credits.toString,
        currency.getCurrencyCode,
        category = Option(category),
        subAggregation = subAggregation
      )
    }.toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Category,
      categorySpend
    )
  }

  private def extractDailySpendAggregation(rows: List[FieldValueList],
                                           currency: Currency,
                                           subAggregationKey: Option[SpendReportingAggregationKey] = None,
                                           workspaceProjectsToNames: Map[GoogleProject, WorkspaceName] = Map.empty
  ): SpendReportingAggregation = {
    val spendByStartTime: Map[DateTime, List[FieldValueList]] =
      rows.groupBy(row => DateTime.parse(row.get("date").getStringValue))
    val dailySpend = spendByStartTime.map { case (startTime, rowsForStartTime) =>
      val (cost, credits) = sumCostsAndCredits(rowsForStartTime, currency)
      val subAggregation = subAggregationKey.map { key =>
        extractSpendAggregation(rowsForStartTime,
                                currency,
                                aggregationKey = SpendReportingAggregationKeyWithSub(key),
                                workspaceProjectsToNames = workspaceProjectsToNames
        )
      }
      SpendReportingForDateRange(
        cost.toString,
        credits.toString,
        currency.getCurrencyCode,
        Option(startTime),
        endTime = Option(startTime.plusDays(1).minusMillis(1)),
        subAggregation = subAggregation
      )
    }.toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Daily,
      dailySpend
    )
  }

  private def dateTimeToISODateString(dt: DateTime): String = dt.toString(ISODateTimeFormat.date())

  private def getSpendExportConfiguration(
    billingProjectName: RawlsBillingProjectName
  ): Future[BillingProjectSpendExport] =
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

  private def getWorkspaceGoogleProjects(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[GoogleProject, WorkspaceName]] =
    dataSource
      .inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
      }
      .map { workspaces =>
        workspaces.collect {
          case workspace if workspace.workspaceVersion == WorkspaceVersions.V2 =>
            GoogleProject(workspace.googleProjectId.value) -> workspace.toWorkspaceName
        }.toMap
      }

  private def validateReportParameters(startDate: DateTime, endDate: DateTime): Unit =
    if (startDate.isAfter(endDate)) {
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

  def getSpendForBillingProject(billingProjectName: RawlsBillingProjectName,
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
          case rows =>
            extractSpendReportingResults(rows, startDate, endDate, workspaceProjectsToNames, aggregationKeyParameters)
        }
      }
    }
  }
}
