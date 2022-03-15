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
  def constructor(dataSource: SlickDataSource, bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]], samDAO: SamDAO, spendReportingServiceConfig: SpendReportingServiceConfig)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SpendReportingService = {
    new SpendReportingService(userInfo, dataSource, bigQueryService, samDAO, spendReportingServiceConfig)
  }
}

class SpendReportingService(userInfo: UserInfo, dataSource: SlickDataSource, bigQueryService: cats.effect.Resource[IO, GoogleBigQueryService[IO]], samDAO: SamDAO, spendReportingServiceConfig: SpendReportingServiceConfig)
                           (implicit val executionContext: ExecutionContext) extends LazyLogging {
  private def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(op: => Future[T]): Future[T] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"${userInfo.userEmail.value} cannot perform ${action.value} on project ${projectName.value}")))
    }
  }

  private def requireAlphaUser[T]()(op: => Future[T]): Future[T] = {
    samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "This API is not live yet.")))
    }
  }

  def extractSpendReportingResults(rows: List[FieldValueList],
                                   startTime: DateTime,
                                   endTime: DateTime,
                                   workspaceProjectsToNames: Map[GoogleProject, WorkspaceName],
                                   aggregationKey: Option[SpendReportingAggregationKey],
                                   subAggregationKey: Option[SpendReportingAggregationKey]): SpendReportingResults = {
    val currency = getCurrency(rows)

    val spendSummary = extractSpendSummary(rows, currency, startTime, endTime)

    val spendAggregation = aggregationKey.map {
      case SpendReportingAggregationKeys.Daily => extractDailySpendAggregation(rows, currency)
      case SpendReportingAggregationKeys.Workspace => extractWorkspaceSpendAggregation(rows, currency, startTime, endTime, workspaceProjectsToNames)
      case SpendReportingAggregationKeys.Category => extractCategorySpendAggregation(rows, currency, startTime, endTime)
    }.toList

    val fullyAggregatedSpend = spendAggregation.map { aggregation =>
      aggregation.copy(
        spendData = aggregation.spendData.map { data =>
          val relevantRows = aggregationKey match {
            case Some(SpendReportingAggregationKeys.Daily) => rows.filter(row => DateTime.parse(row.get("date").getStringValue).equals(data.startTime))
            case Some(SpendReportingAggregationKeys.Workspace) => rows.filter(row => row.get("googleProjectId").getStringValue.equals(data.googleProjectId.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "oh no"))).value))
            case Some(SpendReportingAggregationKeys.Category) => rows.filter(row => TerraSpendCategories.categorize(row.get("service").getStringValue) == data.service.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "oh no"))))
            case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "cannot return sub-aggregated data without a top-level aggregation key"))
          }
          data.copy(
            subAggregation = subAggregationKey.map {
              case SpendReportingAggregationKeys.Daily => extractDailySpendSubAggregation(relevantRows, currency, data)
              case SpendReportingAggregationKeys.Workspace => extractWorkspaceSpendSubAggregation(relevantRows, currency, startTime, endTime, workspaceProjectsToNames, data)
              case SpendReportingAggregationKeys.Category => extractCategorySpendSubAggregation(relevantRows, currency, startTime, endTime, data)
            }
          )
        }
      )
    }

    SpendReportingResults(fullyAggregatedSpend, spendSummary)
  }

  private def extractCategorySpendSubAggregation(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime, parentAggregation: SpendReportingForDateRange): SpendReportingAggregation = {
    val categoryAggregation = extractCategorySpendAggregation(rows, currency, startTime, endTime)
    val subAggregatedCategorySpend = categoryAggregation.spendData.map { categorySpend =>
      parentAggregation.copy(
        cost = categorySpend.cost,
        credits = categorySpend.credits,
        service = categorySpend.service
      )
    }

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Category,
      subAggregatedCategorySpend
    )
  }

  private def extractWorkspaceSpendSubAggregation(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime, workspaceProjectsToNames: Map[GoogleProject, WorkspaceName], parentAggregation: SpendReportingForDateRange): SpendReportingAggregation = {
    val workspaceAggregation = extractWorkspaceSpendAggregation(rows, currency, startTime, endTime, workspaceProjectsToNames)
    val subAggregatedWorkspaceSpend = workspaceAggregation.spendData.map { workspaceSpend =>
      parentAggregation.copy(
        cost = workspaceSpend.cost,
        credits = workspaceSpend.credits,
        workspace = workspaceSpend.workspace,
        googleProjectId = workspaceSpend.googleProjectId
      )
    }

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Workspace,
      subAggregatedWorkspaceSpend
    )
  }

  private def extractDailySpendSubAggregation(rows: List[FieldValueList], currency: Currency, parentAggregation: SpendReportingForDateRange): SpendReportingAggregation = {
    val dailyAggregation = extractDailySpendAggregation(rows, currency)
    val subAggregatedDailySpend = dailyAggregation.spendData.map { dailySpend =>
      parentAggregation.copy(
        cost = dailySpend.cost,
        credits = dailySpend.credits,
        startTime = dailySpend.startTime,
        endTime = dailySpend.endTime
      )
    }

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Daily,
      subAggregatedDailySpend
    )
  }

  private def sumCostsAndCredits(rows: List[FieldValueList], currency: Currency): (BigDecimal, BigDecimal) = {
    (
      rows.map(row => BigDecimal(row.get("cost").getDoubleValue)).sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN),
      rows.map(row => BigDecimal(row.get("credits").getDoubleValue)).sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
    )
  }

  private def extractWorkspaceSpendAggregation(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime, workspaceProjectsToNames: Map[GoogleProject, WorkspaceName]): SpendReportingAggregation = {
    val spendByGoogleProjectId = rows.groupBy(row => GoogleProject(row.get("googleProjectId").getStringValue))
    val workspaceSpend = spendByGoogleProjectId.map { case (googleProjectId, rowsForGoogleProjectId) =>
      val (cost, credits) = sumCostsAndCredits(rowsForGoogleProjectId, currency)
      val workspaceName = workspaceProjectsToNames.getOrElse(googleProjectId, throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, s"unexpected project ${googleProjectId.value} returned by BigQuery")))
      SpendReportingForDateRange(cost.toString(),
        credits.toString(),
        currency.getCurrencyCode,
        startTime,
        endTime,
        workspace = Option(workspaceName),
        googleProjectId = Option(googleProjectId))
    }.toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Workspace, workspaceSpend
    )
  }

  private def extractCategorySpendAggregation(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime): SpendReportingAggregation = {
    val spendByService = rows.groupBy(row => TerraSpendCategories.categorize(row.get("service").getStringValue))
    val categorySpend = spendByService.map { case (service, rowsForService) =>
      val cost = rowsForService.map(row => BigDecimal(row.get("cost").getDoubleValue)).sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val credits = rowsForService.map(row => BigDecimal(row.get("credits").getDoubleValue)).sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      SpendReportingForDateRange(
        cost.toString,
        credits.toString,
        currency.getCurrencyCode,
        startTime,
        endTime,
        service = Option(service)
      )
    }.toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Category, categorySpend
    )
  }

  private def extractDailySpendAggregation(rows: List[FieldValueList], currency: Currency): SpendReportingAggregation = {
    val spendByStartTime = rows.groupBy(row => DateTime.parse(row.get("date").getStringValue))
    val dailySpend = spendByStartTime.map { case (startTime, rowsForStartTime) =>
      val (cost, credits) = sumCostsAndCredits(rowsForStartTime, currency)
      SpendReportingForDateRange(
        cost.toString,
        credits.toString,
        currency.getCurrencyCode,
        startTime,
        endTime = startTime.plusDays(1).minusMillis(1)
      )
    }.toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.Daily, dailySpend
    )
  }

  private def extractSpendSummary(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime): SpendReportingForDateRange = {
    val (cost, credits) = sumCostsAndCredits(rows, currency)

    SpendReportingForDateRange(
      cost.toString(),
      credits.toString(),
      currency.getCurrencyCode,
      startTime,
      endTime
    )
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
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, s"Inconsistent currencies found while aggregating spend data: $x and $y cannot be combined"))
      }
    })
  }

  private def dateTimeToISODateString(dt: DateTime): String = dt.toString(ISODateTimeFormat.date())

  private def getSpendExportConfiguration(billingProjectName: RawlsBillingProjectName): Future[BillingProjectSpendExport] = {
    dataSource.inTransaction { dataAccess =>
       dataAccess.rawlsBillingProjectQuery.getBillingProjectSpendConfiguration(billingProjectName)
    }.recover {
      case _: RawlsException => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"billing account not found on billing project ${billingProjectName.value}"))
    }.map(_.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"billing project ${billingProjectName.value} not found"))))
  }

  private def getWorkspaceGoogleProjects(billingProjectName: RawlsBillingProjectName): Future[Map[GoogleProject, WorkspaceName]] = {
    dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
    }.map { workspaces =>
      workspaces.collect {
        case workspace if workspace.workspaceVersion == WorkspaceVersions.V2 => GoogleProject(workspace.googleProjectId.value) -> workspace.toWorkspaceName
      }.toMap
    }
  }

  private def validateReportParameters(startDate: DateTime, endDate: DateTime, aggregationKey: Option[SpendReportingAggregationKey], subAggregationKey: Option[SpendReportingAggregationKey]): Unit = {
    if (startDate.isAfter(endDate)) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"start date ${dateTimeToISODateString(startDate)} must be before end date ${dateTimeToISODateString(endDate)}"))
    } else if (Days.daysBetween(startDate, endDate).getDays > spendReportingServiceConfig.maxDateRange) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"provided dates exceed maximum report date range of ${spendReportingServiceConfig.maxDateRange} days"))
    } else if (subAggregationKey.isDefined && aggregationKey.isEmpty) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"cannot return sub-aggregated data without a top-level aggregation key"))
    }
  }

  private def stringQueryParameterValue(parameterValue: String): QueryParameterValue = {
    QueryParameterValue.newBuilder()
      .setType(StandardSQLTypeName.STRING)
      .setValue(parameterValue)
      .build()
  }

  private def stringArrayQueryParameterValue(parameterValues: List[String]): QueryParameterValue = {
    val queryParameterArrayValues = parameterValues.map { parameterValue =>
      QueryParameterValue.newBuilder()
        .setType(StandardSQLTypeName.STRING)
        .setValue(parameterValue)
        .build()
    }.asJava

    QueryParameterValue.newBuilder()
      .setType(StandardSQLTypeName.ARRAY)
      .setArrayType(StandardSQLTypeName.STRING)
      .setArrayValues(queryParameterArrayValues)
      .build()
}

  def getSpendForBillingProject(billingProjectName: RawlsBillingProjectName, startDate: DateTime, endDate: DateTime, aggregationKey: Option[SpendReportingAggregationKey] = None, subAggregationKey: Option[SpendReportingAggregationKey] = None): Future[SpendReportingResults] = {
    validateReportParameters(startDate, endDate, aggregationKey, subAggregationKey)
    requireAlphaUser() {
      requireProjectAction(billingProjectName, SamBillingProjectActions.readSpendReport) {
        for {
          spendExportConf <- getSpendExportConfiguration(billingProjectName)
          workspaceProjectsToNames <- getWorkspaceGoogleProjects(billingProjectName)

          query = getQuery(aggregationKey, subAggregationKey, spendExportConf.spendExportTable.getOrElse(spendReportingServiceConfig.defaultTableName))

          queryJobConfiguration = QueryJobConfiguration
            .newBuilder(query)
            .addNamedParameter("billingAccountId", stringQueryParameterValue(spendExportConf.billingAccountId.withoutPrefix()))
            .addNamedParameter("startDate", stringQueryParameterValue(dateTimeToISODateString(startDate)))
            .addNamedParameter("endDate", stringQueryParameterValue(dateTimeToISODateString(endDate)))
            .addNamedParameter("projects", stringArrayQueryParameterValue(workspaceProjectsToNames.keySet.map(_.value).toList))
            .build()

          result <- bigQueryService.use(_.query(queryJobConfiguration)).unsafeToFuture()
        } yield {
          result.getValues.asScala.toList match {
            case Nil => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"no spend data found for billing project ${billingProjectName.value} between dates ${dateTimeToISODateString(startDate)} and ${dateTimeToISODateString(endDate)}"))
            case rows => extractSpendReportingResults(rows, startDate, endDate, workspaceProjectsToNames, aggregationKey, subAggregationKey)
          }
        }
      }
    }
  }

  case class AggregationKeyQueryField(bigQueryField: String, alias: String) {
    def aliased(): String = s""", $bigQueryField as $alias"""

    def groupBy(): String = s""", $alias"""
  }

  private def getQuery(aggregationKey: Option[SpendReportingAggregationKey], subAggregationKey: Option[SpendReportingAggregationKey], tableName: String): String = {
    val aggregationKeyQueryFields = List(aggregationKey, subAggregationKey).flatten.map {
      case SpendReportingAggregationKeys.Daily => AggregationKeyQueryField("DATE(_PARTITIONTIME)", "date")
      case SpendReportingAggregationKeys.Workspace => AggregationKeyQueryField("project.id", "googleProjectId")
      case SpendReportingAggregationKeys.Category => AggregationKeyQueryField("service.description", "service")
    }

    val query = s"""
       | SELECT
       |  SUM(cost) as cost,
       |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
       |  currency ${aggregationKeyQueryFields.map(_.aliased()).mkString}
       | FROM `$tableName`
       | WHERE billing_account_id = @billingAccountId
       | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
       | AND project.id in UNNEST(@projects)
       | GROUP BY currency ${aggregationKeyQueryFields.map(_.groupBy()).mkString}
       |""".stripMargin
    logger.info(query)
    query
  }
}
