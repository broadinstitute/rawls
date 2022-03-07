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

  def extractSpendReportingResults(rows: List[FieldValueList], startTime: DateTime, endTime: DateTime, workspaceProjectsToNames: Map[GoogleProject, WorkspaceName], aggregationKey: SpendReportingAggregationKey): SpendReportingResults = {
    val currency = getCurrency(rows)

    val spendAggregation = aggregationKey match {
      case SpendReportingAggregationKeys.Daily => extractDailySpendAggregation(rows, currency, startTime, endTime)
      case SpendReportingAggregationKeys.Workspace => extractWorkspaceSpendAggregation(rows, currency, startTime, endTime, workspaceProjectsToNames)
    }
    val spendSummary = extractSpendSummary(rows, currency, startTime, endTime)

    SpendReportingResults(Seq(spendAggregation), spendSummary)
  }

  private def extractWorkspaceSpendAggregation(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime, workspaceProjectsToNames: Map[GoogleProject, WorkspaceName]): SpendReportingAggregation = {
    val workspaceSpend = rows.map { row =>
      val rowCost = BigDecimal(row.get("cost").getDoubleValue).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val rowCredits = BigDecimal(row.get("credits").getDoubleValue).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val rowGoogleProjectId = GoogleProject(row.get("googleProjectId").getStringValue)
      val rowWorkspaceName = workspaceProjectsToNames.getOrElse(rowGoogleProjectId, throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, s"unexpected project ${rowGoogleProjectId.value} returned by BigQuery")))
      SpendReportingForDateRange(rowCost.toString(),
        rowCredits.toString(),
        currency.getCurrencyCode,
        startTime,
        endTime,
        Option(rowWorkspaceName),
        Option(rowGoogleProjectId))
    }
    SpendReportingAggregation(
      SpendReportingAggregationKeys.Workspace, workspaceSpend
    )
  }

  private def extractDailySpendAggregation(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime): SpendReportingAggregation = {
    val dailySpend = rows.map { row =>
      val rowCost = BigDecimal(row.get("cost").getDoubleValue).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val rowCredits = BigDecimal(row.get("credits").getDoubleValue).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      SpendReportingForDateRange(rowCost.toString(),
        rowCredits.toString(),
        currency.getCurrencyCode,
        DateTime.parse(row.get("date").getStringValue),
        DateTime.parse(row.get("date").getStringValue).plusDays(1).minusSeconds(1))
    }
    SpendReportingAggregation(
      SpendReportingAggregationKeys.Daily, dailySpend
    )
  }

  private def extractSpendSummary(rows: List[FieldValueList], currency: Currency, startTime: DateTime, endTime: DateTime): SpendReportingForDateRange = {
    val costRollup = rows.map { row =>
      BigDecimal(row.get("cost").getDoubleValue)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
    val creditsRollup = rows.map { row =>
      BigDecimal(row.get("credits").getDoubleValue)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

    SpendReportingForDateRange(
      costRollup.toString(),
      creditsRollup.toString(),
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

  private def validateReportParameters(startDate: DateTime, endDate: DateTime): Unit = {
    if (startDate.isAfter(endDate)) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"start date $startDate must be before end date $endDate"))
    } else if (Days.daysBetween(startDate, endDate).getDays > spendReportingServiceConfig.maxDateRange) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"provided dates exceed maximum report date range of ${spendReportingServiceConfig.maxDateRange} days"))
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

  def getSpendForBillingProject(billingProjectName: RawlsBillingProjectName, startDate: DateTime, endDate: DateTime, aggregationKey: SpendReportingAggregationKey = SpendReportingAggregationKeys.Daily): Future[SpendReportingResults] = {
    validateReportParameters(startDate, endDate)
    requireAlphaUser() {
      requireProjectAction(billingProjectName, SamBillingProjectActions.readSpendReport) {
        for {
          spendExportConf <- getSpendExportConfiguration(billingProjectName)
          workspaceProjectsToNames <- getWorkspaceGoogleProjects(billingProjectName)

          query = getQuery(aggregationKey, spendExportConf.spendExportTable.getOrElse(spendReportingServiceConfig.defaultTableName))

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
            case Nil => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"no spend data found for billing project ${billingProjectName.value} between dates $startDate and $endDate"))
            case rows => extractSpendReportingResults(rows, startDate, endDate, workspaceProjectsToNames, aggregationKey)
          }
        }
      }
    }
  }


  private def getQuery(aggregationKey: SpendReportingAggregationKey, tableName: String): String = {
    aggregationKey match {
      case SpendReportingAggregationKeys.Daily => s"""
                                                     | SELECT
                                                     |  SUM(cost) as cost,
                                                     |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
                                                     |  currency,
                                                     |  DATE(_PARTITIONTIME) as date
                                                     | FROM `$tableName`
                                                     | WHERE billing_account_id = @billingAccountId
                                                     | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
                                                     | AND project.id in UNNEST(@projects)
                                                     | GROUP BY currency, date
                                                     |""".stripMargin
      case SpendReportingAggregationKeys.Workspace => s"""
                                                         | SELECT
                                                         |  SUM(cost) as cost,
                                                         |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
                                                         |  currency,
                                                         |  project.id as googleProjectId
                                                         | FROM `$tableName`
                                                         | WHERE billing_account_id = @billingAccountId
                                                         | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
                                                         | AND project.id in UNNEST(@projects)
                                                         | GROUP BY currency, googleProjectId
                                                         |""".stripMargin
    }
  }
}
