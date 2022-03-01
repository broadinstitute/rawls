package org.broadinstitute.dsde.rawls.spendreporting

import java.util.Currency

import akka.http.scaladsl.model.StatusCodes
import com.google.api.services.bigquery.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.SpendReportingServiceConfig
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

object SpendReportingService {
  def constructor(dataSource: SlickDataSource, bigQueryDAO: GoogleBigQueryDAO, samDAO: SamDAO, spendReportingServiceConfig: SpendReportingServiceConfig)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SpendReportingService = {
    new SpendReportingService(userInfo, dataSource, bigQueryDAO, samDAO, spendReportingServiceConfig)
  }
}

class SpendReportingService(userInfo: UserInfo, dataSource: SlickDataSource, bigQueryDAO: GoogleBigQueryDAO, samDAO: SamDAO, spendReportingServiceConfig: SpendReportingServiceConfig)
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

  def extractSpendReportingResults(rows: List[TableRow], startTime: DateTime, endTime: DateTime): SpendReportingResults = {
    val currency = getCurrency(rows)

    val dailySpend = rows.map { row =>
      val rowCost = BigDecimal(row.getF.get(0).getV.toString).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val rowCredits = BigDecimal(row.getF.get(1).getV.toString).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      SpendReportingForDateRange(rowCost.toString(),
        rowCredits.toString(),
        currency.getCurrencyCode,
        DateTime.parse(row.getF.get(3).getV.toString),
        DateTime.parse(row.getF.get(3).getV.toString).plusDays(1).minusSeconds(1))
    }
    val dailySpendAggregation = SpendReportingAggregation(
      SpendReportingAggregationKey("total"), dailySpend
    )

    val costRollup = rows.map { row =>
      BigDecimal(row.getF.get(0).getV.toString)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
    val creditsRollup = rows.map { row =>
      BigDecimal(row.getF.get(1).getV.toString)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

    val spendSummary = SpendReportingForDateRange(
      costRollup.toString(),
      creditsRollup.toString(),
      currency.getCurrencyCode,
      startTime,
      endTime
    )

    SpendReportingResults(Seq(dailySpendAggregation), spendSummary)
  }

  /**
    * Ensure that BigQuery results only include one type of currency and return that currency.
    */
  private def getCurrency(rows: List[TableRow]): Currency = {
    val currencies = rows.map(_.getF.get(2).getV.toString)

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

  private def getWorkspaceGoogleProjects(billingProjectName: RawlsBillingProjectName): Future[Set[GoogleProject]] = {
    dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
    }.map { workspaces =>
      workspaces.collect {
        case workspace if workspace.workspaceVersion == WorkspaceVersions.V2 => GoogleProject(workspace.googleProjectId.value)
      }.toSet
    }
  }

  private def validateReportParameters(startDate: DateTime, endDate: DateTime): Unit = {
    if (startDate.isAfter(endDate)) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"start date $startDate must be before end date $endDate"))
    } else if (Days.daysBetween(startDate, endDate).getDays > spendReportingServiceConfig.maxDateRange) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"provided dates exceed maximum report date range of ${spendReportingServiceConfig.maxDateRange} days"))
    }
  }

  private def stringQueryParameter(parameterName: String, parameterValue: String): QueryParameter = {
    new QueryParameter()
      .setName(parameterName)
      .setParameterType(new QueryParameterType().setType("STRING"))
      .setParameterValue(new QueryParameterValue().setValue(parameterValue))
  }

  private def stringArrayQueryParameter(parameterName: String, parameterValues: List[String]): QueryParameter = {
    val queryParameterArrayValues = parameterValues.map(new QueryParameterValue().setValue(_)).asJava

    new QueryParameter()
      .setName(parameterName)
      .setParameterType(
        new QueryParameterType()
          .setType("ARRAY")
          .setArrayType(new QueryParameterType().setType("STRING")))
      .setParameterValue(
        new QueryParameterValue()
          .setArrayValues(queryParameterArrayValues))
  }

  def getSpendForBillingProject(billingProjectName: RawlsBillingProjectName, startDate: DateTime, endDate: DateTime): Future[Option[SpendReportingResults]] = {
    validateReportParameters(startDate, endDate)

    requireAlphaUser() {
      requireProjectAction(billingProjectName, SamBillingProjectActions.readSpendReport) {
        for {
          spendExportConf <- getSpendExportConfiguration(billingProjectName)
          workspaceProjects <- getWorkspaceGoogleProjects(billingProjectName)

          query =
          s"""
             | SELECT
             |  SUM(cost) as cost,
             |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
             |  currency,
             |  DATE(_PARTITIONTIME) as date
             | FROM `${spendExportConf.spendExportTable.getOrElse(spendReportingServiceConfig.defaultTableName)}`
             | WHERE billing_account_id = @billingAccountId
             | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
             | AND project.id in UNNEST(@projects)
             | GROUP BY currency, date
             |""".stripMargin

          queryParams = List(
            stringQueryParameter("billingAccountId", spendExportConf.billingAccountId.withoutPrefix()),
            stringQueryParameter("startDate", dateTimeToISODateString(startDate)),
            stringQueryParameter("endDate", dateTimeToISODateString(endDate)),
            stringArrayQueryParameter("projects", workspaceProjects.map(_.value).toList)
          )
          jobRef <- bigQueryDAO.startParameterizedQuery(spendReportingServiceConfig.serviceProject, query, queryParams, "NAMED")
          jobStatus <- bigQueryDAO.getQueryStatus(jobRef)
          result: GetQueryResultsResponse <- bigQueryDAO.getQueryResult(jobStatus)
        } yield {
          Option(result.getRows).map { rows =>
            extractSpendReportingResults(rows.asScala.toList, startDate, endDate)
          }
        }
      }
    }
  }
}
