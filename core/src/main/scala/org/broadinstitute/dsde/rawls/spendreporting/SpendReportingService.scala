package org.broadinstitute.dsde.rawls.spendreporting

import java.util
import java.util.Currency

import akka.http.scaladsl.model.StatusCodes
import com.google.api.services.bigquery.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

object SpendReportingService {
  def constructor(dataSource: SlickDataSource, bigQueryDAO: GoogleBigQueryDAO, samDAO: SamDAO, defaultTableName: String, serviceProject: GoogleProject)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SpendReportingService = {
    new SpendReportingService(userInfo, dataSource, bigQueryDAO, samDAO, defaultTableName, serviceProject)
  }
}


class SpendReportingService(userInfo: UserInfo, dataSource: SlickDataSource, bigQueryDAO: GoogleBigQueryDAO, samDAO: SamDAO, defaultTableName: String, serviceProject: GoogleProject)
                           (implicit val executionContext: ExecutionContext) extends LazyLogging {

  def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(op: => Future[T]): Future[T] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You cannot perform ${action.value} on project ${projectName.value}")))
    }
  }

  def requireAlphaUser[T]()(op: => Future[T]): Future[T] = {
    samDAO.userHasAction(SamResourceTypeNames.managedGroup, "Alpha_Spend_Report_Users", SamResourceAction("use"), userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "This API is not live yet.")))
    }
  }

  def extractSpendReportingResults(rows: List[TableRow], startTime: DateTime, endTime: DateTime): SpendReportingResults = {
    val currency = Currency.getInstance(rows.head.getF.get(2).getV.toString)

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

  private def dateTimeToISODateString(dt: DateTime): String = dt.toString(ISODateTimeFormat.date())

  private def getSpendExportConfiguration(billingProjectName: RawlsBillingProjectName): Future[BillingProjectSpendExport] = {
    dataSource.inTransaction { dataAccess =>
       dataAccess.rawlsBillingProjectQuery.load(billingProjectName)
    }.map {
      case Some(RawlsBillingProject(_, _, Some(billingAccount), _, _, _, _, _, Some(spendReportDataset), Some(spendReportTable), Some(spendReportDatasetGoogleProject))) =>
        BillingProjectSpendExport(billingProjectName, billingAccount, Option(s"${spendReportDatasetGoogleProject.value}.${spendReportDataset.value}.${spendReportTable.value}"))
      case Some(RawlsBillingProject(_, _, Some(billingAccount), _, _, _, _, _, _, _, _)) => BillingProjectSpendExport(billingProjectName, billingAccount, None)
      case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"project ${billingProjectName.value} not found"))
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"billing account not found on billing project ${billingProjectName.value}"))
    }
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
    if (startDate.isAfter(endDate)) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "start date must be before end date"))
    else if (Days.daysBetween(startDate, endDate).getDays > 90) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "provided dates exceed maximum report date range"))
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
             | FROM `${spendExportConf.spendExportTable.getOrElse(defaultTableName)}`
             | WHERE billing_account_id = @billingAccountId
             | AND _PARTITIONTIME BETWEEN @startDate AND @endDate
             | AND project.id in UNNEST(@projects)
             | GROUP BY currency, date
             |""".stripMargin

          queryParams = List(
            new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("billingAccountId").setParameterValue(new QueryParameterValue().setValue(spendExportConf.billingAccountId.withoutPrefix())),
            new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("startDate").setParameterValue(new QueryParameterValue().setValue(dateTimeToISODateString(startDate))),
            new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("endDate").setParameterValue(new QueryParameterValue().setValue(dateTimeToISODateString(endDate))),
            new QueryParameter().setParameterType(new QueryParameterType().setType("ARRAY").setArrayType(new QueryParameterType().setType("STRING"))).setName("projects").setParameterValue(new QueryParameterValue().setArrayValues(workspaceProjects.map(project => new QueryParameterValue().setValue(project.value)).toList.asJava))
          )
          jobRef <- bigQueryDAO.startParameterizedQuery(serviceProject, query, queryParams, "NAMED")
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
