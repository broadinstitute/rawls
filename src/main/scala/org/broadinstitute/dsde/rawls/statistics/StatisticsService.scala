package org.broadinstitute.dsde.rawls.statistics

import akka.actor.{Actor, Props}
import akka.pattern._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.StatisticsJsonSupport._

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.statistics.StatisticsService._
import org.broadinstitute.dsde.rawls.util.{AdminSupport, FutureSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.joda.time.DateTime
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by mbemis on 7/18/16.
 */
object StatisticsService {
  def props(userServiceConstructor: UserInfo => StatisticsService, userInfo: UserInfo): Props = {
    Props(userServiceConstructor(userInfo))
  }

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new StatisticsService(userInfo, dataSource, googleServicesDAO, userDirectoryDAO)

  sealed trait StatisticsServiceMessage
  case class GetStatistics(startDate: String, endDate: String) extends StatisticsServiceMessage
}

class StatisticsService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with AdminSupport with FutureSupport with UserWiths {

  import dataSource.dataAccess.driver.api._

  override def receive = {
    case GetStatistics(startDate, endDate) => asFCAdmin {getStatistics(startDate, endDate)} pipeTo sender
  }

  def getStatistics(startDate: String, endDate: String): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      if(DateTime.parse(startDate).getMillis >= DateTime.parse(endDate).getMillis)
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Invalid date range"))

      val submissionStatistics = dataAccess.submissionQuery.SubmissionStatisticsQueries
      val workflowStatistics = dataAccess.workflowQuery.WorkflowStatisticsQueries

      val statistics = Map[String, (String, String) => ReadAction[Statistic]](
        "submissionsDuringWindow" ->  submissionStatistics.countSubmissionsInWindow,
        "workflowsDuringWindow" -> workflowStatistics.countWorkflowsInWindow,
        "usersWhoSubmittedDuringWindow" -> submissionStatistics.countUsersWhoSubmittedInWindow,
        "submissionsPerUser" -> submissionStatistics.countSubmissionsPerUserQuery,
        "workflowsPerUser" -> workflowStatistics.countWorkflowsPerUserQuery,
        "workflowsPerSubmission" -> workflowStatistics.countWorkflowsPerSubmission,
        "submissionRunTime" -> submissionStatistics.submissionRunTimeQuery,
        "workflowRunTime" -> workflowStatistics.workflowRunTimeQuery
      )

      val actions = statistics.map { case (name,func) =>
        DBIO.successful(name).zip(func(startDate, endDate))
      }

      DBIO.sequence(actions).flatMap { results =>
        dataAccess.rawlsUserQuery.countUsers() map { numUsers =>
          val allResults = (results.toMap + ("currentTotalUsers" -> numUsers))
          RequestComplete(StatusCodes.OK, StatisticsReport(startDate, endDate, allResults))
        }
      }
    }
  }
}
