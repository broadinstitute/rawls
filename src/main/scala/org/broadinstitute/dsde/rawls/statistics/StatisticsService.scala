package org.broadinstitute.dsde.rawls.statistics

import akka.actor.{Actor, Props}
import akka.pattern._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
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

  override def receive = {
    case GetStatistics(startDate, endDate) => asAdmin {getStatistics(startDate, endDate)} pipeTo sender
  }

  def getStatistics(startDate: String, endDate: String): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      if(DateTime.parse(startDate).getMillis >= DateTime.parse(endDate).getMillis)
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Invalid date range"))
      for {
        currentTotalUsers <- dataAccess.rawlsUserQuery.countUsers()
        submissionsDuringWindow <- dataAccess.submissionQuery.SubmissionStatisticsQueries.countSubmissionsInWindow(startDate, endDate)
        workflowsDuringWindow <- dataAccess.workflowQuery.WorkflowStatisticsQueries.countWorkflowsInWindow(startDate, endDate)
        usersWhoSubmittedDuringWindow <- dataAccess.submissionQuery.SubmissionStatisticsQueries.countUsersWhoSubmittedInWindow(startDate, endDate)
        submissionsPerUser <- dataAccess.submissionQuery.SubmissionStatisticsQueries.countSubmissionsPerUserQuery(startDate, endDate)
        workflowsPerUser <- dataAccess.workflowQuery.WorkflowStatisticsQueries.countWorkflowsPerUserQuery(startDate, endDate)
        workflowsPerSubmission <- dataAccess.workflowQuery.WorkflowStatisticsQueries.countWorkflowsPerSubmission(startDate, endDate)
        submissionRunTime <- dataAccess.submissionQuery.SubmissionStatisticsQueries.submissionRunTimeQuery(startDate, endDate)
        workflowRunTime <- dataAccess.workflowQuery.WorkflowStatisticsQueries.workflowRunTimeQuery(startDate, endDate)
      } yield RequestComplete(StatusCodes.OK, StatisticsReport(Map("currentTotalUsers" -> SingleStatistic(currentTotalUsers),
                                                                   "submissionsDuringWindow" -> submissionsDuringWindow.head,
                                                                   "workflowsDuringWindow" -> workflowsDuringWindow.head,
                                                                   "usersWhoSubmittedDuringWindow" -> usersWhoSubmittedDuringWindow.head,
                                                                   "submissionsPerUser" -> submissionsPerUser.head,
                                                                   "workflowsPerUser" -> workflowsPerUser.head,
                                                                   "workflowsPerSubmission" -> workflowsPerSubmission.head,
                                                                   "submissionRunTimeSeconds" -> submissionRunTime.head,
                                                                   "workflowRunTimeSeconds" -> workflowRunTime.head)))
    }
  }
}
