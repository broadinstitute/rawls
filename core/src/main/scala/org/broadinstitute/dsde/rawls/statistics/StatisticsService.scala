package org.broadinstitute.dsde.rawls.statistics

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.StatisticsJsonSupport._

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{RoleSupport, FutureSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.joda.time.DateTime
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by mbemis on 7/18/16.
 */
object StatisticsService {
  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new StatisticsService(userInfo, dataSource, googleServicesDAO)

  sealed trait StatisticsServiceMessage
  case class GetStatistics(startDate: String, endDate: String, workspaceNamespace: Option[String] = None, workspaceName: Option[String] = None) extends StatisticsServiceMessage
}

class StatisticsService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO)(implicit protected val executionContext: ExecutionContext) extends RoleSupport with FutureSupport with UserWiths {

  import dataSource.dataAccess.driver.api._

  def GetStatistics(startDate: String, endDate: String, workspaceNamespace: Option[String], workspaceName: Option[String]) = asFCAdmin {getStatistics(startDate, endDate, workspaceNamespace, workspaceName)}

  def getStatistics(startDate: String, endDate: String, workspaceNamespace: Option[String] = None, workspaceName: Option[String] = None): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      if(DateTime.parse(startDate).getMillis >= DateTime.parse(endDate).getMillis)
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Invalid date range"))

      val submissionStatistics = dataAccess.submissionQuery.SubmissionStatisticsQueries
      val workflowStatistics = dataAccess.workflowQuery.WorkflowStatisticsQueries
      val entityStatistics = dataAccess.entityQuery.EntityStatisticsQueries

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
        entityStatistics.countEntitiesofTypeInNamespace(workspaceNamespace, workspaceName) map { entityStats =>
          val allResults = results.toMap + ("currentEntityStatistics" -> EntityStatistics(workspaceNamespace, workspaceName, entityStats))
          RequestComplete(StatusCodes.OK, StatisticsReport(startDate, endDate, allResults))
        }
      }
    }
  }
}
