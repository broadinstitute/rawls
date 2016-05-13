package org.broadinstitute.dsde.rawls.jobexec

import java.sql.Timestamp

import akka.actor._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.FutureSupport
import spray.http.OAuth2BearerToken
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.util.{Success, Try, Failure}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, ReadAction, WorkflowRecord, DataAccess}
import scala.concurrent.{ExecutionContext, Future}
import akka.pattern._
import java.util.UUID

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitorActor {
  def props(workspaceName: WorkspaceName,
            submissionId: UUID,
            datasource: SlickDataSource,
            executionServiceDAO: ExecutionServiceDAO,
            credential: Credential,
            submissionPollInterval: FiniteDuration): Props = {
    Props(new SubmissionMonitorActor(workspaceName, submissionId, datasource, executionServiceDAO, credential, submissionPollInterval))
  }

  sealed trait SubmissionMonitorMessage
  case object StartMonitorPass extends SubmissionMonitorMessage

  /**
   * The response from querying the exec services.
   * @param statusResponse If a successful response shows an unchanged status there
   * will be a Success(None) entry in the statusResponse Seq. If the status has changed it will be
   * Some(workflowRecord, outputsOption) where workflowRecord will have the updated status. When the workflow
   * has Succeeded and there are outputs, outputsOption will contain the response from the exec service.
   */
  case class ExecutionServiceStatusResponse(statusResponse: Seq[Try[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]]]) extends SubmissionMonitorMessage
  case class StatusCheckComplete(terminateActor: Boolean) extends SubmissionMonitorMessage
}

/**
 * An actor that monitors the status of a submission. Wakes up every submissionPollInterval and queries
 * the execution service for status of workflows that we don't think are done yet. For any workflows
 * that are successful, query again for outputs. Once all workflows are done mark the submission as done
 * and terminate the actor.
 *
 * @param submissionId id of submission to monitor
 * @param datasource
 * @param submissionPollInterval time between polls of db for all workflow statuses within submission
 */
class SubmissionMonitorActor(val workspaceName: WorkspaceName,
                             val submissionId: UUID,
                             val datasource: SlickDataSource,
                             val executionServiceDAO: ExecutionServiceDAO,
                             val credential: Credential,
                             val submissionPollInterval: FiniteDuration) extends Actor with SubmissionMonitor with LazyLogging {

  import context._

  scheduleNextMonitorPass

  override def receive = {
    case StartMonitorPass =>
      logger.debug(s"polling workflows for submission $submissionId")
      queryExecutionServiceForStatus() pipeTo self
    case response: ExecutionServiceStatusResponse =>
      logger.debug(s"handling execution service response for submission $submissionId")
      handleStatusResponses(response) pipeTo self
    case StatusCheckComplete(terminateActor) =>
      logger.debug(s"done checking status for submission $submissionId, terminateActor = $terminateActor")
      if (terminateActor) stop(self)
      else scheduleNextMonitorPass

    case Status.Failure(t) => throw t // an error happened in some future, let the supervisor handle it
  }

  def scheduleNextMonitorPass: Cancellable = {
    system.scheduler.scheduleOnce(submissionPollInterval, self, StartMonitorPass)
  }

}

trait SubmissionMonitor extends FutureSupport with LazyLogging {
  val workspaceName: WorkspaceName
  val submissionId: UUID
  val datasource: SlickDataSource
  val executionServiceDAO: ExecutionServiceDAO
  val credential: Credential
  val submissionPollInterval: Duration

  import datasource.dataAccess.driver.api._

  /**
   * This function starts a monitoring pass
   * @param executionContext
   * @return
   */
  def queryExecutionServiceForStatus()(implicit executionContext: ExecutionContext): Future[ExecutionServiceStatusResponse] = {
    datasource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(submissionId, WorkflowStatuses.runningStatuses: _*)
    } flatMap { externalWorkflowIds =>
      Future.traverse(externalWorkflowIds) { workflowRec =>
        // for each workflow query the exec service for status and if has Succeeded query again for outputs
        toFutureTry(execServiceStatus(workflowRec) flatMap {
          case Some(updatedWorkflowRec) => execServiceOutputs(updatedWorkflowRec)
          case None => Future.successful(None)
        })
      }
    } map (ExecutionServiceStatusResponse)
  }

  private def execServiceStatus(workflowRec: WorkflowRecord)(implicit executionContext: ExecutionContext): Future[Option[WorkflowRecord]] = {
    executionServiceDAO.status(workflowRec.externalId, getUserInfo).map(newStatus => {
      if (newStatus.status != workflowRec.status) Option(workflowRec.copy(status = newStatus.status))
      else None
    })
  }

  private def execServiceOutputs(workflowRec: WorkflowRecord)(implicit executionContext: ExecutionContext): Future[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]] = {
    WorkflowStatuses.withName(workflowRec.status) match {
      case WorkflowStatuses.Succeeded =>
        executionServiceDAO.outputs(workflowRec.externalId, getUserInfo).map(outputs => Option((workflowRec, Option(outputs))))

      case _ => Future.successful(Option((workflowRec, None)))
    }
  }

  /**
   * once all the execution service queries have completed this function is called to handle the responses
   * @param response
   * @param executionContext
   * @return
   */
  def handleStatusResponses(response: ExecutionServiceStatusResponse)(implicit executionContext: ExecutionContext): Future[StatusCheckComplete] = {
    response.statusResponse.collect { case Failure(t) => t }.foreach { t =>
      logger.error(s"Failure monitoring workflow in submission $submissionId", t)
    }

    datasource.inTransaction { dataAccess =>
      val updatedRecs = response.statusResponse.collect {
        case Success(Some((updatedRec, _))) => updatedRec
      }

      val workflowsWithOutputs = response.statusResponse.collect {
        case Success(Some((workflowRec, Some(outputs)))) => (workflowRec, outputs)
      }

      // to minimize database updates do 1 update per status
      DBIO.seq(updatedRecs.groupBy(_.status).map { case (status, recs) =>
        DBIO.sequence(recs.map(rec => dataAccess.workflowQuery.updateStatus(rec, WorkflowStatuses.withName(status))))
      }.toSeq :_*) andThen
        handleOutputs(workflowsWithOutputs, dataAccess) andThen
          checkOverallStatus(dataAccess)
    } map { shouldStop => StatusCheckComplete(shouldStop) }
  }

  /**
   * When there are no workflows with a running status, mark the submission as done or aborted as appropriate.
   * @param dataAccess
   * @param executionContext
   * @return true if the submission is done/aborted
   */
  def checkOverallStatus(dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Boolean] = {
    dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(submissionId, WorkflowStatuses.runningStatuses:_*) flatMap { workflowRecs =>
      if (workflowRecs.isEmpty) {
        dataAccess.submissionQuery.findById(submissionId).map(_.status).result.head.map { status =>
          SubmissionStatuses.withName(status) match {
            case SubmissionStatuses.Aborting => SubmissionStatuses.Aborted
            case SubmissionStatuses.Submitted => SubmissionStatuses.Done
            case _ => throw new RawlsException(s"submission $submissionId in unexpected state $status, expected Aborting or Submitted")
          }
        } flatMap { newStatus =>
          logger.debug(s"submission $submissionId terminating to status $newStatus")
          dataAccess.submissionQuery.updateStatus(submissionId, newStatus)
        } map(_ => true)
      } else {
        DBIO.successful(false)
      }
    }
  }

  def handleOutputs(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], dataAccess: DataAccess)(implicit executionContext: ExecutionContext) = {
    if (workflowsWithOutputs.isEmpty) {
      DBIO.successful(Unit)
    } else {
      for {
        // load all the starting data
        entitiesById <-      listWorkflowEntitiesById(workflowsWithOutputs, dataAccess)
        outputExpressions <- listMethodConfigOutputsForSubmission(dataAccess)
        workspace <-         getWorkspace(dataAccess).map(_.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found")))

        // update the appropriate entities and workspace (in memory)
        updatedEntitiesAndWorkspace = attachOutputs(workspace, workflowsWithOutputs, entitiesById, outputExpressions)

        // save everything to the db
        _ <- saveWorkspace(dataAccess, updatedEntitiesAndWorkspace)
        _ <- saveEntities(dataAccess, workspace, updatedEntitiesAndWorkspace)
        _ <- saveErrors(updatedEntitiesAndWorkspace.collect { case Right(errors) => errors }, dataAccess)
      } yield Unit
    }
  }

  def getWorkspace(dataAccess: DataAccess): ReadAction[Option[Workspace]] = {
    dataAccess.workspaceQuery.findByName(workspaceName)
  }

  def listMethodConfigOutputsForSubmission(dataAccess: DataAccess): ReadAction[Map[String, String]] = {
    dataAccess.submissionQuery.getMethodConfigOutputExpressions(submissionId)
  }

  def listWorkflowEntitiesById(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], dataAccess: DataAccess): ReadAction[Map[Long, Entity]] = {
    dataAccess.entityQuery.listByIds(workflowsWithOutputs.map { case (workflowRec, outputs) => workflowRec.workflowEntityId.get })
  }

  def saveWorkspace(dataAccess: DataAccess, updatedEntitiesAndWorkspace: Seq[Either[(Option[Entity], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]]) = {
    //note there is only 1 workspace (may be None if it is not updated) even though it may be updated multiple times so reduce it into 1 update
    val workspaces = updatedEntitiesAndWorkspace.collect { case Left((_, Some(workspace))) => workspace }
    if (workspaces.isEmpty) DBIO.successful(0)
    else dataAccess.workspaceQuery.save(workspaces.reduce((a, b) => a.copy(attributes = a.attributes ++ b.attributes)))
  }

  def saveEntities(dataAccess: DataAccess, workspace: Workspace, updatedEntitiesAndWorkspace: Seq[Either[(Option[Entity], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]]) = {
    val entities = updatedEntitiesAndWorkspace.collect { case Left((Some(entity), _)) => entity }
    if (entities.isEmpty) DBIO.successful(0)
    else dataAccess.entityQuery.save(SlickWorkspaceContext(workspace), entities)
  }

  def attachOutputs(workspace: Workspace, workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], entitiesById: Map[Long, Entity], outputExpressions: Map[String, String]): Seq[Either[(Option[Entity], Option[Workspace]), (WorkflowRecord, Seq[AttributeString])]] = {
    workflowsWithOutputs.map { case (workflowRecord, outputsResponse) =>
      val outputs = outputsResponse.outputs

      val attributes = outputExpressions.map { case (outputName, attributeName) =>
        Try {
          attributeName.value -> outputs.getOrElse(outputName, {
            throw new RawlsException(s"output named ${outputName} does not exist")
          })
        }
      }

      if (attributes.forall(_.isSuccess)) {
        Left(updateEntityAndWorkspace(entitiesById(workflowRecord.workflowEntityId.get), workspace, attributes.map(_.get).toMap))

      } else {
        Right((workflowRecord, attributes.collect { case Failure(t) => AttributeString(t.getMessage) }.toSeq))
      }
    }
  }

  def updateEntityAndWorkspace(entity: Entity, workspace: Workspace, workflowOutputs: Map[String, Attribute]): (Option[Entity], Option[Workspace]) = {
    //Partition outputs by whether their attributes are entity attributes (begin with "this.") or workspace ones (implicitly; begin with "workspace.")
    //This assumption (that it's either "this." or "workspace.") will be guaranteed by checking of the method config when it's imported; see DSDEEPB-1603.
    val (partitionEntity, partitionWorkspace) = workflowOutputs.partition({ case (k, v) => k.startsWith("this.") })
    val entityAttributes = partitionEntity.map({ case (k, v) => (k.stripPrefix("this."), v) })
    val workspaceAttributes = partitionWorkspace.map({ case (k, v) => (k.stripPrefix("workspace."), v) })

    val updatedEntity = if (entityAttributes.isEmpty) None else Option(entity.copy(attributes = entity.attributes ++ entityAttributes))
    val updatedWorkspace = if (workspaceAttributes.isEmpty) None else Option(workspace.copy(attributes = workspace.attributes ++ workspaceAttributes))
    
    (updatedEntity, updatedWorkspace)
  }

  def saveErrors(errors: Seq[(WorkflowRecord, Seq[AttributeString])], dataAccess: DataAccess) = {
    DBIO.sequence(errors.map { case (workflowRecord, errorMessages) =>
      dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Failed) andThen
        dataAccess.workflowQuery.saveMessages(errorMessages, workflowRecord.id)
    })
  }

  private def getUserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo("", OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), "")
  }
}
