package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import slick.jdbc.TransactionIsolation
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, SubmissionListResponseFormat, SubmissionReportFormat, SubmissionStatusResponseFormat, SubmissionValidationReportFormat, WorkflowOutputsFormat, WorkflowQueueStatusByUserResponseFormat, WorkflowQueueStatusResponseFormat}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport.AgoraEntityFormat
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport.{WorkspaceACLFormat, WorkspaceCatalogFormat, WorkspaceCatalogUpdateResponseListFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.joda.time.DateTime
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  def constructor(dataSource: SlickDataSource, methodRepoDAO: MethodRepoDAO, executionServiceCluster: ExecutionServiceCluster, execServiceBatchSize: Int, gcsDAO: GoogleServicesDAO, samDAO: SamDAO, notificationDAO: NotificationDAO, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, workbenchMetricBaseName: String, submissionCostService: SubmissionCostService)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) = {
    new WorkspaceService(userInfo, dataSource, methodRepoDAO, executionServiceCluster, execServiceBatchSize, gcsDAO, samDAO, notificationDAO, userServiceConstructor, genomicsServiceConstructor, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, workbenchMetricBaseName, submissionCostService)
  }
}

class WorkspaceService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val methodRepoDAO: MethodRepoDAO, executionServiceCluster: ExecutionServiceCluster, execServiceBatchSize: Int, protected val gcsDAO: GoogleServicesDAO, samDAO: SamDAO, notificationDAO: NotificationDAO, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, override val workbenchMetricBaseName: String, submissionCostService: SubmissionCostService)(implicit protected val executionContext: ExecutionContext)
  extends RoleSupport with LibraryPermissionsSupport with FutureSupport with MethodWiths with UserWiths with LazyLogging with RawlsInstrumented {

  import dataSource.dataAccess.driver.api._

  def CreateWorkspace(workspace: WorkspaceRequest) = createWorkspace(workspace)
  def GetWorkspace(workspaceName: WorkspaceName) = getWorkspace(workspaceName)
  def DeleteWorkspace(workspaceName: WorkspaceName) = deleteWorkspace(workspaceName)
  def UpdateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) = updateWorkspace(workspaceName, operations)
  def UpdateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) = updateLibraryAttributes(workspaceName, operations)
  def ListWorkspaces = listWorkspaces()
  def ListAllWorkspaces = listAllWorkspaces()
  def GetTags(query: Option[String]) = getTags(query)
  def AdminListWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue) = asFCAdmin { listWorkspacesWithAttribute(attributeName, attributeValue) }
  def CloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceRequest) = cloneWorkspace(sourceWorkspace, destWorkspace)
  def GetACL(workspaceName: WorkspaceName) = getACL(workspaceName)
  def UpdateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate], inviteUsersNotFound: Boolean) = updateACL(workspaceName, aclUpdates, inviteUsersNotFound)
  def SendChangeNotifications(workspaceName: WorkspaceName) = sendChangeNotifications(workspaceName)
  def GetCatalog(workspaceName: WorkspaceName) = getCatalog(workspaceName)
  def UpdateCatalog(workspaceName: WorkspaceName, catalogUpdates: Seq[WorkspaceCatalog]) = updateCatalog(workspaceName, catalogUpdates)
  def LockWorkspace(workspaceName: WorkspaceName) = lockWorkspace(workspaceName)
  def UnlockWorkspace(workspaceName: WorkspaceName) = unlockWorkspace(workspaceName)
  def CheckBucketReadAccess(workspaceName: WorkspaceName) = checkBucketReadAccess(workspaceName)
  def GetWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]) = getWorkspaceStatus(workspaceName, userSubjectId)
  def GetBucketUsage(workspaceName: WorkspaceName) = getBucketUsage(workspaceName)
  def GetAccessInstructions(workspaceName: WorkspaceName) = getAccessInstructions(workspaceName)

  def CreateEntity(workspaceName: WorkspaceName, entity: Entity) = createEntity(workspaceName, entity)
  def GetEntity(workspaceName: WorkspaceName, entityType: String, entityName: String) = getEntity(workspaceName, entityType, entityName)
  def UpdateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]) = updateEntity(workspaceName, entityType, entityName, operations)
  def DeleteEntities(workspaceName: WorkspaceName, entities: Seq[AttributeEntityReference]) = deleteEntities(workspaceName, entities)
  def RenameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String) = renameEntity(workspaceName, entityType, entityName, newName)
  def EvaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String) = evaluateExpression(workspaceName, entityType, entityName, expression)
  def GetEntityTypeMetadata(workspaceName: WorkspaceName) = entityTypeMetadata(workspaceName)
  def ListEntities(workspaceName: WorkspaceName, entityType: String) = listEntities(workspaceName, entityType)
  def QueryEntities(workspaceName: WorkspaceName, entityType: String, query: EntityQuery) = queryEntities(workspaceName, entityType, query)
  def CopyEntities(entityCopyDefinition: EntityCopyDefinition, uri:Uri, linkExistingEntities: Boolean) = copyEntities(entityCopyDefinition, uri, linkExistingEntities)
  def BatchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) = batchUpdateEntities(workspaceName, entityUpdates, true)
  def BatchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) = batchUpdateEntities(workspaceName, entityUpdates, false)

  def CreateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration) = createMethodConfiguration(workspaceName, methodConfiguration)
  def RenameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName) = renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)
  def DeleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def GetMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def OverwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) = overwriteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)
  def UpdateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) = updateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)
  def CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) = copyMethodConfiguration(methodConfigNamePair)
  def CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationImport) = copyMethodConfigurationFromMethodRepo(query)
  def CopyMethodConfigurationToMethodRepo(query: MethodRepoConfigurationExport) = copyMethodConfigurationToMethodRepo(query)
  def ListAgoraMethodConfigurations(workspaceName: WorkspaceName) = listAgoraMethodConfigurations(workspaceName)
  def ListMethodConfigurations(workspaceName: WorkspaceName) = listMethodConfigurations(workspaceName)
  def CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) = createMethodConfigurationTemplate(methodRepoMethod)
  def GetMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ) = getMethodInputsOutputs(methodRepoMethod)
  def GetAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = getAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def GetGenomicsOperation(workspaceName: WorkspaceName, jobId: String) = getGenomicsOperation(workspaceName, jobId)

  def ListSubmissions(workspaceName: WorkspaceName) = listSubmissions(workspaceName)
  def CountSubmissions(workspaceName: WorkspaceName) = countSubmissions(workspaceName)
  def CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) = createSubmission(workspaceName, submission)
  def ValidateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) = validateSubmission(workspaceName, submission)
  def GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = getSubmissionStatus(workspaceName, submissionId)
  def AbortSubmission(workspaceName: WorkspaceName, submissionId: String) = abortSubmission(workspaceName, submissionId)
  def GetWorkflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = workflowOutputs(workspaceName, submissionId, workflowId)
  def GetWorkflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = workflowMetadata(workspaceName, submissionId, workflowId)
  def WorkflowQueueStatus = workflowQueueStatus()

  def AdminListAllActiveSubmissions = asFCAdmin { listAllActiveSubmissions() }
  def AdminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) = adminAbortSubmission(workspaceName,submissionId)
  def AdminDeleteWorkspace(workspaceName: WorkspaceName) = adminDeleteWorkspace(workspaceName)
  def AdminWorkflowQueueStatusByUser = adminWorkflowQueueStatusByUser()

  def HasAllUserReadAccess(workspaceName: WorkspaceName) = hasAllUserReadAccess(workspaceName)
  def GrantAllUserReadAccess(workspaceName: WorkspaceName) = grantAllUserReadAccess(workspaceName)
  def RevokeAllUserReadAccess(workspaceName: WorkspaceName) = revokeAllUserReadAccess(workspaceName)

  def createWorkspace(workspaceRequest: WorkspaceRequest): Future[Workspace] =
    withAttributeNamespaceCheck(workspaceRequest) {
      dataSource.inTransaction { dataAccess =>
        withNewWorkspaceContext(workspaceRequest, dataAccess) { workspaceContext =>
          DBIO.successful(workspaceContext.workspace)
        }
      }
    }

  def getWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        getMaximumAccessLevel(RawlsUser(userInfo), workspaceContext, dataAccess) flatMap { accessLevel =>
          if (accessLevel < WorkspaceAccessLevels.Read)
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
          else {
            for {
              catalog <- getUserCatalogPermissions(workspaceContext, dataAccess)
              canShare <- getUserSharePermissions(workspaceContext, accessLevel, dataAccess)
              canCompute <- getUserComputePermissions(workspaceContext, accessLevel, dataAccess)
              stats <- getWorkspaceSubmissionStats(workspaceContext, dataAccess)
              owners <- getWorkspaceOwners(workspaceContext.workspace, dataAccess)
            } yield {
              RequestComplete(StatusCodes.OK, WorkspaceResponse(accessLevel, canShare, canCompute, catalog, workspaceContext.workspace, stats, owners))
            }
          }
        }
      }
    }

  def getMaximumAccessLevel(user: RawlsUser, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadWriteAction[WorkspaceAccessLevel] = {
    val accessLevels = workspaceContext.workspace.authDomainACLs.map { case (accessLevel, groupRef) =>
      dataAccess.rawlsGroupQuery.loadGroupIfMember(groupRef, user).map {
        case Some(_) => accessLevel
        case None => WorkspaceAccessLevels.NoAccess
      }
    }

    DBIO.sequence(accessLevels).map { _.fold(WorkspaceAccessLevels.NoAccess)(WorkspaceAccessLevels.max) }
  }

  def getWorkspaceOwners(workspace: Workspace, dataAccess: DataAccess): ReadWriteAction[Seq[String]] = {
    dataAccess.rawlsGroupQuery.load(workspace.accessLevels(WorkspaceAccessLevels.Owner)).flatMap {
      case None => DBIO.failed(new RawlsException(s"Unable to load owners for workspace ${workspace.toWorkspaceName}"))
      case Some(ownerGroup) =>
        val usersAction = DBIO.sequence(ownerGroup.users.map(dataAccess.rawlsUserQuery.load(_).map(_.get.userEmail.value)).toSeq)
        val subGroupsAction = DBIO.sequence(ownerGroup.subGroups.map(dataAccess.rawlsGroupQuery.load(_).map(_.get.groupEmail.value)).toSeq)

        for {
          users <- usersAction
          subGroups <- subGroupsAction
        } yield users ++ subGroups
    }
  }

  def getWorkspaceContext(workspaceName: WorkspaceName): Future[SlickWorkspaceContext] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel): Future[SlickWorkspaceContext] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, accessLevel, dataAccess) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def adminDeleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] = asFCAdmin {
    getWorkspaceContext(workspaceName) flatMap { ctx =>
      deleteWorkspace(workspaceName, ctx)
    }
  }

  def deleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =  {
     getWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Owner) flatMap { ctx =>
       deleteWorkspace(workspaceName, ctx)
    }
  }

  private def deleteWorkspace(workspaceName: WorkspaceName, workspaceContext: SlickWorkspaceContext): Future[PerRequestMessage] = {
    //Attempt to abort any running workflows so they don't write any more to the bucket.
    //Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
    //This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
    //ExecutionContext run the futures whenever
    val deletionFuture: Future[(Seq[WorkflowRecord], Seq[Option[RawlsGroup]])] = dataSource.inTransaction { dataAccess =>
      for {
        // Gather any active workflows with external ids
        workflowsToAbort <- dataAccess.workflowQuery.findActiveWorkflowsWithExternalIds(workspaceContext)

        //If a workflow is not done, automatically change its status to Aborted
        _ <- dataAccess.workflowQuery.findWorkflowsByWorkspace(workspaceContext).result.map { recs => recs.collect {
          case wf if !WorkflowStatuses.withName(wf.status).isDone =>
            dataAccess.workflowQuery.updateStatus(wf, WorkflowStatuses.Aborted)(workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceName, wf.submissionId)))
        }}

        //Gather the Google groups to remove, but don't remove project owners group which is used by other workspaces
        authDomainAclRefsToRemove = if(workspaceContext.workspace.authorizationDomain.nonEmpty) workspaceContext.workspace.authDomainACLs.values else Seq.empty
        groupRefsToRemove: Set[RawlsGroupRef] = (workspaceContext.workspace.accessLevels.filterKeys(_ != ProjectOwner).values ++ authDomainAclRefsToRemove).toSet
        groupsToRemove <- DBIO.sequence(groupRefsToRemove.toSeq.map(groupRef => dataAccess.rawlsGroupQuery.load(groupRef)))

        // Delete components of the workspace
        _ <- dataAccess.workspaceQuery.deleteWorkspaceAuthDomainRecords(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceAccessReferences(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceInvites(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceSharePermissions(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceComputePermissions(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceCatalogPermissions(workspaceContext.workspaceId)
        _ <- dataAccess.submissionQuery.deleteFromDb(workspaceContext.workspaceId)
        _ <- dataAccess.methodConfigurationQuery.deleteFromDb(workspaceContext.workspaceId)
        _ <- dataAccess.entityQuery.deleteFromDb(workspaceContext.workspaceId)

        // Delete groups
        _ <- DBIO.seq(groupRefsToRemove.map { group => dataAccess.rawlsGroupQuery.delete(group) }.toSeq: _*)

        // Delete the workspace
        _ <- dataAccess.workspaceQuery.delete(workspaceName)

        // Schedule bucket for deletion
        _ <- dataAccess.pendingBucketDeletionQuery.save(PendingBucketDeletionRecord(workspaceContext.workspace.bucketName))

      } yield {
        (workflowsToAbort, groupsToRemove)
      }
    }
    for {
      (workflowsToAbort, groupsToRemove) <- deletionFuture

      // Abort running workflows
      aborts = Future.traverse(workflowsToAbort) { wf => executionServiceCluster.abort(wf, userInfo) }

      // Remove Google Groups
      _ <- Future.traverse(groupsToRemove) {
        case Some(group) => gcsDAO.deleteGoogleGroup(group)
        case None => Future.successful(())
      }
    } yield {
      aborts.onFailure {
        case t: Throwable => logger.info(s"failure aborting workflows while deleting workspace ${workspaceName}", t)
      }
      RequestComplete(StatusCodes.Accepted, s"Your Google bucket ${workspaceContext.workspace.bucketName} will be deleted within 24h.")
    }
  }

  def updateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] = {
    withLibraryAttributeNamespaceCheck(operations.map(_.name)) {
      tryIsCurator(userInfo.userEmail) flatMap { isCurator =>
        getWorkspaceContext(workspaceName) flatMap { ctx =>
          dataSource.inTransaction { dataAccess =>
            getMaximumAccessLevel(RawlsUser(userInfo), ctx, dataAccess) flatMap {maxAccessLevel =>
              withLibraryPermissions(ctx, operations, dataAccess, userInfo, isCurator, maxAccessLevel) {
                updateWorkspace(operations, dataAccess)(ctx)
              }
            }
          }
        } map { ws =>
          RequestComplete(StatusCodes.OK, ws)
        }
      }
    }
  }

  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(operations.map(_.name)) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) {
          updateWorkspace(operations, dataAccess)
        }
      } map { ws =>
        RequestComplete(StatusCodes.OK, ws)
      }
    }
  }

  private def updateWorkspace(operations: Seq[AttributeUpdateOperation], dataAccess: DataAccess)(workspaceContext: SlickWorkspaceContext): ReadWriteAction[Workspace] = {
    val workspace = workspaceContext.workspace
    Try {
      val updatedWorkspace = applyOperationsToWorkspace(workspace, operations)
      dataAccess.workspaceQuery.save(updatedWorkspace)
    } match {
      case Success(result) => result
      case Failure(e: AttributeUpdateOperationException) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspace.name}", ErrorReport(e))))
      case Failure(regrets) => DBIO.failed(regrets)
    }
  }

  def getTags(query: Option[String]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.getTags(query).map { result =>
        RequestComplete(StatusCodes.OK, result)
      }
    }

  def listWorkspaces(): Future[PerRequestMessage] =
    dataSource.inTransaction ({ dataAccess =>

      val query = for {
        permissionsPairs <- listWorkspaces(RawlsUser(userInfo), dataAccess)
        managedGroupsForUser <- DBIO.from(samDAO.listManagedGroups(userInfo))
        ownerEmails <- dataAccess.workspaceQuery.listAccessGroupMemberEmails(permissionsPairs.map(p => UUID.fromString(p.workspaceId)), WorkspaceAccessLevels.Owner)
        publicWorkspaces <- dataAccess.workspaceQuery.listWorkspacesWithGroupAccess(permissionsPairs.map(p => UUID.fromString(p.workspaceId)), UserService.allUsersGroupRef)
        submissionSummaryStats <- dataAccess.workspaceQuery.listSubmissionSummaryStats(permissionsPairs.map(p => UUID.fromString(p.workspaceId)))
        workspaces <- dataAccess.workspaceQuery.listByIds(permissionsPairs.map(p => UUID.fromString(p.workspaceId)))
      } yield (permissionsPairs, managedGroupsForUser, ownerEmails, publicWorkspaces, submissionSummaryStats, workspaces)

      val results = query.map { case (permissionsPairs, managedGroupsForUser, ownerEmails, publicWorkspaces, submissionSummaryStats, workspaces) =>
        val workspacesById = workspaces.groupBy(_.workspaceId).mapValues(_.head)
        permissionsPairs.map { permissionsPair =>
          val groupsForUser = managedGroupsForUser.map(group => ManagedGroupRef(group.groupName)).toSet

          workspacesById.get(permissionsPair.workspaceId).map { workspace =>
            def trueAccessLevel = {
              if((workspace.authorizationDomain -- groupsForUser).nonEmpty) WorkspaceAccessLevels.NoAccess
              else permissionsPair.accessLevel
            }
            val wsId = UUID.fromString(workspace.workspaceId)
            val public: Boolean = publicWorkspaces.contains(wsId)
            WorkspaceListResponse(trueAccessLevel, workspace, submissionSummaryStats(wsId), ownerEmails.getOrElse(wsId, Seq.empty), Some(public))
          }
        }
      }

      results.map { responses => RequestComplete(StatusCodes.OK, responses) }
    }, TransactionIsolation.ReadCommitted)

  def listWorkspaces(user: RawlsUser, dataAccess: DataAccess): ReadWriteAction[Seq[WorkspacePermissionsPair]] = {
    val rawPairs = for {
      groups <- dataAccess.rawlsGroupQuery.listGroupsForUser(user)
      pairs <- dataAccess.workspaceQuery.listPermissionPairsForGroups(groups)
    } yield pairs

    rawPairs.map { pairs =>
      pairs.groupBy(_.workspaceId).map { case (workspaceId, pairsInWorkspace) =>
        pairsInWorkspace.reduce((a, b) => WorkspacePermissionsPair(workspaceId, WorkspaceAccessLevels.max(a.accessLevel, b.accessLevel)))
      }.toSeq
    }
  }

  private def getWorkspaceSubmissionStats(workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadAction[WorkspaceSubmissionStats] = {
    // listSubmissionSummaryStats works against a sequence of workspaces; we call it just for this one workspace
    dataAccess.workspaceQuery
      .listSubmissionSummaryStats(Seq(workspaceContext.workspaceId))
      .map {p => p.get(workspaceContext.workspaceId).get}
  }

  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest): Future[Workspace] = {
    val (libraryAttributeNames, workspaceAttributeNames) = destWorkspaceRequest.attributes.keys.partition(name => name.namespace == AttributeName.libraryNamespace)
    withAttributeNamespaceCheck(workspaceAttributeNames) {
      withLibraryAttributeNamespaceCheck(libraryAttributeNames) {
        dataSource.inTransaction { dataAccess =>
          withWorkspaceContextAndPermissions(sourceWorkspaceName, WorkspaceAccessLevels.Read, dataAccess) { sourceWorkspaceContext =>
            withClonedAuthDomain(sourceWorkspaceContext, destWorkspaceRequest) { newAuthDomain =>

              // add to or replace current attributes, on an individual basis
              val newAttrs = sourceWorkspaceContext.workspace.attributes ++ destWorkspaceRequest.attributes

              withNewWorkspaceContext(destWorkspaceRequest.copy(authorizationDomain = Option(newAuthDomain), attributes = newAttrs), dataAccess) { destWorkspaceContext =>
                dataAccess.entityQuery.copyAllEntities(sourceWorkspaceContext, destWorkspaceContext) andThen
                  dataAccess.methodConfigurationQuery.listActive(sourceWorkspaceContext).flatMap { methodConfigShorts =>
                    val inserts = methodConfigShorts.map { methodConfigShort =>
                      dataAccess.methodConfigurationQuery.get(sourceWorkspaceContext, methodConfigShort.namespace, methodConfigShort.name).flatMap { methodConfig =>
                        dataAccess.methodConfigurationQuery.create(destWorkspaceContext, methodConfig.get)
                      }
                    }
                    DBIO.seq(inserts: _*)
                  } andThen {
                  DBIO.successful(destWorkspaceContext.workspace)
                }
              }
            }
          }
        }
      }
    }
  }

  private def withClonedAuthDomain[T](sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceRequest: WorkspaceRequest)(op: (Set[ManagedGroupRef]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    // if the source has an auth domain, the dest must also have that auth domain as a subset
    // otherwise, the caller may choose to add to the auth domain
    val sourceWorkspaceADs = sourceWorkspaceContext.workspace.authorizationDomain
    val destWorkspaceADs = destWorkspaceRequest.authorizationDomain.getOrElse(Set.empty)

    if(sourceWorkspaceADs.subsetOf(destWorkspaceADs)) op(sourceWorkspaceADs ++ destWorkspaceADs)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has an Authorization Domain containing the groups ${missingGroups.map(_.membersGroupName.value).mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }
  }

  def getACL(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireSharePermission(workspaceContext.workspace, dataAccess) { _ =>
          dataAccess.workspaceQuery.listEmailsAndAccessLevel(workspaceContext).flatMap { emailsAndAccess =>
            dataAccess.workspaceQuery.getInvites(workspaceContext.workspaceId).map { invites =>
              // toMap below will drop duplicate keys, keeping the last entry only
              // sort by access level to make sure higher access levels remain in the resulting map

              // Note: we only store share permissions in the database if a user explicitly sets them. Since owners and project owners
              // have implicit sharing permissions, we rectify that with ((accessLevel >= WorkspaceAccessLevels.Owner) || hasSharePermission) so
              // the response from getACL returns canShare and canCompute = true for owners and project owners
              val granted = emailsAndAccess.sortBy { case (_, accessLevel, _, _) => accessLevel }.map { case (email, accessLevel, hasSharePermission, hasComputePermission) => email -> AccessEntry(accessLevel, false, ((accessLevel >= WorkspaceAccessLevels.Owner) || hasSharePermission), ((accessLevel >= WorkspaceAccessLevels.Owner) || hasComputePermission)) }
              val pending = invites.sortBy { case (_, accessLevel) => accessLevel }.map { case (email, accessLevel) => email -> AccessEntry(accessLevel, true, false, false) }

              RequestComplete(StatusCodes.OK, WorkspaceACL((granted ++ pending).toMap))
            }
          }
        }
      }
    }

  def getCatalog(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        dataAccess.workspaceQuery.listEmailsWithCatalogAccess(workspaceContext).map {RequestComplete(StatusCodes.OK, _)}
      }
    }

  def updateCatalog(workspaceName: WorkspaceName, input: Seq[WorkspaceCatalog]): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        updateCatalogPermissions(input, dataAccess, workspaceContext).map {RequestComplete(StatusCodes.OK, _)}
      }
    }
  }

  /**
   * updates acls for a workspace
   * @param workspaceName
   * @param aclUpdates changes to make, if an entry already exists it will be changed to the level indicated in this
   *                   Seq, use NoAccess to remove an entry, all other preexisting accesses remain unchanged
   * @return
   */
  def updateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate], inviteUsersNotFound: Boolean): Future[PerRequestMessage] = {

    import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._

    val overwriteGroupMessagesFuture = dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireSharePermission(workspaceContext.workspace, dataAccess) { accessLevel =>
          determineCompleteNewAcls(aclUpdates, accessLevel, dataAccess, workspaceContext)
        }
      }
    }

    def getExistingWorkspaceInvites(workspaceName: WorkspaceName) = {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceQuery.getInvites(workspaceContext.workspaceId).map { pairs =>
            pairs.map(pair => WorkspaceACLUpdate(pair._1, pair._2))
          }
        }
      }
    }

    def deleteWorkspaceInvites(invites: Seq[WorkspaceACLUpdate], existingInvites: Seq[WorkspaceACLUpdate], workspaceName: WorkspaceName) = {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          val removals = invites.filter(_.accessLevel == WorkspaceAccessLevels.NoAccess)
          val dedupedRemovals = removals.filter(update => existingInvites.map(_.email).contains(update.email))
          DBIO.sequence(dedupedRemovals.map(removal => dataAccess.workspaceQuery.removeInvite(workspaceContext.workspaceId, removal.email))) map { _ =>
            dedupedRemovals
          }
        }
      }
    }

    def saveWorkspaceInvites(invites: Seq[WorkspaceACLUpdate], workspaceName: WorkspaceName): Future[Seq[WorkspaceACLUpdate]] = {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          DBIO.sequence(invites.map(invite => dataAccess.workspaceQuery.saveInvite(workspaceContext.workspaceId, userInfo.userSubjectId.value, invite)))
        }
      }
    }

    def getUsersUpdatedResponse(actualChangesToMake: Map[Either[RawlsUserRef, RawlsGroupRef], WorkspaceAccessLevel], invitesUpdated: Seq[WorkspaceACLUpdate], emailsNotFound: Seq[WorkspaceACLUpdate], existingInvites: Seq[WorkspaceACLUpdate], refsToUpdateByEmail: Map[String, Either[RawlsUserRef, RawlsGroupRef]]): WorkspaceACLUpdateResponseList = {
      val emailsByRef = refsToUpdateByEmail.map { case (k, v) => v -> k }
      val usersUpdated = actualChangesToMake.map {
        case (eitherRef@Left(RawlsUserRef(subjectId)), accessLevel) => WorkspaceACLUpdateResponse(subjectId.value, emailsByRef(eitherRef), accessLevel)
        case (eitherRef@Right(RawlsGroupRef(groupName)), accessLevel) => WorkspaceACLUpdateResponse(groupName.value, emailsByRef(eitherRef), accessLevel)
      }.toSeq

      val usersNotFound = emailsNotFound.filterNot(aclUpdate => invitesUpdated.map(_.email).contains(aclUpdate.email)).filterNot(aclUpdate => existingInvites.map(_.email).contains(aclUpdate.email))
      val invitesSent = invitesUpdated.filterNot(aclUpdate => existingInvites.map(_.email).contains(aclUpdate.email))

      WorkspaceACLUpdateResponseList(usersUpdated, invitesSent, invitesUpdated.diff(invitesSent), usersNotFound)
    }

    def updateWorkspaceSharePermissions(actualChangesToMake: Map[Either[RawlsUserRef, RawlsGroupRef], Option[Boolean]]) = {
      val (usersToAdd, usersToRemove) = actualChangesToMake.collect{ case (Left(userRef), Some(canShare)) => userRef -> canShare }.toSeq
        .partition { case (_, canShare) => canShare }
      val (groupsToAdd, groupsToRemove) = actualChangesToMake.collect{ case (Right(groupRef), Some(canShare)) => groupRef -> canShare }.toSeq
        .partition { case (_, canShare) => canShare }

      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceQuery.insertUserSharePermissions(workspaceContext.workspaceId, usersToAdd.map { case (userRef, _) => userRef } ) andThen
            dataAccess.workspaceQuery.deleteUserSharePermissions(workspaceContext.workspaceId, usersToRemove.map { case (userRef, _) => userRef } ) andThen
              dataAccess.workspaceQuery.insertGroupSharePermissions(workspaceContext.workspaceId, groupsToAdd.map { case (groupRef, _) => groupRef } ) andThen
                dataAccess.workspaceQuery.deleteGroupSharePermissions(workspaceContext.workspaceId, groupsToRemove.map { case (groupRef, _) => groupRef } )
        }
      }
    }

    def updateWorkspaceComputePermissions(actualChangesToMake: Map[Either[RawlsUserRef, RawlsGroupRef], Option[Boolean]]) = {
      val (usersToAdd, usersToRemove) = actualChangesToMake.collect{ case (Left(userRef), Some(canCompute)) => userRef -> canCompute }.toSeq
        .partition { case (_, canCompute) => canCompute }
      val (groupsToAdd, groupsToRemove) = actualChangesToMake.collect{ case (Right(groupRef), Some(canCompute)) => groupRef -> canCompute }.toSeq
        .partition { case (_, canCompute) => canCompute }

      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceQuery.insertUserComputePermissions(workspaceContext.workspaceId, usersToAdd.map { case (userRef, _) => userRef } ) andThen
            dataAccess.workspaceQuery.deleteUserComputePermissions(workspaceContext.workspaceId, usersToRemove.map { case (userRef, _) => userRef } ) andThen
              dataAccess.workspaceQuery.insertGroupComputePermissions(workspaceContext.workspaceId, groupsToAdd.map { case (groupRef, _) => groupRef } ) andThen
                dataAccess.workspaceQuery.deleteGroupComputePermissions(workspaceContext.workspaceId, groupsToRemove.map { case (groupRef, _) => groupRef } )
        }
      }
    }

    val userServiceRef = userServiceConstructor(userInfo)
    val resultsFuture = for {
      (overwriteGroupMessages, emailsNotFound, actualChangesToMake, actualShareChangesToMake, actualComputeChangesToMake, refsToUpdateByEmail) <- overwriteGroupMessagesFuture
      overwriteGroupResults <- Future.traverse(overwriteGroupMessages) { message => (userServiceRef.OverwriteGroupMembers(message.groupRef, message.memberList)) }
      existingInvites <- getExistingWorkspaceInvites(workspaceName)
      _ <- updateWorkspaceSharePermissions(actualShareChangesToMake)
      _ <- updateWorkspaceComputePermissions(actualComputeChangesToMake)
      deletedInvites <- deleteWorkspaceInvites(emailsNotFound, existingInvites, workspaceName)
      workspaceContext <- getWorkspaceContext(workspaceName)
      savedInvites <- if(inviteUsersNotFound) {
        val invites = emailsNotFound diff deletedInvites

        // only send invites for those that do not already exist
        val newInviteEmails = invites.map(_.email) diff existingInvites.map((_.email))
        val inviteNotifications = newInviteEmails.map(em => Notifications.WorkspaceInvitedNotification(RawlsUserEmail(em), userInfo.userSubjectId, workspaceName, workspaceContext.workspace.bucketName))
        notificationDAO.fireAndForgetNotifications(inviteNotifications)

        saveWorkspaceInvites(invites, workspaceName)
      } else {
        // save changes to only existing invites
        val invitesToUpdate = emailsNotFound.filter(rec => existingInvites.map(_.email).contains(rec.email)) diff deletedInvites
        saveWorkspaceInvites(invitesToUpdate, workspaceName)
      }
    } yield {
      // fire and forget notifications
      val notificationMessages = actualChangesToMake collect {
        // note that we don't send messages to groups
        case (Left(userRef), NoAccess) => Notifications.WorkspaceRemovedNotification(userRef.userSubjectId, NoAccess.toString, workspaceName, userInfo.userSubjectId)
        case (Left(userRef), access) => Notifications.WorkspaceAddedNotification(userRef.userSubjectId, access.toString, workspaceName, userInfo.userSubjectId)
      }
      notificationDAO.fireAndForgetNotifications(notificationMessages)

      overwriteGroupResults.map {
        case RequestComplete(StatusCodes.NoContent) =>
          RequestComplete(StatusCodes.OK, getUsersUpdatedResponse(actualChangesToMake, (deletedInvites ++ savedInvites), (emailsNotFound diff savedInvites), existingInvites, refsToUpdateByEmail))
        case otherwise => otherwise
      }.reduce { (prior, next) =>
        // this reduce will propagate the first non-NoContent (i.e. error) response
        prior match {
          case RequestComplete(StatusCodes.NoContent) => next
          case otherwise => prior
        }
      }
    }

    maybeShareProjectComputePolicy(resultsFuture, workspaceName)
  }

  // called from test harness
  private[workspace] def maybeShareProjectComputePolicy(resultsFuture: Future[PerRequestMessage], workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    for {
      results <- resultsFuture
      _ <- results match {
        case RequestComplete((status: StatusCode, WorkspaceACLUpdateResponseList(usersUpdated, _, _, _))) if status.isSuccess =>
          Future.traverse(usersUpdated) {
            case WorkspaceACLUpdateResponse(_, email, WorkspaceAccessLevels.Write) =>
              samDAO.addUserToPolicy(SamResourceTypeNames.billingProject, workspaceName.namespace, UserService.canComputeUserPolicyName, email, userInfo)
            case _ => Future.successful(())
          }
        case RequestComplete((status: StatusCode, _)) if status.isSuccess => Future.failed(throw new RawlsException(s"unexpected message returned: $results"))
        case _ => Future.successful(())
      }
    } yield results
  }

  def sendChangeNotifications(workspaceName: WorkspaceName): Future[PerRequestMessage] = {

    val getUsers = {
      dataSource.inTransaction{ dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) {workspaceContext =>
          requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Write, dataAccess) {
            DBIO.sequence(workspaceContext.workspace.accessLevels.values.map {group =>
              dataAccess.rawlsGroupQuery.flattenGroupMembership(group)
            })
          }
        }
      }
    }
    getUsers.map { groups =>
      val userIds = groups.flatten.map(user => user.userSubjectId).toSet
      val notificationMessages = userIds.map { userId => Notifications.WorkspaceChangedNotification(userId, workspaceName) }
      val numMessages = notificationMessages.size.toString
      notificationDAO.fireAndForgetNotifications(notificationMessages)
      RequestComplete(StatusCodes.OK, numMessages)
    }

  }

  private def updateCatalogPermissions(catalogUpdates: Seq[WorkspaceCatalog], dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext): ReadWriteAction[WorkspaceCatalogUpdateResponseList] = {

    // map from email to ref
    dataAccess.rawlsGroupQuery.loadRefsFromEmails(catalogUpdates.map(_.email)) flatMap { refsToUpdateByEmail =>
      val (emailsFound, emailsNotFound) = catalogUpdates.partition(catalogUpdate => refsToUpdateByEmail.keySet.contains(catalogUpdate.email))
      val emailsFoundWithRefs:Seq[(WorkspaceCatalog,Either[RawlsUserRef, RawlsGroupRef])] = emailsFound map {wc =>
        (wc, refsToUpdateByEmail(wc.email))
      }
      val (emailsToAddCatalog, emailsToRemoveCatalog) = emailsFoundWithRefs.partition(update => update._1.catalog)

      dataAccess.workspaceQuery.findWorkspaceUsersAndGroupsWithCatalog(workspaceContext.workspaceId) flatMap { case ((usersWithCatalog, groupsWithCatalog)) =>
        val usersToAdd = emailsToAddCatalog.collect {
          case (cat, Left(userref)) if !usersWithCatalog.contains(userref) => (userref, true)
        }
        val usersToRemove = emailsToRemoveCatalog.collect {
          case (cat, Left(userref)) if usersWithCatalog.contains(userref) => (userref, false)
        }
        val groupsToAdd = emailsToAddCatalog.collect {
          case (cat, Right(groupref)) if !groupsWithCatalog.contains(groupref) => (groupref, true)
        }
        val groupsToRemove = emailsToRemoveCatalog.collect {
          case (cat, Right(groupref)) if groupsWithCatalog.contains(groupref) => (groupref, false)
        }

        val emails = emailsNotFound.map { wsCatalog => wsCatalog.email }
        val users = (usersToAdd ++ usersToRemove).map { case (userRef, catalog) => WorkspaceCatalogResponse(userRef.userSubjectId.value, catalog) }
        val groups = (groupsToAdd ++ groupsToRemove).map { case (groupRef, catalog) => WorkspaceCatalogResponse(groupRef.groupName.value, catalog) }

        dataAccess.workspaceQuery.insertUserCatalogPermissions(workspaceContext.workspaceId, usersToAdd.map { case (userRef, _) => userRef }) andThen
          dataAccess.workspaceQuery.deleteUserCatalogPermissions(workspaceContext.workspaceId, usersToRemove.map { case (userRef, _) => userRef }) andThen
          dataAccess.workspaceQuery.insertGroupCatalogPermissions(workspaceContext.workspaceId, groupsToAdd.map { case (groupRef, _) => groupRef }) andThen
          dataAccess.workspaceQuery.deleteGroupCatalogPermissions(workspaceContext.workspaceId, groupsToRemove.map { case (groupRef, _) => groupRef }) map { _ =>
          WorkspaceCatalogUpdateResponseList(users ++ groups, emails)
        }
      }
    }
  }

  /**
   * Determine what the access groups for a workspace should look like after the requested updates are applied
   *
   * @param aclUpdates requested updates
   * @param dataAccess
   * @param workspaceContext
   * @return tuple: messages to send to UserService to overwrite acl groups, email that were not found in the process
   */
  private def determineCompleteNewAcls(aclUpdates: Seq[WorkspaceACLUpdate], userAccessLevel: WorkspaceAccessLevel, dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext): ReadWriteAction[(Iterable[UserService.OverwriteGroupMembers], Seq[WorkspaceACLUpdate], Map[Either[RawlsUserRef,RawlsGroupRef], WorkspaceAccessLevels.WorkspaceAccessLevel], Map[Either[RawlsUserRef,RawlsGroupRef], Option[Boolean]], Map[Either[RawlsUserRef,RawlsGroupRef], Option[Boolean]], Map[String, Either[RawlsUserRef,RawlsGroupRef]])] = {
    for {
      refsToUpdateByEmail <- dataAccess.rawlsGroupQuery.loadRefsFromEmails(aclUpdates.map(_.email))
      existingRefsAndLevels <- dataAccess.workspaceQuery.findWorkspaceUsersAndAccessLevel(workspaceContext.workspaceId)
    } yield {
      val emailsNotFound = aclUpdates.filterNot(aclChange => refsToUpdateByEmail.keySet.map(_.toLowerCase).contains(aclChange.email.toLowerCase))
      val lcRefsToUpdateByEmail = refsToUpdateByEmail.map{ case (key, value) => (key.toLowerCase, value)}

      // match up elements of aclUpdates and refsToUpdateByEmail ignoring unfound emails
      val refsToUpdate = aclUpdates.map { aclUpdate => (lcRefsToUpdateByEmail.get(aclUpdate.email.toLowerCase), aclUpdate.accessLevel, aclUpdate.canShare, aclUpdate.canCompute) }.collect {
        case (Some(ref), WorkspaceAccessLevels.NoAccess, _, _) => ref -> (WorkspaceAccessLevels.NoAccess, Option(false), Option(false))
        // this next case provides backwards compatibility for when write is specified but canCompute is not, in this case canCompute should be true by default
        case (Some(ref), WorkspaceAccessLevels.Write, canShare, None) => ref -> (WorkspaceAccessLevels.Write, canShare, Option(true))
        case (Some(ref), accessLevel, canShare, canCompute) => ref -> (accessLevel, canShare, canCompute)
      }.toSet

      val refsToUpdateAndSharePermission = refsToUpdate.map { case (ref, (_, canShare, _)) => ref -> canShare }
      val refsToUpdateAndComputePermission = refsToUpdate.map { case (ref, (_, _, canCompute)) => ref -> canCompute }
      val refsToUpdateAndAccessLevel = refsToUpdate.map { case (ref, (accessLevel, _, _)) => ref -> accessLevel }

      val existingRefsAndSharePermission = existingRefsAndLevels.map { case (ref, (_, canShare, _))  => ref -> canShare }
      val existingRefsAndComputePermission = existingRefsAndLevels.map { case (ref, (_, _, canCompute))  => ref -> canCompute }
      val existingRefsAndAccessLevel = existingRefsAndLevels.map { case (ref, (accessLevel, _, _)) => ref -> accessLevel }

      // remove everything that is not changing
      val actualAccessChangesToMake = refsToUpdateAndAccessLevel.diff(existingRefsAndAccessLevel)
      val actualShareChangesToMake = refsToUpdateAndSharePermission.filter{ case (_, canShare) => canShare.isDefined }
        .diff(existingRefsAndSharePermission.map { case (ref, canShare) => ref -> Option(canShare)})
      val actualComputeChangesToMake = refsToUpdateAndComputePermission.filter{ case (_, canCompute) => canCompute.isDefined }
        .diff(existingRefsAndComputePermission.map { case (ref, canCompute) => ref -> Option(canCompute)})

      val membersWithTooManyEntries = actualAccessChangesToMake.groupBy {
        case (member, _) => member
      }.collect {
        case (member, entries) if entries.size > 1 => refsToUpdateByEmail.collect {
          case (email, member2) if member == member2 => email
        }
      }.flatten

      if (membersWithTooManyEntries.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Only 1 entry per email allowed. Emails with more than one entry: $membersWithTooManyEntries"))
      }

      val membersWithHigherExistingAccessLevelThanGranter = existingRefsAndLevels.filterNot { case (_, (accessLevel, _, _)) => accessLevel == WorkspaceAccessLevels.ProjectOwner }
        .filter { case (member, _) => actualAccessChangesToMake.map { case(ref, _) => ref }.contains(member) }.filter { case (_, (accessLevel, _, _)) => accessLevel > userAccessLevel }

      if (membersWithHigherExistingAccessLevelThanGranter.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not alter the access level of users with higher access than yourself. Please correct these entries: $membersWithHigherExistingAccessLevelThanGranter"))
      }

      val membersWithHigherAccessLevelThanGranter = actualAccessChangesToMake.filter { case (_, accessLevel) => accessLevel > userAccessLevel }

      if (membersWithHigherAccessLevelThanGranter.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not grant higher access than your own access level. Please correct these entries: $membersWithHigherAccessLevelThanGranter"))
      }

      val membersWithSharePermission = actualShareChangesToMake.filter { case (_, canShare) => canShare.isDefined }
      val membersWithComputePermission = actualComputeChangesToMake.filter { case (_, canCompute) => canCompute.isDefined }

      if(membersWithSharePermission.nonEmpty && userAccessLevel < WorkspaceAccessLevels.Owner) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not alter the share permissions of users unless you are a workspace owner. Please correct these entries: $membersWithSharePermission"))
      }
      if(membersWithComputePermission.nonEmpty && userAccessLevel < WorkspaceAccessLevels.Owner) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not alter the compute permissions of users unless you are a workspace owner. Please correct these entries: $membersWithComputePermission"))
      }
      if(refsToUpdate.exists {
        case (_, (WorkspaceAccessLevels.Read, _, Some(true))) => true
        case _ => false
      }) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Readers may not be granted compute permissions"))
      }

      val actualChangesToMakeByMember = actualAccessChangesToMake.toMap
      val actualShareChangesToMakeByMember = actualShareChangesToMake.toMap
      val actualComputeChangesToMakeByMember = actualComputeChangesToMake.toMap

      // some security checks
      if (actualChangesToMakeByMember.contains(Right(UserService.allUsersGroupRef))) {
        // UserService.allUsersGroupRef cannot be updated in this code path, there is an admin end point for that
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Please contact an administrator to alter access to ${UserService.allUsersGroupRef.groupName}"))
      }
      if (actualChangesToMakeByMember.contains(Left(RawlsUser(userInfo)))) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "You may not change your own permissions"))
      }
      if (actualChangesToMakeByMember.exists { case (_, level) => level == ProjectOwner }) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Project owners can only be added in the billing area of the application."))
      }
      val existingProjectOwners = existingRefsAndLevels.collect { case (member, (ProjectOwner, _, _)) => member }
      if ((actualChangesToMakeByMember.keySet intersect existingProjectOwners).nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Project owners can only be removed in the billing area of the application."))
      }

      // we are not updating ProjectOwner acls here, if any are added we threw an exception above
      // any user/group that is already a project owner and is added to another level should end up in both levels
      // so lets ignore all the existing project owners, we should not update by mistake
      val existingRefsAndLevelsExcludingPO = existingRefsAndAccessLevel.filterNot { case (_, level) => level == ProjectOwner }

      // update levels for all existing refs, add refs that don't exist, remove all no access levels
      val updatedRefsAndLevels =
        (existingRefsAndLevelsExcludingPO.map { case (ref, level) => ref -> actualChangesToMakeByMember.getOrElse(ref, level) } ++
          actualChangesToMakeByMember).filterNot { case (_, level) => level == WorkspaceAccessLevels.NoAccess }

      // formulate the UserService.OverwriteGroupMembers messages to send - 1 per level with users and groups separated
      val updatedRefsByLevel: Map[WorkspaceAccessLevel, Set[(Either[RawlsUserRef, RawlsGroupRef], WorkspaceAccessLevel)]] =
        updatedRefsAndLevels.toSet.groupBy { case (_, level) => level }

      // the above transformations drop empty groups so add those back in
      val emptyLevels = Seq(WorkspaceAccessLevels.Owner, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Read).map(_ -> Set.empty[(Either[RawlsUserRef, RawlsGroupRef], WorkspaceAccessLevel)]).toMap
      val updatedRefsByLevelWithEmpties = emptyLevels ++ updatedRefsByLevel

      val overwriteGroupMessages = updatedRefsByLevelWithEmpties.map { case (level, refs) =>
        val userSubjectIds = refs.collect { case (Left(userRef), _) => userRef.userSubjectId.value }.toSeq
        val subGroupNames = refs.collect { case (Right(groupRef), _) => groupRef.groupName.value }.toSeq
        UserService.OverwriteGroupMembers(workspaceContext.workspace.accessLevels(level), RawlsGroupMemberList(userSubjectIds = Option(userSubjectIds), subGroupNames = Option(subGroupNames)))
      }

      // voila
      (overwriteGroupMessages, emailsNotFound, actualChangesToMakeByMember, actualShareChangesToMakeByMember, actualComputeChangesToMakeByMember, refsToUpdateByEmail)
    }
  }

  def lockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Owner, dataAccess) {
          dataAccess.submissionQuery.list(workspaceContext).flatMap { submissions =>
            if (!submissions.forall(_.status.isTerminated)) {
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"There are running submissions in workspace $workspaceName, so it cannot be locked.")))
            } else {
              dataAccess.workspaceQuery.lock(workspaceContext.workspace.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
            }
          }
        }
      }
    }

  def unlockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Owner, dataAccess) {
          dataAccess.workspaceQuery.unlock(workspaceContext.workspace.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, WorkspaceAccessLevels.Write, dataAccess) { destWorkspaceContext =>
        withWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace, WorkspaceAccessLevels.Read, dataAccess) { sourceWorkspaceContext =>
          authDomainCheck(sourceWorkspaceContext, destWorkspaceContext) flatMap { _ =>
            val entityNames = entityCopyDef.entityNames
            val entityType = entityCopyDef.entityType
            val copyResults = dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, linkExistingEntities)
            copyResults.map { response =>
              if(response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntities)) RequestComplete(StatusCodes.Created, response)
              else RequestComplete(StatusCodes.Conflict, response)
            }
          }
        }
      }
    }

  // can't use withClonedAuthDomain because the Auth Domain -> no Auth Domain logic is different
  private def authDomainCheck(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Boolean] = {
    // if the source has any auth domains, the dest must also *at least* have those auth domains
    val sourceWorkspaceADs = sourceWorkspaceContext.workspace.authorizationDomain
    val destWorkspaceADs = destWorkspaceContext.workspace.authorizationDomain

    if(sourceWorkspaceADs.subsetOf(destWorkspaceADs)) DBIO.successful(true)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has an Authorization Domain containing the groups ${missingGroups.map(_.membersGroupName.value).mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }
  }

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] =
    withAttributeNamespaceCheck(entity) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspaceName}")))
            case None => dataAccess.entityQuery.save(workspaceContext, entity)
          }
        }
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean = false): Future[PerRequestMessage] = {
    val namesToCheck = for {
      update <- entityUpdates
      operation <- update.operations
    } yield operation.name

    withAttributeNamespaceCheck(namesToCheck) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          val updateTrialsAction = dataAccess.entityQuery.getActiveEntities(workspaceContext, entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name))) map { entities =>
            val entitiesByName = entities.map(e => (e.entityType, e.name) -> e).toMap
            entityUpdates.map { entityUpdate =>
              entityUpdate -> (entitiesByName.get((entityUpdate.entityType, entityUpdate.name)) match {
                case Some(e) =>
                  Try(applyOperationsToEntity(e, entityUpdate.operations))
                case None =>
                  if (upsert) {
                    Try(applyOperationsToEntity(Entity(entityUpdate.name, entityUpdate.entityType, Map.empty), entityUpdate.operations))
                  } else {
                    Failure(new RuntimeException("Entity does not exist"))
                  }
              })
            }
          }

          val saveAction = updateTrialsAction flatMap { updateTrials =>
            val errorReports = updateTrials.collect { case (entityUpdate, Failure(regrets)) =>
              ErrorReport(s"Could not update ${entityUpdate.entityType} ${entityUpdate.name}", ErrorReport(regrets))
            }
            if (!errorReports.isEmpty) {
              DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.", errorReports)))
            } else {
              val t = updateTrials.collect { case (entityUpdate, Success(entity)) => entity }

              dataAccess.entityQuery.save(workspaceContext, t)
            }
          }

          saveAction.map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }
  }

  def entityTypeMetadata(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.getEntityTypeMetadata(workspaceContext).map(r => RequestComplete(StatusCodes.OK, r))
      }
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.listActiveEntitiesOfType(workspaceContext, entityType).map(r => RequestComplete(StatusCodes.OK, r.toSeq))
      }
    }

  def queryEntities(workspaceName: WorkspaceName, entityType: String, query: EntityQuery): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.loadEntityPage(workspaceContext, entityType, query) map { case (unfilteredCount, filteredCount, entities) =>
          createEntityQueryResponse(query, unfilteredCount, filteredCount, entities.toSeq).get
        }
      }
    }
  }

  def createEntityQueryResponse(query: EntityQuery, unfilteredCount: Int, filteredCount: Int, page: Seq[Entity]): Try[RequestComplete[(StatusCodes.Success, EntityQueryResponse)]] = {
    val pageCount = Math.ceil(filteredCount.toFloat / query.pageSize).toInt
    if (filteredCount > 0 && query.page > pageCount) {
      Failure(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"requested page ${query.page} is greater than the number of pages $pageCount")))

    } else {
      val response = EntityQueryResponse(query, EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount), page)

      Success(RequestComplete(StatusCodes.OK, response))
    }
  }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, entity))
        }
      }
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
            val updateAction = Try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              dataAccess.entityQuery.save(workspaceContext, updatedEntity)
            } match {
              case Success(result) => result
              case Failure(e: AttributeUpdateOperationException) =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}", ErrorReport(e))))
              case Failure(regrets) => DBIO.failed(regrets)
            }
            updateAction.map(RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withAllEntities(workspaceContext, dataAccess, entRefs) { entities =>
          dataAccess.entityQuery.getAllReferringEntities(workspaceContext, entRefs.toSet) flatMap { referringEntities =>
            if (referringEntities != entRefs.toSet)
              DBIO.successful(RequestComplete(StatusCodes.Conflict, referringEntities))
            else {
              dataAccess.entityQuery.hide(workspaceContext, entRefs).map(_ => RequestComplete(StatusCodes.NoContent))
            }
          }
        }
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
          } map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withSingleEntityRec(entityType, entityName, workspaceContext, dataAccess) { entities =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, Some(entities)) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, expression).asTry map { tryValuesByEntity => tryValuesByEntity match {
              //parsing failure
              case Failure(regret) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret))
              case Success(valuesByEntity) =>
                if (valuesByEntity.size != 1) {
                  //wrong number of entities?!
                  throw new RawlsException(s"Expression parsing should have returned a single entity for ${entityType}/$entityName $expression, but returned ${valuesByEntity.size} entities instead")
                } else {
                  assert(valuesByEntity.head._1 == entityName)
                  valuesByEntity.head match {
                    case (_, Success(result)) => RequestComplete(StatusCodes.OK, result.toSeq)
                    case (_, Failure(regret)) =>
                      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}", ErrorReport(regret)))
                  }
                }
              }
            }
          }
        }
      }
    }

  /**
   * Applies the sequence of operations in order to the entity.
   *
   * @param entity to update
   * @param operations sequence of operations
   * @throws AttributeNotFoundException when removing from a list attribute that does not exist
   * @throws AttributeUpdateOperationException when adding or removing from an attribute that is not a list
   * @return the updated entity
   */
  def applyOperationsToEntity(entity: Entity, operations: Seq[AttributeUpdateOperation]): Entity = {
    entity.copy(attributes = applyAttributeUpdateOperations(entity, operations))
  }

  /**
   * Applies the sequence of operations in order to the workspace.
   *
   * @param workspace to update
   * @param operations sequence of operations
   * @throws AttributeNotFoundException when removing from a list attribute that does not exist
   * @throws AttributeUpdateOperationException when adding or removing from an attribute that is not a list
   * @return the updated entity
   */
  def applyOperationsToWorkspace(workspace: Workspace, operations: Seq[AttributeUpdateOperation]): Workspace = {
    workspace.copy(attributes = applyAttributeUpdateOperations(workspace, operations))
  }

  private def applyAttributeUpdateOperations(attributable: Attributable, operations: Seq[AttributeUpdateOperation]): AttributeMap = {
    operations.foldLeft(attributable.attributes) { (startingAttributes, operation) =>

      operation match {
        case AddUpdateAttribute(attributeName, attribute) => startingAttributes + (attributeName -> attribute)

        case RemoveAttribute(attributeName) => startingAttributes - attributeName

        case CreateAttributeEntityReferenceList(attributeName) =>
          if( startingAttributes.contains(attributeName) ) { //non-destructive
            startingAttributes
          } else {
            startingAttributes + (attributeName -> AttributeEntityReferenceEmptyList)
          }

        case CreateAttributeValueList(attributeName) =>
          if( startingAttributes.contains(attributeName) ) { //non-destructive
            startingAttributes
          } else {
            startingAttributes + (attributeName -> AttributeValueEmptyList)
          }

        case AddListMember(attributeListName, newMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(AttributeValueEmptyList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(Seq(newMember)))
                case newMember: AttributeEntityReference =>
                  throw new AttributeUpdateOperationException("Cannot add non-value to list of values.")
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(AttributeEntityReferenceEmptyList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(Seq(newMember)))
                case newMember: AttributeValue =>
                  throw new AttributeUpdateOperationException("Cannot add non-reference to list of references.")
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(l: AttributeValueList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(l.list :+ newMember))
                case _ => throw new AttributeUpdateOperationException("Cannot add non-value to list of values.")
              }

            case Some(l: AttributeEntityReferenceList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(l.list :+ newMember))
                case _ => throw new AttributeUpdateOperationException("Cannot add non-reference to list of references.")
              }

            case None =>
              newMember match {
                case AttributeNull =>
                  throw new AttributeUpdateOperationException("Cannot use AttributeNull to create empty list. Use CreateEmpty[Ref|Val]List instead.")
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(Seq(newMember)))
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(Seq(newMember)))
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.briefName} is not a list")
          }

        case RemoveListMember(attributeListName, removeMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(l: AttributeValueList) =>
              startingAttributes + (attributeListName -> AttributeValueList(l.list.filterNot(_ == removeMember)))
            case Some(l: AttributeEntityReferenceList) =>
              startingAttributes + (attributeListName -> AttributeEntityReferenceList(l.list.filterNot(_ == removeMember)))
            case None => throw new AttributeNotFoundException(s"$attributeListName of ${attributable.briefName} does not exist")
            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.briefName} is not a list")
          }
      }
    }
  }

  def createMCAndValidateExpressions(workspaceContext: SlickWorkspaceContext, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration) map { _ =>
      ExpressionValidator.validateAndParseMCExpressions(methodConfiguration, methodConfiguration.rootEntityType.isDefined, dataAccess)
    }
  }

  def updateMCAndValidateExpressions(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration) map { _ =>
      ExpressionValidator.validateAndParseMCExpressions(methodConfiguration, methodConfiguration.rootEntityType.isDefined, dataAccess)
    }
  }

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, ExpressionValidator.validateAndParseMCExpressions(methodConfig, methodConfig.rootEntityType.isDefined, dataAccess)))
        }
      }
    }
  }

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}")))
            case None => createMCAndValidateExpressions(workspaceContext, methodConfiguration, dataAccess)
          }
        }
      }
    }
  }

  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          dataAccess.methodConfigurationQuery.delete(workspaceContext, methodConfigurationNamespace, methodConfigurationName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        //It's terrible that we pass unnecessary junk that we don't read in the payload, but a big refactor of the API is going to have to wait until Some Other Time.
        if(newName.workspaceName != workspaceName) {
          DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Workspace name and namespace in payload must match those in the URI")))
        } else {
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfiguration =>
            //If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
            dataAccess.methodConfigurationQuery.get(workspaceContext, newName.namespace, newName.name) flatMap {
              case Some(_) if methodConfigurationNamespace != newName.namespace || methodConfigurationName != newName.name =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport =
                  ErrorReport(StatusCodes.Conflict, s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
              case Some(_) => DBIO.successful(()) //renaming self to self: no-op
              case None =>
                dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration.copy(name = newName.name, namespace = newName.namespace))
            } map (_ => RequestComplete(StatusCodes.NoContent))
          }
        }
      }
    }

  //Overwrite the method configuration at methodConfiguration[namespace|name] with the new method configuration.
  def overwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      // create transaction
      dataSource.inTransaction { dataAccess =>
        // check permissions
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          if(methodConfiguration.namespace != methodConfigurationNamespace || methodConfiguration.name != methodConfigurationName) {
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest,
              s"The method configuration name and namespace in the URI should match the method configuration name and namespace in the request body. If you want to move this method configuration, use POST.")))
          } else {
            createMCAndValidateExpressions(workspaceContext, methodConfiguration, dataAccess)
          }
        }
      }
    }
  }

  //Move the method configuration at methodConfiguration[namespace|name] to the location specified in methodConfiguration, _and_ update it.
  //It's like a rename and upsert all rolled into one.
  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(methodConfiguration) {
     // create transaction
      dataSource.inTransaction { dataAccess =>
       // check permissions
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { _ =>
              dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
                //If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
                case Some(_) if methodConfigurationNamespace != methodConfiguration.namespace || methodConfigurationName != methodConfiguration.name =>
                  DBIO.failed(new RawlsExceptionWithErrorReport(errorReport =
                    ErrorReport(StatusCodes.Conflict, s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
                case _ =>
                  updateMCAndValidateExpressions(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration, dataAccess)
              } map (RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }
  }

  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[ValidatedMethodConfiguration] = {
    // split into two transactions because we need to call out to Google after retrieving the source MC

    val transaction1Result = dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(mcnp.destination.workspaceName, WorkspaceAccessLevels.Write, dataAccess) { destContext =>
        withWorkspaceContextAndPermissions(mcnp.source.workspaceName, WorkspaceAccessLevels.Read, dataAccess) { sourceContext =>
          dataAccess.methodConfigurationQuery.get(sourceContext, mcnp.source.namespace, mcnp.source.name) flatMap {
            case None =>
              val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}.")
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = err))
            case Some(methodConfig) => DBIO.successful((methodConfig, destContext))
          }
        }
      }
    }

    transaction1Result flatMap { case (methodConfig, destContext) =>
      withAttributeNamespaceCheck(methodConfig) {
        dataSource.inTransaction { dataAccess =>
          saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, dataAccess)
        }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[ValidatedMethodConfiguration] =
    methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo) flatMap {
      case None =>
        val name = s"${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId}"
        val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named $name in the repository.")
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = err))
      case Some(agoraEntity) => Future.fromTry(parseAgoraEntity(agoraEntity)) flatMap { targetMethodConfig =>
        withAttributeNamespaceCheck(targetMethodConfig) {
          dataSource.inTransaction { dataAccess =>
            withWorkspaceContextAndPermissions(methodRepoQuery.destination.workspaceName, WorkspaceAccessLevels.Write, dataAccess) { destContext =>
              saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, dataAccess)
            }
          }
        }
      }
    }

  private def parseAgoraEntity(agoraEntity: AgoraEntity): Try[MethodConfiguration] = {
    val parsed = Try {
      agoraEntity.payload.map(JsonParser(_).convertTo[AgoraMethodConfiguration])
    } recoverWith {
      case e: Exception =>
        Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))))
    }

    parsed flatMap {
      case Some(agoraMC) => Success(convertToMethodConfiguration(agoraMC))
      case None => Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")))
    }
  }

  private def convertToMethodConfiguration(agoraMethodConfig: AgoraMethodConfiguration): MethodConfiguration = {
    MethodConfiguration(agoraMethodConfig.namespace, agoraMethodConfig.name, Some(agoraMethodConfig.rootEntityType), agoraMethodConfig.prerequisites, agoraMethodConfig.inputs, agoraMethodConfig.outputs, agoraMethodConfig.methodRepoMethod)
  }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodRepoQuery.source.namespace, methodRepoQuery.source.name, dataAccess) { methodConfig =>

          DBIO.from(methodRepoDAO.postMethodConfig(
            methodRepoQuery.methodRepoNamespace,
            methodRepoQuery.methodRepoName,
            methodConfig.copy(namespace = methodRepoQuery.methodRepoNamespace, name = methodRepoQuery.methodRepoName),
            userInfo)) map { RequestComplete(StatusCodes.OK, _) }
        }
      }
    }
  }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration, dest: MethodConfigurationName, destContext: SlickWorkspaceContext, dataAccess: DataAccess) = {
    val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)

    dataAccess.methodConfigurationQuery.get(destContext, dest.namespace, dest.name).flatMap {
      case Some(existingMethodConfig) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}")))
      case None => createMCAndValidateExpressions(destContext, target, dataAccess)
    }
  }

  def listAgoraMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          RequestComplete(StatusCodes.OK, r.toList.filter(_.methodRepoMethod.repo == Agora))
        }
      }
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          RequestComplete(StatusCodes.OK, r.toList)
        }
      }
    }

  def createMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withMethod(methodRepoMethod, userInfo) { method =>
        withWdl(method) { wdl => MethodConfigResolver.toMethodConfiguration(wdl, methodRepoMethod) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodConfig) => DBIO.successful(RequestComplete(StatusCodes.OK, methodConfig))
        }}
      }
    }
  }

  def getMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withMethod(methodRepoMethod, userInfo) { method =>
        withWdl(method) { wdl => MethodConfigResolver.getMethodInputsOutputs(wdl) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(inputsOutputs) => DBIO.successful(RequestComplete(StatusCodes.OK, inputsOutputs))
        }}
      }
    }
  }

  def listSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.submissionQuery.listWithSubmitter(workspaceContext)
          .map(RequestComplete(StatusCodes.OK, _))
      }
    }

  def countSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.submissionQuery.countByStatus(workspaceContext).map(RequestComplete(StatusCodes.OK, _))
      }
    }

  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] = {
    withSubmissionParameters(workspaceName, submissionRequest) {
      (dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext, wdl: String, header: SubmissionValidationHeader, successes: Seq[SubmissionValidationEntityInputs], failures: Seq[SubmissionValidationEntityInputs], workflowFailureMode: Option[WorkflowFailureMode]) =>
        requireComputePermission(workspaceContext.workspace, dataAccess) {
          val submissionId: UUID = UUID.randomUUID()
          val submissionEntityOpt = if(header.entityType.isEmpty) { None } else { Some(AttributeEntityReference(entityType = submissionRequest.entityType.get, entityName = submissionRequest.entityName.get)) }

          val workflows = successes map { entityInputs =>
            val workflowEntityOpt = header.entityType.map(_ => AttributeEntityReference(entityType = header.entityType.get, entityName = entityInputs.entityName))
            Workflow(workflowId = None,
              status = WorkflowStatuses.Queued,
              statusLastChangedDate = DateTime.now,
              workflowEntity = workflowEntityOpt,
              inputResolutions = entityInputs.inputResolutions
            )
          }

          val workflowFailures = failures map { entityInputs =>
            val workflowEntityOpt = header.entityType.map(_ => AttributeEntityReference(entityType = header.entityType.get, entityName = entityInputs.entityName))
            Workflow(workflowId = None,
              status = WorkflowStatuses.Failed,
              statusLastChangedDate = DateTime.now,
              workflowEntity = workflowEntityOpt,
              inputResolutions = entityInputs.inputResolutions,
              messages = for (entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined) yield (AttributeString(entityValue.inputName + " - " + entityValue.error.get))
            )
          }

          val submission = Submission(submissionId = submissionId.toString,
            submissionDate = DateTime.now(),
            submitter = RawlsUser(userInfo),
            methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
            methodConfigurationName = submissionRequest.methodConfigurationName,
            submissionEntity = submissionEntityOpt,
            workflows = workflows ++ workflowFailures,
            status = SubmissionStatuses.Submitted,
            useCallCache = submissionRequest.useCallCache,
            workflowFailureMode = workflowFailureMode
          )

          // implicitly passed to SubmissionComponent.create
          implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceName))
          implicit val wfStatusCounter = workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceName, submissionId))

          dataAccess.submissionQuery.create(workspaceContext, submission) map { _ =>
            RequestComplete(StatusCodes.Created, SubmissionReport(submissionRequest, submission.submissionId, submission.submissionDate, userInfo.userEmail.value, submission.status, header, successes))
          }
        }
    }
  }

  def validateSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] =
    withSubmissionParameters(workspaceName,submissionRequest) {
      (dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext, wdl: String, header: SubmissionValidationHeader, succeeded: Seq[SubmissionValidationEntityInputs], failed: Seq[SubmissionValidationEntityInputs], _) =>
        DBIO.successful(RequestComplete(StatusCodes.OK, SubmissionValidationReport(submissionRequest, header, succeeded, failed)))
    }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String): Future[PerRequestMessage] = {
    val submissionWithoutCosts = dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
          withUser(submission.submitter, dataAccess) { user =>
            DBIO.successful(submission, user)
          }
        }
      }
    }

    submissionWithoutCosts flatMap {
      case (submission, user) => {
        val allWorkflowIds: Seq[String] = submission.workflows.flatMap(_.workflowId)
        toFutureTry(submissionCostService.getWorkflowCosts(allWorkflowIds, workspaceName.namespace)) map {
          case Failure(ex) =>
            logger.error("Unable to get workflow costs for this submission. ", ex)
            RequestComplete((StatusCodes.OK, new SubmissionStatusResponse(submission, user)))
          case Success(costMap) =>
            val costedWorkflows = submission.workflows.map { workflow =>
              workflow.workflowId match {
                case Some(wfId) => workflow.copy(cost = costMap.get(wfId))
                case None => workflow
              }
            }
            val costedSubmission = submission.copy(cost = Some(costMap.values.sum), workflows = costedWorkflows)
            RequestComplete((StatusCodes.OK, new SubmissionStatusResponse(costedSubmission, user)))
        }
      }
    }
  }

  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        abortSubmission(workspaceContext, submissionId, dataAccess)
      }
    }
  }

  private def abortSubmission(workspaceContext: SlickWorkspaceContext, submissionId: String, dataAccess: DataAccess): ReadWriteAction[PerRequestMessage] = {
    withSubmissionId(workspaceContext, submissionId, dataAccess) { submissionId =>
      // implicitly passed to SubmissionComponent.updateStatus
      implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceContext.workspace.toWorkspaceName))
      dataAccess.submissionQuery.updateStatus(submissionId, SubmissionStatuses.Aborting) map { rows =>
        if(rows == 1)
          RequestComplete(StatusCodes.NoContent)
        else
          RequestComplete(ErrorReport(StatusCodes.NotFound, s"Unable to abort submission. Submission ${submissionId} could not be found."))
      }
    }
  }

  /**
   * Munges together the output of Cromwell's /outputs and /logs endpoints, grouping them by task name */
  private def mergeWorkflowOutputs(execOuts: ExecutionServiceOutputs, execLogs: ExecutionServiceLogs, workflowId: String): PerRequestMessage = {
    val outs = execOuts.outputs
    val logs = execLogs.calls

    //Cromwell workflow outputs look like workflow_name.task_name.output_name.
    //Under perverse conditions it might just be workflow_name.output_name.
    //Group outputs by everything left of the rightmost dot.
    val outsByTask = outs groupBy { case (k,_) => k.split('.').dropRight(1).mkString(".") }

    val taskMap = (outsByTask.keySet ++ logs.keySet).map( key => key -> TaskOutput( logs.get(key), outsByTask.get(key)) ).toMap
    RequestComplete(StatusCodes.OK, WorkflowOutputs(workflowId, taskMap))
  }

  /**
   * Get the list of outputs for a given workflow in this submission */
  def workflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withWorkflowRecord(workspaceName, submissionId, workflowId, dataAccess) { wr =>
          val outputFTs = toFutureTry(executionServiceCluster.outputs(wr, userInfo))
          val logFTs = toFutureTry(executionServiceCluster.logs(wr, userInfo))
          DBIO.from(outputFTs zip logFTs map {
            case (Success(outputs), Success(logs)) =>
              mergeWorkflowOutputs(outputs, logs, workflowId)
            case (Failure(outputsFailure), Success(logs)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs for ${submissionId}.", executionServiceCluster.toErrorReport(outputsFailure)))
            case (Success(outputs), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get logs for ${submissionId}.", executionServiceCluster.toErrorReport(logsFailure)))
            case (Failure(outputsFailure), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs and unable to get logs for ${submissionId}.",
                Seq(executionServiceCluster.toErrorReport(outputsFailure),executionServiceCluster.toErrorReport(logsFailure))))
          })
        }
      }
    }
  }

  def workflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String): Future[PerRequestMessage] = {

    // two possibilities here:
    //
    // (classic case) if the workflow is a top-level workflow of a submission, it has a row in the DB and an
    // association with a specific execution service shard.  Use the DB to verify the submission association and retrieve
    // the execution service identifier.
    //
    // if it's a subworkflow (or sub-sub-workflow, etc) it's not present in the Rawls DB and we don't know which
    // execution service shard has processed it.  Query all* execution service shards for the workflow to learn its
    // submission association and which shard processed it.
    //
    // * in practice, one shard does everything except for some older workflows on shard 2.  Revisit this if that changes!

    // determine which case this is, and close the DB transaction
    val execIdFutOpt: Future[Option[ExecutionServiceId]] = dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withSubmissionAndWorkflowExecutionServiceKey(workspaceContext, submissionId, workflowId, dataAccess) { optExecKey =>
          DBIO.successful(optExecKey map (ExecutionServiceId(_)))
        }
      }
    }

    // query the execution service(s) for the metadata
    execIdFutOpt flatMap {
      executionServiceCluster.callLevelMetadata(submissionId, workflowId, _, userInfo)
    } map(RequestComplete(StatusCodes.OK, _))
  }

  def workflowQueueStatus() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.countWorkflowsByQueueStatus.flatMap { statusMap =>
        // determine the current size of the workflow queue
        statusMap.get(WorkflowStatuses.Queued.toString) match {
          case Some(x) if x > 0 =>
            for {
              timeEstimate <- dataAccess.workflowAuditStatusQuery.queueTimeMostRecentSubmittedWorkflow
              workflowsAhead <- dataAccess.workflowQuery.countWorkflowsAheadOfUserInQueue(userInfo)
            } yield {
              RequestComplete(StatusCodes.OK, WorkflowQueueStatusResponse(timeEstimate, workflowsAhead, statusMap))
            }
          case _ => DBIO.successful(RequestComplete(StatusCodes.OK, WorkflowQueueStatusResponse(0, 0, statusMap)))
        }
      }
    }
  }

  def adminWorkflowQueueStatusByUser() = {
    asFCAdmin {
      dataSource.inTransaction ({ dataAccess =>
        for {
          global <- dataAccess.workflowQuery.countWorkflowsByQueueStatus
          perUser <- dataAccess.workflowQuery.countWorkflowsByQueueStatusByUser
        } yield RequestComplete(StatusCodes.OK, WorkflowQueueStatusByUserResponse(global, perUser, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser))
      }, TransactionIsolation.ReadUncommitted)
    }
  }

  def checkBucketReadAccess(workspaceName: WorkspaceName) = {
    for {
      (workspace, maxAccessLevel) <- dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
          getMaximumAccessLevel(RawlsUser(userInfo), workspaceContext, dataAccess).map { accessLevel =>
            (workspaceContext.workspace, accessLevel)
          }
        }
      }

      petKey <- samDAO.getPetServiceAccountKeyForUser(workspace.namespace, userInfo.userEmail)

      accessToken <- gcsDAO.getAccessTokenUsingJson(petKey)

      //if the user only has read access, it doesn't matter if their pet doesn't have access because it will never be used. so we skip over it.
      resultsForPet <- if (maxAccessLevel >= WorkspaceAccessLevels.Write) {
        gcsDAO.diagnosticBucketRead(UserInfo(userInfo.userEmail, OAuth2BearerToken(accessToken), 60, userInfo.userSubjectId), workspace.bucketName)
      } else Future.successful(None)
      resultsForUser <- gcsDAO.diagnosticBucketRead(userInfo, workspace.bucketName)
    } yield {
      (resultsForUser, resultsForPet) match {
        case (None, None) => RequestComplete(StatusCodes.OK)
        case (Some(report), _) => RequestComplete(report) // report actual user does not have access first
        case (_, Some(report)) => RequestComplete(report)
      }
    }
  }

  def listAllActiveSubmissions() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.listAllActiveSubmissions().map(RequestComplete(StatusCodes.OK, _))
    }
  }

  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          abortSubmission(workspaceContext, submissionId, dataAccess)
        }
      }
    }
  }

  def hasAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.rawlsGroupQuery.load(workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read)) map { readerGroup =>
            readerGroup match {
              case Some(group) =>
                if (group.subGroups.contains(UserService.allUsersGroupRef)) {
                  RequestComplete(StatusCodes.NoContent)
                } else {
                  RequestComplete(StatusCodes.NotFound)
                }
              case None =>
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, "allUsersGroup not found"))
            }
          }
        }
      }
    }
  }

  def grantAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          val userServiceRef = userServiceConstructor(userInfo)
          DBIO.from(userServiceRef.AddGroupMembers(
            workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read),
            RawlsGroupMemberList(subGroupNames = Option(Seq(UserService.allUsersGroupRef.groupName.value)))))
        } map {
          case RequestComplete(StatusCodes.OK) => RequestComplete(StatusCodes.Created)
          case otherwise => otherwise
        }
      }
    }
  }

  def revokeAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          val userServiceRef = userServiceConstructor(userInfo)
          DBIO.from(userServiceRef.RemoveGroupMembers(
            workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read),
            RawlsGroupMemberList(subGroupNames = Option(Seq(UserService.allUsersGroupRef.groupName.value)))))
        }
      }
    }
  }

  def listAllWorkspaces() = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listAll.map(RequestComplete(StatusCodes.OK, _))
      }
    }
  }

  def listWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.listWithAttribute(attributeName, attributeValue).map(RequestComplete(StatusCodes.OK, _))
    }
  }

  // this function is a bit naughty because it waits for database results
  // this is acceptable because it is a seldom used admin functions, not part of the mainstream
  // refactoring it to do the propper database action chaining is too much work at this time
  def getWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]): Future[PerRequestMessage] = {
    def run[T](action: DataAccess => ReadWriteAction[T]): T = {
      Await.result(dataSource.inTransaction { dataAccess => action(dataAccess) }, 10 seconds)
    }

    asFCAdmin {
      val workspace = run { _.workspaceQuery.findByName(workspaceName) }.get
      val STATUS_FOUND = "FOUND"
      val STATUS_NOT_FOUND = "NOT_FOUND"
      val STATUS_CAN_WRITE = "USER_CAN_WRITE"
      val STATUS_CANNOT_WRITE = "USER_CANNOT_WRITE"
      val STATUS_NA = "NOT_AVAILABLE"

      val bucketName = workspace.bucketName
      val rawlsAccessGroupRefs = workspace.accessLevels
      val googleAccessGroupRefs = rawlsAccessGroupRefs map { case (accessLevel, groupRef) =>
        accessLevel -> run {
          _.rawlsGroupQuery.load(groupRef)
        }
      }
      val rawlsIntersectionGroupRefs = workspace.authDomainACLs
      val googleIntersectionGroupRefs = rawlsIntersectionGroupRefs map { case (accessLevel, groupRef) =>
        accessLevel -> run {
          _.rawlsGroupQuery.load(groupRef)
        }
      }

      val userRef = userSubjectId.flatMap(id => run {
        _.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(id)))
      })

      val userStatus = userRef match {
        case Some(user) => "FIRECLOUD_USER: " + user.userSubjectId.value -> STATUS_FOUND
        case None => userSubjectId match {
          case Some(id) => "FIRECLOUD_USER: " + id -> STATUS_NOT_FOUND
          case None => "FIRECLOUD_USER: None Supplied" -> STATUS_NA
        }
      }

      val rawlsAccessGroupStatuses = rawlsAccessGroupRefs map { case (_, groupRef) =>
        run {
          _.rawlsGroupQuery.load(groupRef)
        } match {
          case Some(group) => "WORKSPACE_ACCESS_GROUP: " + group.groupName.value -> STATUS_FOUND
          case None => "WORKSPACE_ACCESS_GROUP: " + groupRef.groupName.value -> STATUS_NOT_FOUND
        }
      }

      val googleAccessGroupStatuses = googleAccessGroupRefs map { case (_, groupRef) =>
        val groupEmail = groupRef.get.groupEmail.value
        toFutureTry(gcsDAO.getGoogleGroup(groupEmail).map(_ match {
          case Some(_) => "GOOGLE_ACCESS_GROUP: " + groupEmail -> STATUS_FOUND
          case None => "GOOGLE_ACCESS_GROUP: " + groupEmail -> STATUS_NOT_FOUND
        }))
      }

      val rawlsIntersectionGroupStatuses = rawlsIntersectionGroupRefs map { case (_, groupRef) =>
        run {
          _.rawlsGroupQuery.load(groupRef)
        } match {
          case Some(group) => "WORKSPACE_INTERSECTION_GROUP: " + group.groupName.value -> STATUS_FOUND
          case None => "WORKSPACE_INTERSECTION_GROUP: " + groupRef.groupName.value -> STATUS_NOT_FOUND
        }
      }

      val googleIntersectionGroupStatuses = googleIntersectionGroupRefs map { case (_, groupRef) =>
        val groupEmail = groupRef.get.groupEmail.value
        toFutureTry(gcsDAO.getGoogleGroup(groupEmail).map(_ match {
          case Some(_) => "GOOGLE_INTERSECTION_GROUP: " + groupEmail -> STATUS_FOUND
          case None => "GOOGLE_INTERSECTION_GROUP: " + groupEmail -> STATUS_NOT_FOUND
        }))
      }

      val bucketStatus = toFutureTry(gcsDAO.getBucket(bucketName).map(_ match {
        case Some(_) => "GOOGLE_BUCKET: " + bucketName -> STATUS_FOUND
        case None => "GOOGLE_BUCKET: " + bucketName -> STATUS_NOT_FOUND
      }))

      val bucketWriteStatus = userStatus match {
        case (_, STATUS_FOUND) => {
          toFutureTry(gcsDAO.diagnosticBucketWrite(userRef.get, bucketName).map(_ match {
            case None => "GOOGLE_BUCKET_WRITE: " + bucketName -> STATUS_CAN_WRITE
            case Some(error) => "GOOGLE_BUCKET_WRITE: " + bucketName -> error.message
          }))
        }
        case (_, _) => Future(Try("GOOGLE_BUCKET_WRITE: " + bucketName -> STATUS_NA))
      }

      val userProxyStatus = userStatus match {
        case (_, STATUS_FOUND) => {
          toFutureTry(gcsDAO.isUserInProxyGroup(userRef.get).map { status =>
            if (status) "FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_FOUND
            else "FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_NOT_FOUND
          })
        }
        case (_, _) => Future(Try("FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_NA))
      }

      val userAccessLevel = userStatus match {
        case (_, STATUS_FOUND) =>
          "WORKSPACE_USER_ACCESS_LEVEL" -> run {
            getMaximumAccessLevel(userRef.get, SlickWorkspaceContext(workspace), _)
          }.toString
        case (_, _) => "WORKSPACE_USER_ACCESS_LEVEL" -> STATUS_NA
      }

      val googleAccessLevel = userStatus match {
        case (_, STATUS_FOUND) => {
          val accessLevel = run {
            getMaximumAccessLevel(userRef.get, SlickWorkspaceContext(workspace), _)
          }
          if (accessLevel >= WorkspaceAccessLevels.Read) {
            val groupEmail = run {
              _.rawlsGroupQuery.load(workspace.accessLevels.get(accessLevel).get)
            }.get.groupEmail.value
            toFutureTry(gcsDAO.isEmailInGoogleGroup(gcsDAO.toProxyFromUser(userRef.get.userSubjectId), groupEmail).map { status =>
              if (status) "GOOGLE_USER_ACCESS_LEVEL: " + groupEmail -> STATUS_FOUND
              else "GOOGLE_USER_ACCESS_LEVEL: " + groupEmail -> STATUS_NOT_FOUND
            })
          }
          else Future(Try("GOOGLE_USER_ACCESS_LEVEL" -> WorkspaceAccessLevels.NoAccess.toString))
        }
        case (_, _) => Future(Try("GOOGLE_USER_ACCESS_LEVEL" -> STATUS_NA))
      }

      Future.sequence(googleAccessGroupStatuses ++ googleIntersectionGroupStatuses ++ Seq(bucketStatus, bucketWriteStatus, userProxyStatus, googleAccessLevel)).map { tries =>
        val statuses = tries.collect { case Success(s) => s }.toSeq
        RequestComplete(WorkspaceStatus(workspaceName, (rawlsAccessGroupStatuses ++ rawlsIntersectionGroupStatuses ++ statuses ++ Seq(userStatus, userAccessLevel)).toMap))
      }
    }
  }

  def getBucketUsage(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    for {
      bucketName <- dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Write, dataAccess) {
            DBIO.successful(workspaceContext.workspace.bucketName)
          }
        }
      }
      usage <- gcsDAO.getBucketUsage(RawlsBillingProjectName(workspaceName.namespace), bucketName)
    } yield {
      RequestComplete(BucketUsageResponse(usage))
    }
  }

  def getAccessInstructions(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        val accessGroups = DBIO.sequence(workspaceContext.workspace.accessLevels.values.map { ref =>
          dataAccess.rawlsGroupQuery.loadGroupIfMember(ref, RawlsUser(userInfo))
        })

        accessGroups.flatMap { memberOf =>
          if (memberOf.flatten.isEmpty) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
          else {
            dataAccess.managedGroupQuery.getManagedGroupAccessInstructions(workspaceContext.workspace.authorizationDomain) map { instructions =>
              RequestComplete(StatusCodes.OK, instructions)
            }
          }
        }
      }
    }
  }
  
  def getGenomicsOperation(workspaceName: WorkspaceName, jobId: String): Future[PerRequestMessage] = {
    // First check the workspace context and permissions in a DB transaction.
    // We don't need any DB information beyond that, so just return Unit on success.
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { _ =>
        DBIO.successful(())
      }
    }
    // Next call GenomicsService, which actually makes the Google Genomics API call.
    .flatMap { _ =>
      val genomicsServiceRef = genomicsServiceConstructor(userInfo)
      genomicsServiceRef.GetOperation(jobId)
    }
  }

  // helper methods

  // note: success is indicated by  Map.empty
  private def attributeNamespaceCheck(attributeNames: Iterable[AttributeName]): Map[String, String] = {
    val namespaces = attributeNames.map(_.namespace).toSet

    // no one can modify attributes with invalid namespaces
    val invalidNamespaces = namespaces -- AttributeName.validNamespaces
    invalidNamespaces.map { ns => ns -> s"Invalid attribute namespace $ns" }.toMap
  }

  private def withAttributeNamespaceCheck[T](attributeNames: Iterable[AttributeName])(op: => T): T = {
    val errors = attributeNamespaceCheck(attributeNames)
    if (errors.isEmpty) op
    else {
      val reasons = errors.values.mkString(", ")
      val err = ErrorReport(statusCode = StatusCodes.Forbidden, message = s"Attribute namespace validation failed: [$reasons]")
      throw new RawlsExceptionWithErrorReport(errorReport = err)
    }
  }

  private def withAttributeNamespaceCheck[T](hasAttributes: Attributable)(op: => Future[T]): Future[T] =
    withAttributeNamespaceCheck(hasAttributes.attributes.keys)(op)

  private def withAttributeNamespaceCheck[T](methodConfiguration: MethodConfiguration)(op: => Future[T]): Future[T] = {
    // TODO: this duplicates expression parsing, the canonical way to do this.  Use that instead?
    // valid method configuration outputs are either in the format this.attrname or workspace.attrname
    // invalid (unparseable) will be caught by expression parsing instead
    val attrNames = methodConfiguration.outputs map { case (_, attr) => AttributeName.fromDelimitedName(attr.value.split('.').last) }
    withAttributeNamespaceCheck(attrNames)(op)
  }

  private def withNewWorkspaceContext[T](workspaceRequest: WorkspaceRequest, dataAccess: DataAccess)
                                     (op: (SlickWorkspaceContext) => ReadWriteAction[T]): ReadWriteAction[T] = {

    def saveNewWorkspace(workspaceId: String, googleWorkspaceInfo: GoogleWorkspaceInfo, workspaceRequest: WorkspaceRequest, dataAccess: DataAccess): ReadWriteAction[Workspace] = {
      val currentDate = DateTime.now

      val accessGroups = googleWorkspaceInfo.accessGroupsByLevel.map { case (a, g) => (a -> RawlsGroup.toRef(g)) }
      val intersectionGroups = googleWorkspaceInfo.intersectionGroupsByLevel map {
        _.map { case (a, g) => (a -> RawlsGroup.toRef(g)) }
      }

      val workspace = Workspace(
        namespace = workspaceRequest.namespace,
        name = workspaceRequest.name,
        authorizationDomain = workspaceRequest.authorizationDomain.getOrElse(Set.empty),
        workspaceId = workspaceId,
        bucketName = googleWorkspaceInfo.bucketName,
        createdDate = currentDate,
        lastModified = currentDate,
        createdBy = userInfo.userEmail.value,
        attributes = workspaceRequest.attributes,
        accessLevels = accessGroups,
        authDomainACLs = intersectionGroups getOrElse accessGroups
      )

      val groupInserts =
        // project owner group should already exsist, don't save it again
        googleWorkspaceInfo.accessGroupsByLevel.filterKeys(_ != ProjectOwner).values.map(dataAccess.rawlsGroupQuery.save) ++
          googleWorkspaceInfo.intersectionGroupsByLevel.map(_.values.map(dataAccess.rawlsGroupQuery.save)).getOrElse(Seq.empty)

      DBIO.seq(groupInserts.toSeq: _*) andThen
        dataAccess.workspaceQuery.save(workspace)
    }


    requireCreateWorkspaceAccess(workspaceRequest, dataAccess) {
      dataAccess.workspaceQuery.findByName(workspaceRequest.toWorkspaceName) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
        case None =>
          val workspaceId = UUID.randomUUID.toString
          for {
            project <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRequest.namespace))

            // we have already verified that the user is in all of the auth domain groups but the project owners might not be
            // so if there is an auth domain we have to do the intersection. There should not be any readers or writers
            // at this point (brand new workspace) so we don't need to do intersections for those
            authDomainProjectOwnerIntersection <- DBIOUtils.maybeDbAction(workspaceRequest.authorizationDomain.flatMap(ad => if (ad.isEmpty) None else Option(ad))) { authDomain =>
              dataAccess.rawlsGroupQuery.intersectGroupMembership(authDomain.map(_.toMembersGroupRef) ++ Set(RawlsGroupRef(RawlsGroupName(dataAccess.policyGroupName(SamResourceTypeNames.billingProject.value, project.get.projectName.value, ProjectRoles.Owner.toString)))))
            }
            projectOwnerGroupEmail <- DBIO.from(samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.get.projectName.value, SamProjectRoles.owner).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email"))))
            projectOwnerGroupOE <- dataAccess.rawlsGroupQuery.loadFromEmail(projectOwnerGroupEmail.value)
            projectOwnerGroup = projectOwnerGroupOE.collect { case Right(g) => g } getOrElse(throw new RawlsException(s"could not find project owner group for email $projectOwnerGroupEmail"))
            googleWorkspaceInfo <- DBIO.from(gcsDAO.setupWorkspace(userInfo, project.get, projectOwnerGroup, workspaceId, workspaceRequest.toWorkspaceName, workspaceRequest.authorizationDomain.getOrElse(Set.empty), authDomainProjectOwnerIntersection))

            savedWorkspace <- saveNewWorkspace(workspaceId, googleWorkspaceInfo, workspaceRequest, dataAccess)
            response <- op(SlickWorkspaceContext(savedWorkspace))
          } yield response
      }
    }
  }

  private def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  private def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"

  private def requireCreateWorkspaceAccess[T](workspaceRequest: WorkspaceRequest, dataAccess: DataAccess)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    for {
      authDomainAccesses <- DBIO.from(samDAO.listManagedGroups(userInfo))
      userHasAction <- DBIO.from(samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamResourceActions.createWorkspace, userInfo))
      response <- userHasAction match {
        case true =>
          dataAccess.rawlsBillingProjectQuery.load(projectName).flatMap {
            case Some(RawlsBillingProject(_, _, CreationStatuses.Ready, _, _)) =>
              if(workspaceRequest.authorizationDomain.getOrElse(Set.empty).subsetOf(authDomainAccesses.map(g => ManagedGroupRef(g.groupName)).toSet)) op
              else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"In order to use a group in an Authorization Domain, you must be a member of that group.")))

            case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, CreationStatuses.Creating, _, _)) =>
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"${name} is still being created")))

            case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, CreationStatuses.Error, _, messageOp)) =>
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Error creating ${name}: ${messageOp.getOrElse("no message")}")))

            case None =>
              // this can't happen with the current code but a 404 would be the correct response
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${workspaceRequest.toWorkspaceName.namespace} does not exist")))
          }
        case false =>
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}")))
      }
    } yield response
  }

  private def withWorkspaceContextAndPermissions[T](workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[T]): ReadWriteAction[T] = {
    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      requireAccess(workspaceContext.workspace, accessLevel, dataAccess) { op(workspaceContext) }
    }
  }

  private def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[T]) = {
    dataAccess.workspaceQuery.findByName(workspaceName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }

  private def requireAccess[T](workspace: Workspace, requiredLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(codeBlock: => ReadWriteAction[T]): ReadWriteAction[T] = {
    getMaximumAccessLevel(RawlsUser(userInfo), SlickWorkspaceContext(workspace), dataAccess) flatMap { userLevel =>
      if (userLevel >= requiredLevel) {
        if ( (requiredLevel > WorkspaceAccessLevels.Read) && workspace.isLocked )
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"The workspace ${workspace.toWorkspaceName} is locked.")))
        else codeBlock
      }
      else if (userLevel >= WorkspaceAccessLevels.Read) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
      else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
    }
  }

  private def requireAccessIgnoreLock[T](workspace: Workspace, requiredLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    requireAccess(workspace.copy(isLocked = false), requiredLevel, dataAccess)(op)
  }

  private def requireSharePermission[T](workspace: Workspace, dataAccess: DataAccess)(codeBlock: (WorkspaceAccessLevel) => ReadWriteAction[T]): ReadWriteAction[T] = {
    getMaximumAccessLevel(RawlsUser(userInfo), SlickWorkspaceContext(workspace), dataAccess) flatMap { userLevel =>
      if (userLevel >= WorkspaceAccessLevels.Owner) codeBlock(userLevel)
      else dataAccess.workspaceQuery.getUserSharePermissions(userInfo.userSubjectId, SlickWorkspaceContext(workspace)) flatMap { canShare =>
        if (canShare) codeBlock(userLevel)
        else if (userLevel >= WorkspaceAccessLevels.Read) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
        else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
      }
    }
  }

  private def requireComputePermission[T](workspace: Workspace, dataAccess: DataAccess)(codeBlock: => ReadWriteAction[T]): ReadWriteAction[T] = {
    DBIO.from(samDAO.userHasAction(SamResourceTypeNames.billingProject, workspace.namespace, SamResourceActions.launchBatchCompute, userInfo)) flatMap { projectCanCompute =>
      if (!projectCanCompute) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
      else {
        getMaximumAccessLevel(RawlsUser(userInfo), SlickWorkspaceContext(workspace), dataAccess) flatMap { userLevel =>
          if (userLevel >= WorkspaceAccessLevels.Owner) codeBlock
          else dataAccess.workspaceQuery.getUserComputePermissions(userInfo.userSubjectId, SlickWorkspaceContext(workspace)) flatMap { canCompute =>
            if (canCompute) codeBlock
            else if (userLevel >= WorkspaceAccessLevels.Read) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
            else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
          }
        }
      }
    }
  }

  def getUserSharePermissions(workspaceContext: SlickWorkspaceContext, userAccessLevel: WorkspaceAccessLevel, dataAccess: DataAccess): ReadWriteAction[Boolean] = {
    if (userAccessLevel >= WorkspaceAccessLevels.Owner) DBIO.successful(true)
    else dataAccess.workspaceQuery.getUserSharePermissions(userInfo.userSubjectId, workspaceContext)
  }

  def getUserComputePermissions(workspaceContext: SlickWorkspaceContext, userAccessLevel: WorkspaceAccessLevel, dataAccess: DataAccess): ReadWriteAction[Boolean] = {
    if (userAccessLevel >= WorkspaceAccessLevels.Owner) DBIO.successful(true)
    else dataAccess.workspaceQuery.getUserComputePermissions(userInfo.userSubjectId, workspaceContext)
  }

  def getUserCatalogPermissions(workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadWriteAction[Boolean] = {
    dataAccess.workspaceQuery.getUserCatalogPermissions(userInfo.userSubjectId, workspaceContext)
  }

  private def withEntity[T](workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, dataAccess: DataAccess)(op: (Entity) => ReadWriteAction[T]): ReadWriteAction[T] = {
    dataAccess.entityQuery.get(workspaceContext, entityType, entityName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in ${workspaceContext.workspace.toWorkspaceName}")))
      case Some(entity) => op(entity)
    }
  }

  private def withAllEntities[T](workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess, entities: Seq[AttributeEntityReference])(op: (Seq[Entity]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    val entityActions: Seq[ReadAction[Try[Entity]]] = entities map { e =>
      dataAccess.entityQuery.get(workspaceContext, e.entityType, e.entityName) map {
        case None => Failure(new RawlsException(s"${e.entityType} ${e.entityName} does not exist in ${workspaceContext.workspace.toWorkspaceName}"))
        case Some(entity) => Success(entity)
      }
    }

    DBIO.sequence(entityActions) flatMap { entityTries =>
      val failures = entityTries.collect { case Failure(y) => y.getMessage }
      if (failures.isEmpty) op(entityTries collect { case Success(e) => e })
      else {
        val err = ErrorReport(statusCode = StatusCodes.BadRequest, message = (Seq("Entities were not found:") ++ failures) mkString System.lineSeparator())
        DBIO.failed(new RawlsExceptionWithErrorReport(err))
      }
    }
  }

  private def withSubmission[T](workspaceContext: SlickWorkspaceContext, submissionId: String, dataAccess: DataAccess)(op: (Submission) => ReadWriteAction[T]): ReadWriteAction[T] = {
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(t: IllegalArgumentException) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")))
      case _ =>
        dataAccess.submissionQuery.get(workspaceContext, submissionId) flatMap {
          case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext.workspace.toWorkspaceName}")))
          case Some(submission) => op(submission)
        }
    }
  }

  // confirm that the Submission is a member of this workspace, but don't unmarshal it from the DB
  private def withSubmissionId[T](workspaceContext: SlickWorkspaceContext, submissionId: String, dataAccess: DataAccess)(op: UUID => ReadWriteAction[T]): ReadWriteAction[T] = {
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(t: IllegalArgumentException) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")))
      case Success(uuid) =>
        dataAccess.submissionQuery.confirmInWorkspace(workspaceContext.workspaceId, uuid) flatMap {
          case None =>
            val report = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext.workspace.toWorkspaceName}")
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = report))
          case Some(_) => op(uuid)
        }
    }
  }

  private def withWorkflow(workspaceName: WorkspaceName, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: (Workflow) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.workflowQuery.getByExternalId(workflowId, submissionId) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Workflow with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName}")))
      case Some(workflow) => op(workflow)
    }
  }

  private def withWorkflowRecord(workspaceName: WorkspaceName, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: (WorkflowRecord) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowId, UUID.fromString(submissionId)).result flatMap {
      case Seq() => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"WorkflowRecord with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName}")))
      case Seq(one) => op(one)
      case tooMany => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"found multiple WorkflowRecords with id ${workflowId} in submission ${submissionId} in workspace ${workspaceName}")))
    }
  }

  // used as part of the workflow metadata permission check - more detail at workflowMetadata()

  // require submission to be present, but don't require the workflow to reference it
  // if the workflow does reference the submission, return its executionServiceKey

  private def withSubmissionAndWorkflowExecutionServiceKey[T](workspaceContext: SlickWorkspaceContext, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: Option[String] => ReadWriteAction[T]): ReadWriteAction[T] = {
    withSubmissionId(workspaceContext, submissionId, dataAccess) { _ =>
      dataAccess.workflowQuery.getExecutionServiceIdByExternalId(workflowId, submissionId) flatMap op
    }
  }

  //Finds a single entity record in the db.
  private def withSingleEntityRec(entityType: String, entityName: String, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess)(op: (Seq[EntityRecord]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    val entityRec = dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceId, entityType, entityName).result
    entityRec flatMap { entities =>
      if (entities.isEmpty) {
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"No entity of type ${entityType} named ${entityName} exists in this workspace.")))
      } else if (entities.size == 1) {
        op(entities)
      } else {
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"More than one entity of type ${entityType} named ${entityName} exists in this workspace?!")))
      }
    }
  }

  def withSubmissionEntityRecs(submissionRequest: SubmissionRequest, workspaceContext: SlickWorkspaceContext, rootEntityTypeOpt: Option[String], dataAccess: DataAccess)(op: (Option[Seq[EntityRecord]]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    if( rootEntityTypeOpt.isEmpty ) {
      op(None)
    } else {
      val rootEntityType = rootEntityTypeOpt.get

      //If there's an expression, evaluate it to get the list of entities to run this job on.
      //Otherwise, use the entity given in the submission.
      submissionRequest.expression match {
        case None =>
          if (submissionRequest.entityType.getOrElse("") != rootEntityType) {
            val whatYouGaveUs = if (submissionRequest.entityType.isDefined) s"an entity of type ${submissionRequest.entityType.get}" else "no entity"
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Method configuration expects an entity of type $rootEntityType, but you gave us $whatYouGaveUs.")))
          } else {
            withSingleEntityRec(submissionRequest.entityType.get, submissionRequest.entityName.get, workspaceContext, dataAccess)(rec => op(Some(rec)))
          }
        case Some(expression) =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, workspaceContext, submissionRequest.entityType.get, submissionRequest.entityName.get) { evaluator =>
            evaluator.evalFinalEntity(workspaceContext, expression).asTry
          } flatMap { //gotta close out the expression evaluator to wipe the EXPREVAL_TEMP table
            case Failure(regret) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret)))
            case Success(entityRecords) =>
              if (entityRecords.isEmpty) {
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "No entities eligible for submission were found.")))
              } else {
                val eligibleEntities = entityRecords.filter(_.entityType == rootEntityType).toSeq
                if (eligibleEntities.isEmpty)
                  DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type ${rootEntityType}.)")))
                else
                  op(Some(eligibleEntities))
              }
          }
      }
    }
  }

  /** Validates the workflow failure mode in the submission request. */
  private def withWorkflowFailureMode(submissionRequest: SubmissionRequest)(op: Option[WorkflowFailureMode] => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    Try(submissionRequest.workflowFailureMode.map(WorkflowFailureModes.withName)) match {
      case Success(failureMode) => op(failureMode)
      case Failure(NonFatal(e)) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, e.getMessage)))
    }
  }


  private def withSubmissionParameters(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest)
    (op: (DataAccess, SlickWorkspaceContext, String, SubmissionValidationHeader, Seq[SubmissionValidationEntityInputs], Seq[SubmissionValidationEntityInputs], Option[WorkflowFailureMode]) => ReadWriteAction[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, submissionRequest.methodConfigurationNamespace, submissionRequest.methodConfigurationName, dataAccess) { methodConfig =>
          withMethodInputs(methodConfig, userInfo) { (wdl, inputsToProcess, emptyOptionalInputs) =>
            //either both entityName and entityType must be defined, or neither. Error otherwise
            if(submissionRequest.entityName.isDefined != submissionRequest.entityType.isDefined) {
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"You must set both entityType and entityName to run on an entity, or neither (to run with literal or workspace inputs)."))
            }
            if(methodConfig.rootEntityType.isDefined != submissionRequest.entityName.isDefined) {
              if(methodConfig.rootEntityType.isDefined) {
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Your method config defines a root entity but you haven't passed one to the submission."))
              } else {
                //This isn't _strictly_ necessary, since a single submission entity will create one workflow.
                //However, passing in a submission entity + an expression doesn't make sense for two reasons:
                // 1. you'd have to write an expression from your submission entity to an entity of "no entity necessary" type
                // 2. even if you _could_ do this, you'd kick off a bunch of identical workflows.
                //More likely than not, an MC with no root entity + a submission entity = you're doing something wrong. So we'll just say no here.
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Your method config uses no root entity, but you passed one to the submission."))
              }
            }
            withValidatedMCExpressions(methodConfig, inputsToProcess, emptyOptionalInputs, allowRootEntity = submissionRequest.entityName.isDefined, dataAccess) { _ =>
              withSubmissionEntityRecs(submissionRequest, workspaceContext, methodConfig.rootEntityType, dataAccess) { jobEntityRecs =>
                withWorkflowFailureMode(submissionRequest) { workflowFailureMode =>
                  //Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
                  MethodConfigResolver.evaluateInputExpressions(workspaceContext, inputsToProcess, jobEntityRecs, dataAccess) flatMap { valuesByEntity =>
                    valuesByEntity
                      .map({ case (entityName, values) => SubmissionValidationEntityInputs(entityName, values) })
                      .partition({ entityInputs => entityInputs.inputResolutions.forall(_.error.isEmpty) }) match {
                      case (succeeded, failed) =>
                        val methodConfigInputs = inputsToProcess.map { methodInput => SubmissionValidationInput(methodInput.workflowInput.localName.value, methodInput.expression) }
                        val header = SubmissionValidationHeader(methodConfig.rootEntityType, methodConfigInputs)
                        op(dataAccess, workspaceContext, wdl, header, succeeded.toSeq, failed.toSeq, workflowFailureMode)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
