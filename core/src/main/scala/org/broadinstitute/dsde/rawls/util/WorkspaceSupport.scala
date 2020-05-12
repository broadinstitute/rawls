package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddListMember, AddUpdateAttribute, AttributeUpdateOperation, CreateAttributeEntityReferenceList, CreateAttributeValueList, RemoveAttribute, RemoveListMember}
import org.broadinstitute.dsde.rawls.model.{Attributable, AttributeEntityReference, AttributeEntityReferenceEmptyList, AttributeEntityReferenceList, AttributeName, AttributeNull, AttributeValue, AttributeValueEmptyList, AttributeValueList, CreationStatuses, Entity, ErrorReport, MethodConfiguration, RawlsBillingProject, RawlsBillingProjectName, SamBillingProjectActions, SamResourceAction, SamResourceTypeNames, SamWorkspaceActions, UserInfo, Workspace, WorkspaceAttributeSpecs, WorkspaceName, WorkspaceRequest}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import org.broadinstitute.dsde.rawls.workspace.{AttributeNotFoundException, AttributeUpdateOperationException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait WorkspaceSupport {
  val samDAO: SamDAO
  protected val userInfo: UserInfo
  implicit protected val executionContext: ExecutionContext
  protected val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  //Access/permission helpers

  def accessCheck(workspace: Workspace, requiredAction: SamResourceAction, ignoreLock: Boolean): Future[Unit] = {
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, requiredAction, userInfo) flatMap { hasRequiredLevel =>
      if (hasRequiredLevel) {
        if (Set(SamWorkspaceActions.write, SamWorkspaceActions.compute).contains(requiredAction) && workspace.isLocked && !ignoreLock)
          Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"The workspace ${workspace.toWorkspaceName} is locked.")))
        else
          Future.successful(())
      } else {
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, userInfo) flatMap { canRead =>
          if (canRead) {
            Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
          }
          else {
            Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
          }
        }
      }
    }
  }

  def requireAccessF[T](workspace: Workspace, requiredAction: SamResourceAction)(codeBlock: => Future[T]): Future[T] = {
    accessCheck(workspace, requiredAction, ignoreLock = false) flatMap { _ => codeBlock }
  }

  def requireAccessIgnoreLockF[T](workspace: Workspace, requiredAction: SamResourceAction)(codeBlock: => Future[T]): Future[T] = {
    accessCheck(workspace, requiredAction, ignoreLock = true) flatMap { _ => codeBlock }
  }

  def requireComputePermission(workspaceName: WorkspaceName): Future[Unit] = {
    for {
      workspaceContext <- getWorkspaceContext(workspaceName)
      hasCompute <- {
        samDAO.userHasAction(SamResourceTypeNames.billingProject, workspaceName.namespace, SamBillingProjectActions.launchBatchCompute, userInfo).flatMap { projectCanCompute =>
          if (!projectCanCompute) Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspaceName))))
          else {
            samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceContext.workspace.workspaceId, SamWorkspaceActions.compute, userInfo).flatMap { launchBatchCompute =>
              if (launchBatchCompute) Future.successful(())
              else samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceContext.workspace.workspaceId, SamWorkspaceActions.read, userInfo).flatMap { workspaceRead =>
                if (workspaceRead) Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspaceName))))
                else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
              }
            }
          }
        }
      }
    } yield {
      hasCompute
    }
  }

  // TODO: find and assess all usages. This is written to reside inside a DB transaction, but it makes a REST call to Sam.
  def requireCreateWorkspaceAccess[T](workspaceRequest: WorkspaceRequest, dataAccess: DataAccess, parentSpan: Span = null)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    for {
      userHasAction <- traceDBIOWithParent("userHasAction", parentSpan)(_ => DBIO.from(samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectActions.createWorkspace, userInfo)))
      response <- userHasAction match {
        case true =>
          traceDBIOWithParent("loadBillingProject", parentSpan)( _ => dataAccess.rawlsBillingProjectQuery.load(projectName)).flatMap {
            case Some(RawlsBillingProject(_, _, CreationStatuses.Ready, _, _, _, _, _)) => op //Sam will check to make sure the Auth Domain selection is valid
            case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, CreationStatuses.Creating, _, _, _, _, _)) =>
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"${name} is still being created")))

            case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, CreationStatuses.Error, _, messageOp, _, _, _)) =>
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Error creating ${name}: ${messageOp.getOrElse("no message")}")))
            case Some(_) | None =>
              // this can't happen with the current code but a 404 would be the correct response
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${workspaceRequest.toWorkspaceName.namespace} does not exist")))
          }
        case false =>
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}")))
      }
    } yield response
  }

  //WorkspaceContext helpers

  // function name may be misleading. This returns the workspace context and checks the user's permission,
  // but does not return the permissions.
  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName, requiredAction: SamResourceAction, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): Future[SlickWorkspaceContext] = {
    for {
      workspaceContext <- getWorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspaceContext.workspace, requiredAction, ignoreLock = false) // throws if user does not have permission
    } yield workspaceContext
  }

  def getWorkspaceContext(workspaceName: WorkspaceName, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): Future[SlickWorkspaceContext] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess, attributeSpecs) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess, attributeSpecs: Option[WorkspaceAttributeSpecs] = None)(op: (SlickWorkspaceContext) => ReadWriteAction[T]) = {
    dataAccess.workspaceQuery.findByName(workspaceName, attributeSpecs) flatMap {
      case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName)))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }

  def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"

  // note: success is indicated by  Map.empty
  def attributeNamespaceCheck(attributeNames: Iterable[AttributeName]): Map[String, String] = {
    val namespaces = attributeNames.map(_.namespace).toSet

    // no one can modify attributes with invalid namespaces
    val invalidNamespaces = namespaces -- AttributeName.validNamespaces
    invalidNamespaces.map { ns => ns -> s"Invalid attribute namespace $ns" }.toMap
  }

  def withAttributeNamespaceCheck[T](attributeNames: Iterable[AttributeName])(op: => T): T = {
    val errors = attributeNamespaceCheck(attributeNames)
    if (errors.isEmpty) op
    else {
      val reasons = errors.values.mkString(", ")
      val err = ErrorReport(statusCode = StatusCodes.Forbidden, message = s"Attribute namespace validation failed: [$reasons]")
      throw new RawlsExceptionWithErrorReport(errorReport = err)
    }
  }

  def withAttributeNamespaceCheck[T](hasAttributes: Attributable)(op: => Future[T]): Future[T] =
    withAttributeNamespaceCheck(hasAttributes.attributes.keys)(op)

  def withAttributeNamespaceCheck[T](methodConfiguration: MethodConfiguration)(op: => Future[T]): Future[T] = {
    // TODO: this duplicates expression parsing, the canonical way to do this.  Use that instead?
    // valid method configuration outputs are either in the format this.attrname or workspace.attrname
    // invalid (unparseable) will be caught by expression parsing instead
    val attrNames = methodConfiguration.outputs map { case (_, attr) => AttributeName.fromDelimitedName(attr.value.split('.').last) }
    withAttributeNamespaceCheck(attrNames)(op)
  }

  // can't use withClonedAuthDomain because the Auth Domain -> no Auth Domain logic is different
  def authDomainCheck(sourceWorkspaceADs: Set[String], destWorkspaceADs: Set[String]): ReadWriteAction[Boolean] = {
    // if the source has any auth domains, the dest must also *at least* have those auth domains
    if(sourceWorkspaceADs.subsetOf(destWorkspaceADs)) DBIO.successful(true)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg = s"Source workspace has an Authorization Domain containing the groups ${missingGroups.mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }
  }

  def applyAttributeUpdateOperations(attributable: Attributable, operations: Seq[AttributeUpdateOperation]): AttributeMap = {
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

  //Finds a single entity record in the db.
  def withSingleEntityRec(entityType: String, entityName: String, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess)(op: (Seq[EntityRecord]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
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

  def withAllEntities[T](workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess, entities: Seq[AttributeEntityReference])(op: (Seq[Entity]) => ReadWriteAction[T]): ReadWriteAction[T] = {
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

}
