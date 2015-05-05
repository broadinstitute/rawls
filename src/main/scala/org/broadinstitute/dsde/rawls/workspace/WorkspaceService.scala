package org.broadinstitute.dsde.rawls.workspace

import akka.actor.{Props, Actor}
import org.broadinstitute.dsde.rawls.dataaccess.{WorkspaceDAO}
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import spray.http.{Uri, HttpHeaders, StatusCodes}
import org.joda.time.DateTime
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class SaveWorkspace(workspace: Workspace, rootUri: Uri) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case class CloneWorkspace(sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String, rootUri: Uri) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(workspaceServiceConstructor())
  }

  def constructor(workspaceDAO: WorkspaceDAO) = () => new WorkspaceService(workspaceDAO)
}

class WorkspaceService(workspaceDAO: WorkspaceDAO) extends Actor {
  override def receive = {
    case SaveWorkspace(workspace, rootUri) => saveWorkspace(workspace, rootUri)
    case ListWorkspaces => listWorkspaces()
    case CloneWorkspace(sourceNamespace, sourceWorkspace, destNamespace, destWorkspace, rootUri) => cloneWorkspace(sourceNamespace, sourceWorkspace, destNamespace, destWorkspace, rootUri)
  }

  def saveWorkspace(workspace: Workspace, rootUri: Uri): Unit = {
//    workspaceDAO.load(workspace.namespace, workspace.name) match {
//      case Some(_) => context.parent ! PerRequest.RequestComplete(StatusCodes.Conflict, s"Workspace ${workspace.namespace}/${workspace.name} already exists")
//      case None =>
//        workspaceDAO.save(workspace)
//        context.parent ! PerRequest.RequestCompleteWithHeaders((StatusCodes.Created, workspace), HttpHeaders.Location(rootUri.copy(path = Uri.Path(s"/workspaces/${workspace.namespace}/${workspace.name}"))))
//    }

    workspaceDAO.save(workspace)
    context.parent ! PerRequest.RequestCompleteWithHeaders((StatusCodes.Created, workspace), HttpHeaders.Location(rootUri.copy(path = Uri.Path(s"/workspaces/${workspace.namespace}/${workspace.name}"))))
  }

  def listWorkspaces() = {
    context.parent ! PerRequest.RequestComplete(workspaceDAO.list())
  }

  def cloneWorkspace(sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String, rootUri: Uri): Unit = {
    val originalWorkspace = workspaceDAO.load(sourceNamespace, sourceWorkspace)
    val copyWorkspace = workspaceDAO.load(destNamespace, destWorkspace)
    (originalWorkspace, copyWorkspace) match {
      case ( Some(ws), None ) => {
        val newWorkspace = ws.copy(namespace = destNamespace, name = destWorkspace, createdDate = DateTime.now)
        workspaceDAO.save( newWorkspace )
        context.parent ! PerRequest.RequestCompleteWithHeaders((StatusCodes.Created, newWorkspace), HttpHeaders.Location(rootUri.copy(path = Uri.Path(s"/workspaces/${destNamespace}/${destWorkspace}"))))
      }
      case ( None, _ ) => context.parent ! PerRequest.RequestComplete(StatusCodes.NotFound, "Source workspace " + sourceNamespace + "/" + sourceWorkspace + " not found")
      case ( _, Some(_) ) => context.parent ! PerRequest.RequestComplete(StatusCodes.Conflict, "Destination workspace " + destNamespace + "/" + destWorkspace + " already exists")
    }
  }
}
