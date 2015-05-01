package org.broadinstitute.dsde.rawls.workspace

import akka.actor.{Props, Actor}
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceDAO
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.broadinstitute.dsde.rawls.ws.PerRequest
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class SaveWorkspace(workspace: Workspace) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(workspaceServiceConstructor())
  }

  def constructor(workspaceDAO: WorkspaceDAO) = () => new WorkspaceService(workspaceDAO)
}

class WorkspaceService(workspaceDAO: WorkspaceDAO) extends Actor {
  override def receive = {
    case SaveWorkspace(workspace) => saveWorkspace(workspace)
    case ListWorkspaces => listWorkspaces()
  }

  def saveWorkspace(workspace: Workspace): Unit = {
    workspaceDAO.save(workspace)
    context.parent ! PerRequest.RequestCompleteNoContent(StatusCodes.Created)
  }

  def listWorkspaces() = {
    context.parent ! PerRequest.RequestCompleteOK(workspaceDAO.list())
  }
}
