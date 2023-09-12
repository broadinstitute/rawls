package org.broadinstitute.dsde.rawls.dataaccess
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsApi

import scala.concurrent.Future
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

import java.util.UUID

trait LeonardoDAO {

  def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID]): Unit

  def createApp(token: String,
                workspaceId: UUID,
                appName: String,
                appType: String,
                sourceWorkspaceId: Option[UUID]
               ): Unit

  def deleteApps(token: String, workspaceId: String, deleteDisk: Boolean)

  def listApps(token: String, workspaceId: String): Seq[ListAppResponse]

  def listAzureRuntimes(token: String, workspaceId: String): Seq[ListRuntimeResponse]

  def deleteAzureRuntimes(token: String, workspaceId: String, deleteDisk: Boolean)


}


