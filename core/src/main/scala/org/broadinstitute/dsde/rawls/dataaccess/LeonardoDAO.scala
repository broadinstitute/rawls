package org.broadinstitute.dsde.rawls.dataaccess
import org.broadinstitute.dsde.workbench.client.leonardo.model.ListAppResponse
import java.util.UUID
import java.util.List

trait LeonardoDAO {

  def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID]): Unit

  def createApp(token: String,
                workspaceId: UUID,
                appName: String,
                appType: String,
                sourceWorkspaceId: Option[UUID]
  ): Unit

  def listAppsV2(token: String, workspaceId: UUID): List[ListAppResponse]

  def deleteAppV2(token: String, workspaceId: UUID, appName: String): Unit

}
