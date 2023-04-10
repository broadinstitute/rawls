package org.broadinstitute.dsde.rawls.dataaccess
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import scala.concurrent.Future
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import java.util.UUID

trait LeonardoDAO {

  def createWDSInstance(token: String, workspaceId: UUID): Unit

  def createApp(token: String, workspaceId: UUID, appName: String, appType: String): Unit

}