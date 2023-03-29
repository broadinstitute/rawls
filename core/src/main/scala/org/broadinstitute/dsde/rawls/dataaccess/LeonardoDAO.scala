package org.broadinstitute.dsde.rawls.dataaccess
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import scala.concurrent.Future
import org.broadinstitute.dsde.rawls.model.WorkspaceName


trait LeonardoDAO {

  def getAppsV2leonardoApi(accessToken: String): AppsV2Api

  def createWDSInstance(token: String, workspaceId: String, appName: String, appType: String): Unit

}
