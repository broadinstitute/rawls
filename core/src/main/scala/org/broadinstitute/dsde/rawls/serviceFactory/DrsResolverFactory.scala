package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.drs.{DrsHubResolver, DrsResolver}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object DrsResolverFactory {
  def createDrsResolver(
    appConfigManager: RawlsConfigManager
  )(implicit system: ActorSystem, executionContext: ExecutionContext): DrsResolver =
    appConfigManager.cloudProvider match {
      case Gcp =>
        val drsHubBaseUrl: String = appConfigManager.conf.getString("drshub.baseUrl")
        val drsHubUrl: String = s"$drsHubBaseUrl/api/v4/drs/resolve"
        new DrsHubResolver(drsHubUrl)
      case Azure =>
        newDisabledService[DrsResolver]
    }
}
