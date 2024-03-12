package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, HttpSamDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.util.toScalaDuration

import scala.concurrent.ExecutionContext

object SamDAOFactory {
  def createSamDAO(appConfigManager: RawlsConfigManager, gcsDAO: GoogleServicesDAO)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): SamDAO = {
    val samConfig = appConfigManager.conf.getConfig("sam")
    appConfigManager.cloudProvider match {
      case Gcp =>
        new HttpSamDAO(
          samConfig.getString("server"),
          Option(gcsDAO.getBucketServiceAccountCredential),
          toScalaDuration(samConfig.getDuration("timeout"))
        )
      case Azure =>
        new HttpSamDAO(
          samConfig.getString("server"),
          None,
          toScalaDuration(samConfig.getDuration("timeout"))
        )
    }
  }
}
