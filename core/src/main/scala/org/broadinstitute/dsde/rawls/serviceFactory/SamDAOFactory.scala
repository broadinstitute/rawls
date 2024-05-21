package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.credentials.RawlsCredential
import org.broadinstitute.dsde.rawls.dataaccess.{HttpSamDAO, SamDAO}
import org.broadinstitute.dsde.rawls.util.toScalaDuration

import scala.concurrent.ExecutionContext

object SamDAOFactory {
  def createSamDAO(appConfigManager: RawlsConfigManager)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): SamDAO = {
    val samConfig = appConfigManager.conf.getConfig("sam")
    new HttpSamDAO(
      samConfig.getString("server"),
      RawlsCredential.getCredential(appConfigManager),
      toScalaDuration(samConfig.getDuration("timeout"))
    )
  }
}
