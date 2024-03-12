package org.broadinstitute.dsde.rawls.serviceFactory

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, UserInfo}

import scala.concurrent.ExecutionContext.Implicits.global

object EnableServiceAccountFactory extends LazyLogging {
  def createEnableServiceAccount(appConfigManager: RawlsConfigManager,
                                 gcsDAO: GoogleServicesDAO,
                                 samDAO: SamDAO
  ): Unit =
    appConfigManager.cloudProvider match {
      case Gcp =>
        enableServiceAccount(gcsDAO, samDAO)
      case _ =>
    }
  private def enableServiceAccount(httpGoogleServicesDAO: GoogleServicesDAO, samDAO: SamDAO): Unit = {
    val credential = httpGoogleServicesDAO.getBucketServiceAccountCredential
    val serviceAccountUserInfo = UserInfo.buildFromTokens(credential)
    val registerServiceAccountFuture = samDAO.registerUser(RawlsRequestContext(serviceAccountUserInfo))
    registerServiceAccountFuture.failed.foreach {
      // this is logged as a warning because almost always the service account is already enabled
      // so this is a problem only the first time rawls is started with a new service account
      t: Throwable => logger.warn("error enabling service account", t)
    }
  }
}
