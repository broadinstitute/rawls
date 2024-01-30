package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, UserInfo}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global

class MultiCloudEnableServiceAccountFactory extends LazyLogging{
  def createEnableServiceAccount(gcsDAO: GoogleServicesDAO, samDAO: SamDAO, cloudProvider: String): Unit = {
    cloudProvider match {
      case "gcp" =>
        enableServiceAccount(gcsDAO, samDAO)
      case "azure" =>
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
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
