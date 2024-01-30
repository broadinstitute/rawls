package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{AzureHttpSamDAO, GoogleServicesDAO, HttpGoogleServicesDAO, HttpSamDAO, SamDAO}
import org.broadinstitute.dsde.rawls.util.toScalaDuration

import scala.concurrent.ExecutionContext

object MultiCloudSamDAOFactory {
  def createMultiCloudSamDAO(config: Config,
                             gcsDAO: GoogleServicesDAO,
                             cloudProvider: String
                                     )(implicit system: ActorSystem, executionContext: ExecutionContext): SamDAO = {
    val samConfig = config.getConfig("sam")
    cloudProvider match {
      case "gcp" =>
        new HttpSamDAO(
          samConfig.getString("server"),
          gcsDAO.getBucketServiceAccountCredential,
          toScalaDuration(samConfig.getDuration("timeout"))
        )
      case "azure" =>
        new AzureHttpSamDAO(
          samConfig.getString("server"),
          toScalaDuration(samConfig.getDuration("timeout"))
        )
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
