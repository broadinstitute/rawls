package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.genomics.{DisabledGenomicsService, GenomicsService, GenomicsServiceRequest}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.workbench.dataaccess.PubSubNotificationDAO
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

import scala.concurrent.ExecutionContext

object MultiCloudGenomicsServiceFactory {
  def createMultiCloudGenomicsService(dataSource: SlickDataSource,
                                      gcsDAO: GoogleServicesDAO,
                                      cloudProvider: String
                                     )(implicit executionContext: ExecutionContext): RawlsRequestContext => GenomicsServiceRequest  = {
    cloudProvider match {
      case "gcp" =>
          GenomicsService.constructor(dataSource, gcsDAO)
      case "azure" =>
          DisabledGenomicsService.constructor()(_)
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
