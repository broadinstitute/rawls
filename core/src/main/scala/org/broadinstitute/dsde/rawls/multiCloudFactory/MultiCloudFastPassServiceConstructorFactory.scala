package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.IO
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.AppDependencies
import org.broadinstitute.dsde.rawls.config.{FastPassConfig, MethodRepoConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledHttpMethodRepoDAO, GoogleServicesDAO, HttpMethodRepoDAO, MethodRepoDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.fastpass.{DisabledFastPassService, FastPass, FastPassService}
import org.broadinstitute.dsde.rawls.model.{Agora, Dockstore, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}



object MultiCloudFastPassServiceConstructorFactory {
  def createCloudFastPassService(config: Config,
                                 appDependencies: AppDependencies[IO],
                                 gcsDAO: GoogleServicesDAO,
                                 samDAO: SamDAO,
                                 gcsConfig: Config,
                                 cloudProvider: String
                                )(implicit
                                  executionContext: ExecutionContext,
                                  openTelemetry: OpenTelemetryMetrics[IO]
                                ): (RawlsRequestContext, SlickDataSource) => FastPass
  = cloudProvider match {
      case "gcp" =>
        val fastPassConfig = FastPassConfig.apply(config)
        FastPassService.constructor(
          fastPassConfig,
          appDependencies.httpGoogleIamDAO,
          appDependencies.httpGoogleStorageDAO,
          gcsDAO,
          samDAO,
          terraBillingProjectOwnerRole = gcsConfig.getString("terraBillingProjectOwnerRole"),
          terraWorkspaceCanComputeRole = gcsConfig.getString("terraWorkspaceCanComputeRole"),
          terraWorkspaceNextflowRole = gcsConfig.getString("terraWorkspaceNextflowRole"),
          terraBucketReaderRole = gcsConfig.getString("terraBucketReaderRole"),
          terraBucketWriterRole = gcsConfig.getString("terraBucketWriterRole")
        )
      case "azure" =>
        DisabledFastPassService.constructor()
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
