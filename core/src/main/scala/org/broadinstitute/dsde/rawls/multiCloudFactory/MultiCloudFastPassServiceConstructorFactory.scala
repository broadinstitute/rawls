package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.IO
import org.broadinstitute.dsde.rawls.AppDependencies
import org.broadinstitute.dsde.rawls.config.{FastPassConfig, MultiCloudAppConfigManager}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.fastpass.{FastPass, FastPassService}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}

object MultiCloudFastPassServiceConstructorFactory {
  def createCloudFastPassService(appConfigManager: MultiCloudAppConfigManager,
                                 appDependencies: AppDependencies[IO],
                                 gcsDAO: GoogleServicesDAO,
                                 samDAO: SamDAO
  )(implicit
    executionContext: ExecutionContext
  ): (RawlsRequestContext, SlickDataSource) => FastPass =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val fastPassConfig = FastPassConfig.apply(appConfigManager.conf)
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
      case None =>
        (_, _) => newDisabledService[FastPass]
    }
}
