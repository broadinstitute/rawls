package org.broadinstitute.dsde.rawls.entities.datarepo

import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

class DataRepoEntityProviderBuilder(workspaceManagerDAO: WorkspaceManagerDAO, dataRepoDAO: DataRepoDAO,
                                       samDAO: SamDAO, bqServiceFactory: GoogleBigQueryServiceFactory[IO])
                                   (implicit protected val executionContext: ExecutionContext)
  extends EntityProviderBuilder[DataRepoEntityProvider] {

  override def builds: TypeTag[DataRepoEntityProvider] = typeTag[DataRepoEntityProvider]

  override def build(requestArguments: EntityRequestArguments): DataRepoEntityProvider = {
    new DataRepoEntityProvider(requestArguments, workspaceManagerDAO, dataRepoDAO, samDAO, bqServiceFactory)
  }
}
