package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder

import scala.reflect.runtime.universe._

class DataRepoEntityProviderBuilder(workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String) extends EntityProviderBuilder[DataRepoEntityProvider] {
  override def builds: TypeTag[DataRepoEntityProvider] = typeTag[DataRepoEntityProvider]

  override def build(requestArguments: EntityRequestArguments): DataRepoEntityProvider = {
    new DataRepoEntityProvider(requestArguments, workspaceManagerDAO, terraDataRepoUrl)
  }
}
