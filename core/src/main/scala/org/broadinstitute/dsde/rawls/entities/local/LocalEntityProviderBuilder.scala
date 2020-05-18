package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.model.Workspace

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

/**
 * Builder for the Terra default entity provider
 */
class LocalEntityProviderBuilder(dataSource: SlickDataSource)
                                (implicit protected val executionContext: ExecutionContext)
  extends EntityProviderBuilder[LocalEntityProvider] {

  override def builds: TypeTag[LocalEntityProvider] = typeTag[LocalEntityProvider]

  override def build(workspace: Workspace): LocalEntityProvider = {
    new LocalEntityProvider(workspace, dataSource)
  }

}
