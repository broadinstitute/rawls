package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.Workspace

import scala.reflect.runtime.universe._

import scala.concurrent.ExecutionContext

/**
 * Builder for the Terra default entity provider
 */
class RawlsEntityProviderBuilder(dataSource: SlickDataSource)
                                (implicit protected val executionContext: ExecutionContext)
  extends EntityProviderBuilder[RawlsEntityProvider] {

  override def builds: TypeTag[RawlsEntityProvider] = typeTag[RawlsEntityProvider]

  override def build(workspace: Workspace): RawlsEntityProvider = {
    new RawlsEntityProvider(workspace, dataSource)
  }

}
