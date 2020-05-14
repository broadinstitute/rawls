package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, UserInfo, Workspace}

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

/**
 * Builder for the Terra default entity provider
 */
class LocalEntityProviderBuilder(dataSource: SlickDataSource)
                                (implicit protected val executionContext: ExecutionContext)
  extends EntityProviderBuilder[LocalEntityProvider] {

  override def builds: TypeTag[LocalEntityProvider] = typeTag[LocalEntityProvider]

  override def build(requestArguments: EntityRequestArguments): LocalEntityProvider = {
    new LocalEntityProvider(requestArguments.workspace, dataSource)
  }

}
