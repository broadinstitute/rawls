package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.{Success, Try}

/**
 * Builder for the Terra default entity provider
 */
class LocalEntityProviderBuilder(dataSource: SlickDataSource,
                                 cacheEnabled: Boolean,
                                 queryTimeout: Duration,
                                 metricsPrefix: String
)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProviderBuilder[LocalEntityProvider] {

  override def builds: TypeTag[LocalEntityProvider] = typeTag[LocalEntityProvider]

  override def build(requestArguments: EntityRequestArguments): Try[LocalEntityProvider] =
    Success(new LocalEntityProvider(requestArguments, dataSource, cacheEnabled, queryTimeout, metricsPrefix))
}
