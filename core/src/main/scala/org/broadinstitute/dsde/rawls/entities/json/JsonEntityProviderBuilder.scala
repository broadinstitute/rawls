package org.broadinstitute.dsde.rawls.entities.json

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityProvider

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeTag
import scala.util.{Success, Try}

class JsonEntityProviderBuilder(dataSource: SlickDataSource,
                                cacheEnabled: Boolean,
                                queryTimeout: Duration,
                                metricsPrefix: String
)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProviderBuilder[JsonEntityProvider] {

  /** declares the type of EntityProvider this builder will build.
    */
  override def builds: universe.TypeTag[JsonEntityProvider] = typeTag[JsonEntityProvider]

  /** create the EntityProvider this builder knows how to create.
    */
  override def build(requestArguments: EntityRequestArguments): Try[JsonEntityProvider] = Success(
    new JsonEntityProvider(requestArguments, dataSource, cacheEnabled, queryTimeout, metricsPrefix)
  )
}
