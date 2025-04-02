package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.{Success, Try}

class CompactEntityProviderBuilder(dataSource: SlickDataSource)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProviderBuilder[CompactEntityProvider] {

  /** declares the type of EntityProvider this builder will build.
    */
  override def builds: TypeTag[CompactEntityProvider] = typeTag[CompactEntityProvider]

  /** create the EntityProvider this builder knows how to create.
    */
  override def build(requestArguments: EntityRequestArguments): Try[CompactEntityProvider] =
    Success(new CompactEntityProvider(requestArguments, new CompactEntityRepository(dataSource), dataSource))
}
