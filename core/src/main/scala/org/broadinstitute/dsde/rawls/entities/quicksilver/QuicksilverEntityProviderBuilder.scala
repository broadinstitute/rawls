package org.broadinstitute.dsde.rawls.entities.quicksilver

import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.{Success, Try}

class QuicksilverEntityProviderBuilder()(implicit
  protected val executionContext: ExecutionContext
) extends EntityProviderBuilder[QuicksilverEntityProvider] {

  /** declares the type of EntityProvider this builder will build.
    */
  override def builds: TypeTag[QuicksilverEntityProvider] = typeTag[QuicksilverEntityProvider]

  /** create the EntityProvider this builder knows how to create.
    */
  override def build(requestArguments: EntityRequestArguments): Try[QuicksilverEntityProvider] =
    Success(new QuicksilverEntityProvider)
}
