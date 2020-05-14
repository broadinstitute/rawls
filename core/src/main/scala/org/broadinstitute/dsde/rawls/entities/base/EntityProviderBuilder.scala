package org.broadinstitute.dsde.rawls.entities.base

import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, UserInfo, Workspace}

import scala.reflect.runtime.universe._

/** trait definition EntityProviderBuilders.
 */
trait EntityProviderBuilder[T <: EntityProvider] {

  /** declares the type of EntityProvider this builder will build.
   */
  def builds: TypeTag[T]

  /** create the EntityProvider this builder knows how to create.
    */
  def build(workspace: Workspace,
            userInfo: UserInfo,
            dataReference: Option[String] = None,
            billingProject: Option[RawlsBillingProject] = None): T
}
