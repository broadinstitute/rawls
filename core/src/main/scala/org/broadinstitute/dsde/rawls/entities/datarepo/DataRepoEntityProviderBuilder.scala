package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, UserInfo, Workspace}

import scala.reflect.runtime.universe._

class DataRepoEntityProviderBuilder extends EntityProviderBuilder[DataRepoEntityProvider] {
  override def builds: TypeTag[DataRepoEntityProvider] = typeTag[DataRepoEntityProvider]

  override def build(workspace: Workspace,
                     userInfo: UserInfo,
                     dataReference: Option[String] = None,
                     billingProject: Option[RawlsBillingProject] = None): DataRepoEntityProvider = new DataRepoEntityProvider
}
