package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.{DataReferenceName, RawlsBillingProject, UserInfo, Workspace}

case class EntityRequestArguments(workspace: Workspace,
                                  userInfo: UserInfo,
                                  dataReference: Option[DataReferenceName] = None,
                                  billingProject: Option[RawlsBillingProject] = None)
