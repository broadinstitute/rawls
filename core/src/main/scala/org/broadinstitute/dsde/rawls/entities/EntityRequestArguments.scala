package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, UserInfo, Workspace}

case class EntityRequestArguments(workspace: Workspace,
                                  userInfo: UserInfo,
                                  dataReference: Option[String] = None,
                                  billingProject: Option[RawlsBillingProject] = None)
