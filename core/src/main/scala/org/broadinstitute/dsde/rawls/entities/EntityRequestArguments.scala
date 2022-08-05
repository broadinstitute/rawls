package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.{DataReferenceName, GoogleProjectId, UserInfo, Workspace}

case class EntityRequestArguments(workspace: Workspace,
                                  userInfo: UserInfo,
                                  dataReference: Option[DataReferenceName] = None,
                                  billingProject: Option[GoogleProjectId] = None)
