package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.{DataReferenceName, GoogleProjectId, RawlsRequestContext, Workspace}

case class EntityRequestArguments(workspace: Workspace,
                                  ctx: RawlsRequestContext,
                                  dataReference: Option[DataReferenceName] = None,
                                  billingProject: Option[GoogleProjectId] = None
)
