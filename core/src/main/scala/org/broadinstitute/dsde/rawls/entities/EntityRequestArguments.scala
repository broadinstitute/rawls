package org.broadinstitute.dsde.rawls.entities

import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}

case class EntityRequestArguments(workspace: Workspace, ctx: RawlsRequestContext)
