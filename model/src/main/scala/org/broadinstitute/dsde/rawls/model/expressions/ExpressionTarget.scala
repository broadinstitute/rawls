package org.broadinstitute.dsde.rawls.model.expressions

sealed trait ExpressionTarget { val root: String }
case object EntityTarget extends ExpressionTarget { override val root = "this." }
case object WorkspaceTarget extends ExpressionTarget { override val root = "workspace." }
