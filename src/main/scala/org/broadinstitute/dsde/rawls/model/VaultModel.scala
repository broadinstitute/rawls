package org.broadinstitute.dsde.rawls.model

import scala.annotation.meta.field

case class VaultEntity(
  id: String,
  getURL: String,
  entityType: String,
  attrs: Map[String, Any]
)
