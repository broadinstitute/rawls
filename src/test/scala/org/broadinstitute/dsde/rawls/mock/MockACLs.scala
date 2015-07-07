package org.broadinstitute.dsde.rawls.mock

import org.broadinstitute.dsde.rawls.model.{BucketAccessControls, BucketAccessControl}
import org.broadinstitute.dsde.rawls.model.GCSAccessLevel._

object MockACLs {
  def bacForLevel(level: GCSAccessLevel) = BucketAccessControl(
    bucket = "dummy value",
    domain = None,
    email = None,
    entity = "dummy value",
    entityId = None,
    etag = "dummy value",
    id = "dummy value",
    kind = "dummy value",
    projectTeam = None,
    role = level,
    selfLink = "dummy value"
  )
  def bacsForLevel(level: GCSAccessLevel) = BucketAccessControls(kind = "dummy value", items = Seq(bacForLevel(level)))
}
