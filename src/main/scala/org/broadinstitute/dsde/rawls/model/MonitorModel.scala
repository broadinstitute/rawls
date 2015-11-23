package org.broadinstitute.dsde.rawls.model

case class PendingBucketDeletions(buckets: Set[String]) extends DomainObject {
  def idFields = Seq()
}