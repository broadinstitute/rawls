package org.broadinstitute.dsde.rawls.monitor.migration

object FailureModes {
  // when issuing storage transfer jobs. caused by delays in propagating iam policy changes
  val noBucketPermissionsFailure: String =
    "%FAILED_PRECONDITION: Service account project-%@storage-transfer-service.iam.gserviceaccount.com " +
      "does not have required permissions%"

  // sts rates-limits us if we run too many concurrent jobs or exceed a threshold number of requests
  val stsRateLimitedFailure: String =
    "%RESOURCE_EXHAUSTED: Quota exceeded for quota metric 'Create requests' " +
      "and limit 'Create requests per day' of service 'storagetransfer.googleapis.com'%"

  // transfer operations fail midway
  val noObjectPermissionsFailure: String =
    "%PERMISSION_DENIED%project-%@storage-transfer-service.iam.gserviceaccount.com " +
      "does not have storage.objects.% access to the Google Cloud Storage%"

  val gcsUnavailableFailure: String =
    "%UNAVAILABLE:%Additional details: GCS is temporarily unavailable."
}
