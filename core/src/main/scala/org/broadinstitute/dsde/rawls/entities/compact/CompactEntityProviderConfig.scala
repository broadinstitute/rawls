package org.broadinstitute.dsde.rawls.entities.compact

case class CompactEntityProviderConfig(
  // Controls approximately how large a single batch-insert SQL statement will be for the batchUpsert/batchUpdate APIs.
  // Default is 100 Mb, well under the 1Gb packet size we have set for MySQL
  maxSqlBatchSizeBytes: Int = 100 * 1024 * 1024
)
