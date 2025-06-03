package org.broadinstitute.dsde.rawls.entities.compact

case class CompactEntityProviderConfig(
  // number of entities to handle in a single SQL statement when processing batchUpsert/batchUpdate
  batchUpsertBatchSize: Int = 250
)
