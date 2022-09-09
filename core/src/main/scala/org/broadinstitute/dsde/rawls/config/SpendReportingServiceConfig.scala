package org.broadinstitute.dsde.rawls.config

final case class SpendReportingServiceConfig(
  defaultTableName: String,
  defaultTimePartitionColumn: String,
  maxDateRange: Int,
  workbenchMetricBaseName: String
)
