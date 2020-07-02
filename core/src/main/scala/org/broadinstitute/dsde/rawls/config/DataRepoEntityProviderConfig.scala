package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class DataRepoEntityProviderConfig(
                                       // calculated as number of rows in base table * number entity expressions
                                       maxInputsPerSubmission: Long,
                                       // number of results from any single BigQuery job
                                       maxRowsPerQuery: Long)

object DataRepoEntityProviderConfig {
  def apply(conf: Config): DataRepoEntityProviderConfig = {
    DataRepoEntityProviderConfig(
      conf.getLong("maxInputsPerSubmission"),
      conf.getLong("maxRowsPerQuery")
    )
  }
}
