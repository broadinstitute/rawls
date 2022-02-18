package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._

case class BillingProjectSpendConfiguration(datasetGoogleProject: GoogleProject, datasetName: BigQueryDatasetName)

case class BillingProjectSpendExport(billingProjectName: RawlsBillingProjectName, billingAccountId: RawlsBillingAccountName, spendExportTable: Option[String])

case class SpendReportingResults(spendDetails: Seq[SpendReportingAggregation], spendSummary: SpendReportingForDateRange)
case class SpendReportingAggregation(aggregationKey: SpendReportingAggregationKey, spendData: Seq[SpendReportingForDateRange])
case class SpendReportingForDateRange(
                                       cost: String,
                                       credits: String,
                                       currency: String,
                                       startTime: DateTime,
                                       endTime: DateTime
                                     )

case class SpendReportingAggregationKey(key: String)

class SpendReportingJsonSupport extends JsonSupport {
  implicit val BillingProjectSpendConfigurationFormat = jsonFormat2(BillingProjectSpendConfiguration)

  implicit val SpendReportingForDateRangeFormat = jsonFormat5(SpendReportingForDateRange)

  implicit val SpendReportingAggregationKeyFormat = jsonFormat1(SpendReportingAggregationKey)

  implicit val SpendReportingAggregationFormat = jsonFormat2(SpendReportingAggregation)

  implicit val SpendReportingResultsFormat = jsonFormat2(SpendReportingResults)
}

object SpendReportingJsonSupport extends SpendReportingJsonSupport
