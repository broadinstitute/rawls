package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.SpendReportingAggregationKeys.SpendReportingAggregationKey
import org.broadinstitute.dsde.rawls.model.TerraSpendCategories.TerraSpendCategory
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, GoogleProject}
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsString, JsValue, JsonFormat, RootJsonFormat}

import scala.jdk.CollectionConverters._

case class BillingProjectSpendConfiguration(datasetGoogleProject: GoogleProject, datasetName: BigQueryDatasetName)

case class BillingProjectSpendExport(billingProjectName: RawlsBillingProjectName,
                                     billingAccountId: RawlsBillingAccountName,
                                     spendExportTable: Option[String]
)

case class SpendReportingAggregationKeyWithSub(key: SpendReportingAggregationKey,
                                               subAggregationKey: Option[SpendReportingAggregationKey] = None
)

case class SpendReportingResults(spendDetails: Seq[SpendReportingAggregation], spendSummary: SpendReportingForDateRange)
object SpendReportingResults {
  def apply(spendReport: bio.terra.profile.model.SpendReport): SpendReportingResults = {

    val spendDetails = spendReport.getSpendDetails.asScala
      .map(sd =>
        SpendReportingAggregation(
          aggregationKey = SpendReportingAggregationKeys.withName(sd.getAggregationKey.name()),
          spendData = sd.getSpendData.asScala
            .map(srRange =>
              SpendReportingForDateRange(
                srRange.getCost,
                srRange.getCredits,
                srRange.getCurrency,
                Option.when(srRange.getStartTime != null)(DateTime.parse(srRange.getStartTime)),
                Option.when(srRange.getEndTime != null)(DateTime.parse(srRange.getEndTime)),
                category =
                  Option.when(srRange.getCategory != null)(TerraSpendCategories.withName(srRange.getCategory.toString))
              )
            )
            .toSeq
        )
      )
      .toList

    val spendSummary = SpendReportingForDateRange(
      spendReport.getSpendSummary.getCost,
      spendReport.getSpendSummary.getCredits,
      spendReport.getSpendSummary.getCurrency,
      Option.when(spendReport.getSpendSummary.getStartTime != null)(
        DateTime.parse(spendReport.getSpendSummary.getStartTime)
      ),
      Option.when(spendReport.getSpendSummary.getEndTime != null)(
        DateTime.parse(spendReport.getSpendSummary.getEndTime)
      )
    )

    SpendReportingResults(spendDetails, spendSummary)
  }
}

case class SpendReportingAggregation(aggregationKey: SpendReportingAggregationKey,
                                     spendData: Seq[SpendReportingForDateRange]
)
object SpendReportingAggregation {
  def apply(spendReportingAggregation: bio.terra.profile.model.SpendReportingAggregation): SpendReportingAggregation = {

    val spendData = spendReportingAggregation.getSpendData.asScala
      .map(srRange =>
        SpendReportingForDateRange(
          srRange.getCost,
          srRange.getCredits,
          srRange.getCurrency,
          Option.when(srRange.getStartTime != null)(DateTime.parse(srRange.getStartTime)),
          Option.when(srRange.getEndTime != null)(DateTime.parse(srRange.getEndTime))
        )
      )
      .toList

    SpendReportingAggregation(
      SpendReportingAggregationKeys.withName(spendReportingAggregation.getAggregationKey.name()),
      spendData
    )
  }
}

case class SpendReportingForDateRange(
  cost: String,
  credits: String,
  currency: String,
  startTime: Option[DateTime] = None,
  endTime: Option[DateTime] = None,
  workspace: Option[WorkspaceName] = None,
  googleProjectId: Option[GoogleProject] = None,
  category: Option[TerraSpendCategory] = None,
  subAggregation: Option[SpendReportingAggregation] = None
)
object SpendReportingForDateRange {
  def apply(
    spendReportingForDateRange: bio.terra.profile.model.SpendReportingForDateRange
  ): SpendReportingForDateRange =
    SpendReportingForDateRange(
      spendReportingForDateRange.getCost,
      spendReportingForDateRange.getCredits,
      spendReportingForDateRange.getCurrency,
      Option.when(spendReportingForDateRange.getStartTime != null)(
        DateTime.parse(spendReportingForDateRange.getStartTime)
      ),
      Option.when(spendReportingForDateRange.getEndTime != null)(
        DateTime.parse(spendReportingForDateRange.getEndTime)
      ),
      category = Option.when(spendReportingForDateRange.getCategory != null)(
        TerraSpendCategories.withName(spendReportingForDateRange.getCategory.toString)
      )
    )
}

// Key indicating how spendData has been aggregated. Ex. 'workspace' if all data in spendData is for a particular workspace
object SpendReportingAggregationKeys {
  sealed trait SpendReportingAggregationKey extends RawlsEnumeration[SpendReportingAggregationKey] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): SpendReportingAggregationKey = SpendReportingAggregationKeys.withName(name)

    val bigQueryField: String
    val bigQueryAlias: String

    def bigQueryAliasClause(): String = s", $bigQueryField as $bigQueryAlias"
    def bigQueryGroupByClause(): String = s", $bigQueryAlias"
  }

  def withName(name: String): SpendReportingAggregationKey = name.toLowerCase match {
    case "daily"     => Daily
    case "workspace" => Workspace
    case "category"  => Category
    case _           => throw new RawlsException(s"invalid SpendReportingAggregationKey [${name}]")
  }

  case object Daily extends SpendReportingAggregationKey {
    override val bigQueryField: String = "DATE(REPLACE_TIME_PARTITION_COLUMN)"
    override val bigQueryAlias: String = "date"
  }
  case object Workspace extends SpendReportingAggregationKey {
    override val bigQueryField: String = "project.id"
    override val bigQueryAlias: String = "googleProjectId"
  }
  case object Category extends SpendReportingAggregationKey {
    override val bigQueryField: String = "service.description"
    override val bigQueryAlias: String = "service"
  }
}

object TerraSpendCategories {
  sealed trait TerraSpendCategory extends RawlsEnumeration[TerraSpendCategory] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): TerraSpendCategory = TerraSpendCategories.withName(name)
  }

  def withName(name: String): TerraSpendCategory = name.toLowerCase match {
    case "storage" => Storage
    case "compute" => Compute
    case "other"   => Other
    case _         => throw new RawlsException(s"invalid TerraSpendCategory [${name}]")
  }

  def categorize(service: String): TerraSpendCategory = service.toLowerCase.replace(" ", "") match {
    case "cloudstorage"     => Storage
    case "computeengine"    => Compute
    case "kubernetesengine" => Compute
    case _                  => Other
  }

  case object Storage extends TerraSpendCategory
  case object Compute extends TerraSpendCategory
  case object Other extends TerraSpendCategory
}

class SpendReportingJsonSupport extends JsonSupport {
  implicit object SpendReportingAggregationKeyFormat
      extends RootJsonFormat[SpendReportingAggregationKeys.SpendReportingAggregationKey] {
    override def write(obj: SpendReportingAggregationKeys.SpendReportingAggregationKey): JsValue = JsString(
      obj.toString
    )

    override def read(json: JsValue): SpendReportingAggregationKeys.SpendReportingAggregationKey = json match {
      case JsString(name) => SpendReportingAggregationKeys.withName(name)
      case _              => throw DeserializationException("could not deserialize aggregation key")
    }
  }
  implicit object TerraSpendCategoryFormat extends RootJsonFormat[TerraSpendCategories.TerraSpendCategory] {
    override def write(obj: TerraSpendCategories.TerraSpendCategory): JsValue = JsString(obj.toString)

    override def read(json: JsValue): TerraSpendCategories.TerraSpendCategory = json match {
      case JsString(name) => TerraSpendCategories.withName(name)
      case _              => throw DeserializationException("could not deserialize spend category")
    }
  }

  implicit val SpendReportingAggregationKeyParameterFormat = jsonFormat2(SpendReportingAggregationKeyWithSub)

  implicit val BillingProjectSpendConfigurationFormat = jsonFormat2(BillingProjectSpendConfiguration)

  implicit val SpendReportingAggregationFormat: JsonFormat[SpendReportingAggregation] = lazyFormat(
    jsonFormat2(SpendReportingAggregation.apply)
  )

  implicit val SpendReportingForDateRangeFormat: JsonFormat[SpendReportingForDateRange] = lazyFormat(
    jsonFormat9(SpendReportingForDateRange.apply)
  )

  implicit val SpendReportingResultsFormat: RootJsonFormat[SpendReportingResults] = jsonFormat2(
    SpendReportingResults.apply
  )
}

object SpendReportingJsonSupport extends SpendReportingJsonSupport
