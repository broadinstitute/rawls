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

case class SpendReportingResults(spendDetails: Seq[SpendReportingAggregation],
                                 spendSummary: SpendReportingForDateRange
) {
  def +(other: SpendReportingResults): SpendReportingResults =
    new SpendReportingResults(
      spendDetails ++ other.spendDetails,
      SpendReportingForDateRange(
        (BigDecimal(this.spendSummary.cost) + BigDecimal(other.spendSummary.cost)).toString,
        (BigDecimal(this.spendSummary.credits) + BigDecimal(other.spendSummary.credits)).toString,
        this.spendSummary.currency,
        this.spendSummary.startTime,
        this.spendSummary.endTime
      )
    )
}

case class SpendReportingAggregation(aggregationKey: SpendReportingAggregationKey,
                                     spendData: Seq[SpendReportingForDateRange]
)

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
    // Egress is identified by SKU rather than by service, so egress line items are reported as the
    // synthetic service "Egress" (see TerraSpendCategories.categorize); everything else keeps its service name.
    override val bigQueryField: String =
      s"CASE WHEN ${TerraSpendCategories.egressSkuCondition} THEN '${TerraSpendCategories.Egress}' ELSE service.description END"
    override val bigQueryAlias: String = "service"
  }
}

object TerraSpendCategories {
  sealed trait TerraSpendCategory extends RawlsEnumeration[TerraSpendCategory] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): TerraSpendCategory = TerraSpendCategories.withName(name)
  }

  def withName(name: String): TerraSpendCategory = name.toLowerCase match {
    case "storage"                 => Storage
    case "compute"                 => Compute
    case "other"                   => Other
    case "egress"                  => Egress
    case "workspaceinfrastructure" => WorkspaceInfrastructure
    case _                         => throw new RawlsException(s"invalid TerraSpendCategory [${name}]")
  }

  /**
    * SKU description patterns (lower-case, SQL LIKE syntax) that identify network egress line items in the
    * GCP billing export, e.g. "Network Internet Egress from Americas to Americas",
    * "Download Worldwide Destinations (excluding Asia & Australia)" (Cloud Storage egress) and
    * "Network Inter Region Data Transfer Out from Americas to EMEA".
    */
  val egressSkuPatterns: Seq[String] = Seq("%egress%", "download %", "%data transfer out%")

  /** BigQuery boolean expression that is true when a billing export row is an egress charge. */
  val egressSkuCondition: String =
    egressSkuPatterns.map(pattern => s"LOWER(sku.description) LIKE '$pattern'").mkString("(", " OR ", ")")

  def categorize(service: String): TerraSpendCategory = service.toLowerCase.replace(" ", "") match {
    case "egress"           => Egress
    case "cloudstorage"     => Storage
    case "computeengine"    => Compute
    case "kubernetesengine" => Compute
    case _                  => Other
  }

  case object WorkspaceInfrastructure extends TerraSpendCategory
  case object Storage extends TerraSpendCategory
  case object Compute extends TerraSpendCategory
  case object Other extends TerraSpendCategory
  case object Egress extends TerraSpendCategory
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

  implicit val SpendReportingAggregationKeyParameterFormat: RootJsonFormat[SpendReportingAggregationKeyWithSub] =
    jsonFormat2(SpendReportingAggregationKeyWithSub)

  implicit val BillingProjectSpendConfigurationFormat: RootJsonFormat[BillingProjectSpendConfiguration] = jsonFormat2(
    BillingProjectSpendConfiguration
  )

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
