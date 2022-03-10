package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.SpendReportingAggregationKeys.SpendReportingAggregationKey
import org.broadinstitute.dsde.rawls.model.TerraSpendCategories.TerraSpendCategory
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, GoogleProject}
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

case class BillingProjectSpendConfiguration(datasetGoogleProject: GoogleProject, datasetName: BigQueryDatasetName)

case class BillingProjectSpendExport(billingProjectName: RawlsBillingProjectName, billingAccountId: RawlsBillingAccountName, spendExportTable: Option[String])

case class SpendReportingResults(spendDetails: Seq[SpendReportingAggregation], spendSummary: SpendReportingForDateRange)
case class SpendReportingAggregation(aggregationKey: SpendReportingAggregationKey, spendData: Seq[SpendReportingForDateRange])
case class SpendReportingForDateRange(
                                       cost: String,
                                       credits: String,
                                       currency: String,
                                       startTime: DateTime,
                                       endTime: DateTime,
                                       workspace: Option[WorkspaceName] = None,
                                       googleProjectId: Option[GoogleProject] = None,
                                       service: Option[CategorizedGCPService] = None
                                     )

// Key indicating how spendData has been aggregated. Ex. 'workspace' if all data in spendData is for a particular workspace
object SpendReportingAggregationKeys {
  sealed trait SpendReportingAggregationKey extends RawlsEnumeration[SpendReportingAggregationKey] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): SpendReportingAggregationKey = SpendReportingAggregationKeys.withName(name)
  }

  def withName(name: String): SpendReportingAggregationKey = name.toLowerCase match {
    case "daily" => Daily
    case "workspace" => Workspace
    case "category" => Category
    case _ => throw new RawlsException(s"invalid SpendReportingAggregationKey [${name}]")
  }

  case object Daily extends SpendReportingAggregationKey
  case object Workspace extends SpendReportingAggregationKey
  case object Category extends SpendReportingAggregationKey
}

object TerraSpendCategories {
  sealed trait TerraSpendCategory extends RawlsEnumeration[TerraSpendCategory] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): TerraSpendCategory = TerraSpendCategories.withName(name)
  }

  def withName(name: String): TerraSpendCategory = name.toLowerCase match {
    case "storage" => Storage
    case "compute" => Compute
    case "other" => Other
    case _ => throw new RawlsException(s"invalid TerraSpendCategory [${name}]")
  }

  def categorize(service: String): CategorizedGCPService = service.toLowerCase.replace(" ", "") match {
    case "cloudstorage" => CategorizedGCPService(service, Storage)
    case "computeengine" => CategorizedGCPService(service, Compute)
    case "kubernetesengine" => CategorizedGCPService(service, Compute)
    case _ => CategorizedGCPService(service, Other)
  }

  case object Storage extends TerraSpendCategory
  case object Compute extends TerraSpendCategory
  case object Other extends TerraSpendCategory
}

case class CategorizedGCPService(name: String, category: TerraSpendCategory)

class SpendReportingJsonSupport extends JsonSupport {
  implicit object SpendReportingAggregationKeyFormat extends RootJsonFormat[SpendReportingAggregationKeys.SpendReportingAggregationKey] {
    override def write(obj: SpendReportingAggregationKeys.SpendReportingAggregationKey): JsValue = JsString(obj.toString)

    override def read(json: JsValue): SpendReportingAggregationKeys.SpendReportingAggregationKey = json match {
      case JsString(name) => SpendReportingAggregationKeys.withName(name)
      case _ => throw DeserializationException("could not deserialize aggregation key")
    }
  }
  implicit object TerraSpendCategoryFormat extends RootJsonFormat[TerraSpendCategories.TerraSpendCategory] {
    override def write(obj: TerraSpendCategories.TerraSpendCategory): JsValue = JsString(obj.toString)

    override def read(json: JsValue): TerraSpendCategories.TerraSpendCategory = json match {
      case JsString(name) => TerraSpendCategories.withName(name)
      case _ => throw DeserializationException("could not deserialize spend category")
    }
  }

  implicit val BillingProjectSpendConfigurationFormat = jsonFormat2(BillingProjectSpendConfiguration)

  implicit val CategorizedGCPServiceFormat = jsonFormat2(CategorizedGCPService)

  implicit val SpendReportingForDateRangeFormat = jsonFormat8(SpendReportingForDateRange)

  implicit val SpendReportingAggregationFormat = jsonFormat2(SpendReportingAggregation)

  implicit val SpendReportingResultsFormat = jsonFormat2(SpendReportingResults)
}

object SpendReportingJsonSupport extends SpendReportingJsonSupport
