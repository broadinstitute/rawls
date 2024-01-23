package org.broadinstitute.dsde.rawls.model

import bio.terra.profile.model.SpendReport
import bio.terra.profile.model.SpendReportingForDateRange.CategoryEnum
import bio.terra.profile.model.{SpendReportingForDateRange => SpendReportingForDateRangeBPM}
import bio.terra.profile.model.{SpendReportingAggregation => SpendReportingAggregationBPM}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

import scala.jdk.CollectionConverters._

class SpendReportingModelSpec extends AnyFlatSpecLike {
  object TestData {
    val defaultCurrency = "USD"
    val defaultAzureCredits = "0" // always 0 for Azure

    def buildBPMEmptyReport: SpendReport = {
      // some period
      val from = DateTime.now().minusMonths(2)
      val to = from.plusMonths(1)

      // BVM return following spendReport in case specific data doesn't exist
      new SpendReport()
        .spendSummary(
          buildSpendReportingForDateRange("0.00",
                                          "n/a",
                                          null,
                                          from.toString(ISODateTimeFormat.date()),
                                          to.toString(ISODateTimeFormat.date())
          )
        )
        .spendDetails(
          java.util.List.of(
            new SpendReportingAggregationBPM()
              .aggregationKey(SpendReportingAggregationBPM.AggregationKeyEnum.CATEGORY)
              .spendData(java.util.List.of())
          )
        )
    }

    def someBPMNonEmptyReport(from: DateTime, to: DateTime, costs: Map[CategoryEnum, BigDecimal]): SpendReport =
      new SpendReport()
        .spendSummary(
          buildSpendReportingForDateRange(costs.values.sum.toString(),
                                          defaultCurrency,
                                          null,
                                          from.toString(ISODateTimeFormat.date()),
                                          to.toString(ISODateTimeFormat.date())
          )
        )
        .spendDetails(
          java.util.List.of(
            buildSpendReportingAggregation(costs)
          )
        )

    def buildSpendReportingForDateRange(cost: String,
                                        currency: String,
                                        category: SpendReportingForDateRangeBPM.CategoryEnum,
                                        from: String,
                                        to: String
    ): SpendReportingForDateRangeBPM =
      new SpendReportingForDateRangeBPM()
        .cost(cost)
        .credits(defaultAzureCredits)
        .currency(currency)
        .category(category)
        .startTime(from)
        .endTime(to)

    def buildSpendReportingAggregation(costs: Map[CategoryEnum, BigDecimal]): SpendReportingAggregationBPM = {
      val categoriesCosts = costs
        .map(costPerCategoryKvp =>
          buildSpendReportingForDateRange(costPerCategoryKvp._2.toString,
                                          defaultCurrency,
                                          costPerCategoryKvp._1,
                                          null,
                                          null
          )
        )
        .asJavaCollection
        .stream()
        .toList

      new SpendReportingAggregationBPM()
        .aggregationKey(SpendReportingAggregationBPM.AggregationKeyEnum.CATEGORY)
        .spendData(categoriesCosts)
    }
  }

  behavior of "SpendReportingResults conversion"

  it should "successfully convert empty SpendReport" in {
    val emptySpendReport = TestData.buildBPMEmptyReport

    val result = SpendReportingResults(emptySpendReport)

    result shouldNot equal(null)
  }

  it should "successfully convert some non empty SpendReport" in {
    val from = DateTime.now().minusMonths(2)
    val to = DateTime.now().plusMonths(1)
    val computeCost: BigDecimal = 100.20
    val storageCost: BigDecimal = 54.32
    val k8sInfrastructureCost: BigDecimal = 75.15
    val categoriesCosts = Map(CategoryEnum.COMPUTE -> computeCost,
                              CategoryEnum.STORAGE -> storageCost,
                              CategoryEnum.WORKSPACEINFRASTRUCTURE -> k8sInfrastructureCost
    )
    val someReport = TestData.someBPMNonEmptyReport(from, to, categoriesCosts)

    val result = SpendReportingResults(someReport)

    result shouldNot equal(null)
    result.spendSummary shouldNot equal(null)
    result.spendSummary.cost shouldBe categoriesCosts.values.sum.toString()
    result.spendDetails shouldNot equal(null)
    result.spendDetails.head shouldNot equal(null)
    result.spendDetails.head.spendData should have size categoriesCosts.size

    val computeDetails = result.spendDetails.head.spendData
      .find(v => v.category.nonEmpty && v.category.get.equals(TerraSpendCategories.Compute))
    computeDetails.nonEmpty shouldBe true
    computeDetails.get.cost shouldBe computeCost.toString()
    computeDetails.get.credits shouldBe TestData.defaultAzureCredits
    computeDetails.get.currency shouldBe TestData.defaultCurrency
    computeDetails.get.category.get shouldBe TerraSpendCategories.Compute

    val storageDetails = result.spendDetails.head.spendData
      .find(v => v.category.nonEmpty && v.category.get.equals(TerraSpendCategories.Storage))
    storageDetails.nonEmpty shouldBe true
    storageDetails.get.cost shouldBe storageCost.toString()
    storageDetails.get.credits shouldBe TestData.defaultAzureCredits
    storageDetails.get.currency shouldBe TestData.defaultCurrency
    storageDetails.get.category.get shouldBe TerraSpendCategories.Storage

    val k8sDetails = result.spendDetails.head.spendData
      .find(v => v.category.nonEmpty && v.category.get.equals(TerraSpendCategories.WorkspaceInfrastructure))
    k8sDetails.nonEmpty shouldBe true
    k8sDetails.get.cost shouldBe k8sInfrastructureCost.toString()
    k8sDetails.get.credits shouldBe TestData.defaultAzureCredits
    k8sDetails.get.currency shouldBe TestData.defaultCurrency
    k8sDetails.get.category.get shouldBe TerraSpendCategories.WorkspaceInfrastructure
  }

  behavior of "SpendReportingForDateRange conversion"

  it should "successfully convert BPM model for SpendReportingForDateRange" in {
    val cost = "20.51"
    val category = CategoryEnum.STORAGE
    val from = DateTime.now().minusMonths(2)
    val to = from.plusMonths(1)

    val spendReportingForDateRangeBPM =
      TestData.buildSpendReportingForDateRange(cost,
                                               TestData.defaultCurrency,
                                               category,
                                               from.toString(ISODateTimeFormat.date()),
                                               to.toString(ISODateTimeFormat.date())
      )

    val result = SpendReportingForDateRange(spendReportingForDateRangeBPM)

    result shouldNot equal(null)
    result.cost shouldBe cost
    result.category.nonEmpty shouldBe true
    result.category.get shouldBe TerraSpendCategories.Storage
    result.currency shouldBe TestData.defaultCurrency
    result.credits shouldBe TestData.defaultAzureCredits
    result.startTime.nonEmpty shouldBe true
    result.startTime.get.toString(ISODateTimeFormat.date()) shouldBe from.toString(ISODateTimeFormat.date())
    result.endTime.nonEmpty shouldBe true
    result.endTime.get.toString(ISODateTimeFormat.date()) shouldBe to.toString(ISODateTimeFormat.date())
  }

  behavior of "SpendReportingAggregation conversion"

  it should "successfully convert BPM model for SpendReportingAggregation" in {
    val computeCost: BigDecimal = 1000.20
    val storageCost: BigDecimal = 900.32
    val workspaceInfrastructureCost: BigDecimal = 2500.01
    val categoriesCosts = Map(CategoryEnum.COMPUTE -> computeCost,
                              CategoryEnum.STORAGE -> storageCost,
                              CategoryEnum.WORKSPACEINFRASTRUCTURE -> workspaceInfrastructureCost
    )

    val spendReportingAggregationBPM = TestData.buildSpendReportingAggregation(categoriesCosts)
    val result = SpendReportingAggregation(spendReportingAggregationBPM)

    result shouldNot equal(null)
    result.aggregationKey shouldBe SpendReportingAggregationKeys.Category
    result.spendData should have size categoriesCosts.size

    val computeCategory =
      result.spendData.find(sd => sd.category.nonEmpty && sd.category.get.equals(TerraSpendCategories.Compute))
    computeCategory.nonEmpty shouldBe true
    computeCategory.get.cost shouldBe computeCost.toString()
    computeCategory.get.credits shouldBe TestData.defaultAzureCredits
    computeCategory.get.currency shouldBe TestData.defaultCurrency
    computeCategory.get.category.get shouldBe TerraSpendCategories.Compute

    val storageCategory =
      result.spendData.find(sd => sd.category.nonEmpty && sd.category.get.equals(TerraSpendCategories.Storage))
    storageCategory.nonEmpty shouldBe true
    storageCategory.get.cost shouldBe storageCost.toString()
    storageCategory.get.credits shouldBe TestData.defaultAzureCredits
    storageCategory.get.currency shouldBe TestData.defaultCurrency
    storageCategory.get.category.get shouldBe TerraSpendCategories.Storage

    val workspaceInfrastructureCategory =
      result.spendData.find(sd =>
        sd.category.nonEmpty && sd.category.get.equals(TerraSpendCategories.WorkspaceInfrastructure)
      )
    workspaceInfrastructureCategory.nonEmpty shouldBe true
    workspaceInfrastructureCategory.get.cost shouldBe workspaceInfrastructureCost.toString()
    workspaceInfrastructureCategory.get.credits shouldBe TestData.defaultAzureCredits
    workspaceInfrastructureCategory.get.currency shouldBe TestData.defaultCurrency
    workspaceInfrastructureCategory.get.category.get shouldBe TerraSpendCategories.WorkspaceInfrastructure
  }

}
