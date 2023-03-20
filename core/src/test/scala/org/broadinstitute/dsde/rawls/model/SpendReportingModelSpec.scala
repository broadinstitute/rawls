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
    val defaultCredits = "0" // always 0 for Azure

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

    def someBPMNonEmptyReport(from: DateTime, to: DateTime, costs: Map[CategoryEnum, BigDecimal]): SpendReport = {
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
            new SpendReportingAggregationBPM()
              .aggregationKey(SpendReportingAggregationBPM.AggregationKeyEnum.CATEGORY)
              .spendData(categoriesCosts)
          )
        )
    }

    private def buildSpendReportingForDateRange(cost: String,
                                                currency: String,
                                                category: SpendReportingForDateRangeBPM.CategoryEnum,
                                                from: String,
                                                to: String
    ): SpendReportingForDateRangeBPM =
      new SpendReportingForDateRangeBPM()
        .cost(cost)
        .credits(defaultCredits)
        .currency(currency)
        .category(category)
        .startTime(from)
        .endTime(to)
  }

  behavior of "SpendReportingResultsConvertor"

  it should "successfully convert empty SpendReport" in {
    val emptySpendReport = TestData.buildBPMEmptyReport

    val result = SpendReportingResultsConvertor(emptySpendReport)

    result shouldNot equal(null)
  }

  it should "successfully convert some non empty SpendReport" in {
    val from = DateTime.now().minusMonths(2)
    val to = DateTime.now().plusMonths(1)
    val computeCost: BigDecimal = 100.20
    val storageCost: BigDecimal = 54.32
    val categoriesCosts = Map(CategoryEnum.COMPUTE -> computeCost, CategoryEnum.STORAGE -> storageCost)
    val someReport = TestData.someBPMNonEmptyReport(from, to, categoriesCosts)

    val result = SpendReportingResultsConvertor(someReport)

    result shouldNot equal(null)
    result.spendSummary shouldNot equal(null)
    result.spendSummary.cost shouldBe categoriesCosts.values.sum.toString()
    result.spendDetails shouldNot equal(null)
    result.spendDetails(0) shouldNot equal(null)
    result.spendDetails(0).spendData should have size categoriesCosts.size

    val computeDetails = result
      .spendDetails(0)
      .spendData
      .find(v => v.category.nonEmpty && v.category.get.equals(TerraSpendCategories.Compute))
    computeDetails.get shouldNot equal(null)
    computeDetails.get.cost shouldBe computeCost.toString()
    computeDetails.get.credits shouldBe TestData.defaultCredits
    computeDetails.get.currency shouldBe TestData.defaultCurrency

    val storageDetails = result
      .spendDetails(0)
      .spendData
      .find(v => v.category.nonEmpty && v.category.get.equals(TerraSpendCategories.Storage))
    storageDetails.get shouldNot equal(null)
    storageDetails.get.cost shouldBe storageCost.toString()
    storageDetails.get.credits shouldBe TestData.defaultCredits
    storageDetails.get.currency shouldBe TestData.defaultCurrency
  }
}
