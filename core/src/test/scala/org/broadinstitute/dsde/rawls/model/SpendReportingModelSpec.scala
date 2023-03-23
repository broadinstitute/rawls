package org.broadinstitute.dsde.rawls.model

import bio.terra.profile.model.SpendReport
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
    def buildBPMEmptyReport: SpendReport = {
      // requested period
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

    def someBPMNonEmptyReport(from: DateTime, to: DateTime, costs: Seq[BigDecimal]): SpendReport = {
      val currency = "USD"
      val categoriesCosts = costs
        .map(cost =>
          buildSpendReportingForDateRange(cost.toString(),
                                          currency,
                                          null,
                                          from.toString(ISODateTimeFormat.date()),
                                          to.toString(ISODateTimeFormat.date())
          )
        )
        .asJava
      new SpendReport()
        .spendSummary(
          buildSpendReportingForDateRange(costs.sum.toString(),
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
        .credits("0")
        .currency(currency)
        .category(category)
        .startTime(from)
        .endTime(to)
  }

  behavior of "SpendReportingResultsConvertor"

  it should "successfully convert empty SpendReport" in {
    val emptySpendReport = TestData.buildBPMEmptyReport

    val result = SpendReportingResultsConvertor.apply(emptySpendReport)

    result shouldNot equal(null)
  }

  it should "successfully convert some non empty SpendReport" in {
    val from = DateTime.now().minusMonths(2)
    val to = DateTime.now().plusMonths(1)
    val computeCost: BigDecimal = 100.20
    val storageCost: BigDecimal = 54.32
    val categoriesCosts = Seq(computeCost, storageCost)

    val result = TestData.someBPMNonEmptyReport(from, to, categoriesCosts)

    result shouldNot equal(null)
    result.getSpendSummary shouldNot equal(null)
    result.getSpendDetails shouldNot equal(null)
    result.getSpendDetails.get(0) shouldNot equal(null)
    result.getSpendDetails.get(0).getSpendData should have size categoriesCosts.size
  }
}
