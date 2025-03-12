package org.broadinstitute.dsde.rawls.model

import java.time.LocalDateTime

object WorkspaceSpendReport {
  def newWorkspaceSpendReport(googleProjectId: String,
                              reportStartDate: LocalDateTime,
                              reportEndDate: LocalDateTime,
                              currency: String,
                              isDataAvailable: Boolean,
                              totalCompute: Option[Float],
                              totalStorage: Option[Float],
                              otherSpend: Option[Float],
                              computeCredits: Option[Float],
                              storageCredits: Option[Float],
                              otherCredits: Option[Float]
  ) = WorkspaceSpendReport(
    -1L,
    googleProjectId,
    reportStartDate,
    reportEndDate,
    currency,
    isDataAvailable,
    totalCompute,
    totalStorage,
    otherSpend,
    computeCredits,
    storageCredits,
    otherCredits
  )

  def newEmptySpendReport(googleProjectId: String,
                          reportStartDate: LocalDateTime,
                          reportEndDate: LocalDateTime,
                          currency: String
  ) = WorkspaceSpendReport(
    -1L,
    googleProjectId,
    reportStartDate,
    reportEndDate,
    currency,
    isDataAvailable = false,
    Option.empty,
    Option.empty,
    Option.empty,
    Option.empty,
    Option.empty,
    Option.empty
  )
}

case class WorkspaceSpendReport(id: Long,
                                googleProjectId: String,
                                reportStartDate: LocalDateTime,
                                reportEndDate: LocalDateTime,
                                currency: String,
                                isDataAvailable: Boolean,
                                totalCompute: Option[Float],
                                totalStorage: Option[Float],
                                otherSpend: Option[Float],
                                computeCredits: Option[Float],
                                storageCredits: Option[Float],
                                otherCredits: Option[Float]
)
