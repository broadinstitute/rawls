package org.broadinstitute.dsde.rawls.model

import java.time.LocalDateTime

object WorkspaceSpendReport {
  def newWorkspaceSpendReport(googleProjectId: String,
                              reportStartDate: LocalDateTime,
                              reportEndDate: LocalDateTime,
                              totalCompute: Option[Float],
                              totalStorage: Option[Float],
                              otherSpend: Option[Float],
                              isDataAvailable: Boolean
  ) = WorkspaceSpendReport(-1L,
    googleProjectId,
    reportStartDate,
    reportEndDate,
    totalCompute,
    totalStorage,
    otherSpend,
    isDataAvailable
  )
}

case class WorkspaceSpendReport(id: Long,
                                googleProjectId: String,
                                reportStartDate: LocalDateTime,
                                reportEndDate: LocalDateTime,
                                totalCompute: Option[Float],
                                totalStorage: Option[Float],
                                otherSpend: Option[Float],
                                isDataAvailable: Boolean)

