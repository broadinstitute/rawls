package org.broadinstitute.dsde.rawls.model.deltalayer.v1

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsUserSubjectId}
import spray.json.JsValue

import java.time.Instant
import java.util.UUID

case class DeltaInsert(insertId: UUID,
                       insertTimestamp: Instant,
                       source: InsertSource,
                       destination: InsertDestination,
                       inserts: Seq[DeltaRow])

case class InsertDestination(workspaceId: UUID,
                             bqDataset: String,
                             datasetProject: GoogleProjectId,
                             projectToBill: Option[GoogleProjectId])

case class InsertSource(referenceId: UUID,
                        insertingUser: RawlsUserSubjectId)

case class DeltaRow(datarepoRowId: UUID,
                    name: String,
                    value: JsValue)
