package org.broadinstitute.dsde.rawls.model.deltalayer.v1

import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsUserSubjectIdFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.GoogleProjectIdFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import spray.json.DefaultJsonProtocol._
import spray.json._

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

object DeltaLayerJsonSupport extends DeltaLayerJsonSupport

// extend JsonSupport in order to reuse its UUIDFormat
class DeltaLayerJsonSupport extends JsonSupport {

  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  implicit val deltaRowFormat = jsonFormat3(DeltaRow)
  implicit val insertSourceFormat = jsonFormat2(InsertSource)
  implicit val insertDestinationFormat = jsonFormat4(InsertDestination)
  implicit val deltaInsertFormat = jsonFormat5(DeltaInsert)

}
