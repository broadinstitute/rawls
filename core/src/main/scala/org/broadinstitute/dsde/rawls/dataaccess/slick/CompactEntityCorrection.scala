package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{AttributeName, Entity}
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatus.AttributeCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatus.EntityCorrectionStatusType
import slick.jdbc.GetResult
import slick.jdbc.MySQLProfile.api._

import java.util.UUID

// low-level representation of a correction as saved in MySQL
case class EntityCorrectionRecord(
  id: Long,
  workspaceId: UUID,
  entityType: String,
  entityName: String,
  attributes: String
)

// high-level representation of a correction for use in the Scala tier
object EntityCorrection {
  def fromRecord(rec: EntityCorrectionRecord): EntityCorrection =
    new EntityCorrection(
      rec.id,
      rec.workspaceId,
      rec.entityType,
      rec.entityName,
      CompactEntitySerialization.fromSql(Option(rec.attributes))
    )
}
case class EntityCorrection(
  id: Long,
  workspaceId: UUID,
  entityType: String,
  entityName: String,
  attributes: AttributeMap
)

trait CompactEntityCorrection {
  this: CompactEntityQuery =>

  implicit val getEntityCorrectionRecord: GetResult[EntityCorrectionRecord] =
    GetResult(r => EntityCorrectionRecord(r.<<, r.<<, r.<<, r.<<, r.<<))

  val CORRECTION_SQL =
    """
       from ENTITY_CORRECTIONS ec
          where ec.status in ('Correctable', 'Mixed')
	        and ec.consent = 'Yes'
      """

  /** count the outstanding corrections */
  def countOutstandingCorrections: ReadAction[Int] =
    sql"""select count(1)
          #$CORRECTION_SQL
         """.as[Int].head
  // 9,616,714

  /**
   * retrieve the next N corrections from ENTITY_CORRECTIONS:
   * */
  def getNextCorrectionBatch(batchSize: Int): ReadAction[List[EntityCorrection]] =
    sql"""select ec.id, ec.workspace_id, ec.entity_type, ec.name, ec.attributes
          #$CORRECTION_SQL
          limit $batchSize
         """
      .as[EntityCorrectionRecord]
      .map(recs => recs.toList.map(r => EntityCorrection.fromRecord(r)))

  /** update a single ENTITY_CORRECTIONS's status */
  def updateCorrectionStatus(workspaceId: UUID,
                             entityType: String,
                             entityName: String,
                             status: EntityCorrectionStatusType
  ): ReadWriteAction[Int] =
    sql"""update ENTITY_CORRECTIONS set status = ${status.toString}
         where workspace_id = $workspaceId
         and entity_type = $entityType
         and name = $entityName;""".asUpdate

  /** upsert ATTRIBUTE_CORRECTIONS statuses */
  def updateAttributeStatuses(correctionId: Long,
                              statuses: Map[AttributeName, AttributeCorrectionStatusType]
  ): ReadWriteAction[Int] =
    if (statuses.isEmpty) {
      DBIO.successful(0)
    } else {

      val startSql =
        sql"""insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
          values """

      val valuesSql = reduceSqlActionsWithDelim(
        statuses.map { case (attrName, status) =>
          sql"""($correctionId, ${attrName.namespace}, ${attrName.name}, ${status.toString})"""
        }.toSeq,
        sql","
      )

      val endSql = sql""" as newvalues on duplicate key update status = newvalues.status;"""

      concatSqlActions(startSql, valuesSql, endSql).asUpdate
    }

  def saveUncorrectedEntity(workspaceId: UUID, uncorrectedEntity: Entity): ReadWriteAction[Int] =
    sql"""update ENTITY_CORRECTIONS
          set corrected_at = ${java.sql.Timestamp.from(java.time.Instant.now())},
              history = ${CompactEntitySerialization.toSql(uncorrectedEntity.attributes).compactPrint}
          where workspace_id = $workspaceId
            and entity_type = ${uncorrectedEntity.entityType}
            and name = ${uncorrectedEntity.name}""".asUpdate

  def updateWorkspaceGone(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update ENTITY_CORRECTIONS
          set status = 'CurrentGone'
          where workspace_id = $workspaceId
      """.asUpdate

  def updateWorkspaceModified(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update ENTITY_CORRECTIONS
          set status = CONCAT(status, 'Modified')
          where workspace_id = $workspaceId
          and status not like '%Modified'
      """.asUpdate

  def checkWorkspaceLastModified(workspaceId: UUID): ReadAction[Option[Boolean]] =
    sql"""
         select (w.last_modified > ws.LAST_UPDATED)
         from WORKSPACE w, WORKSPACE_SETTINGS ws
         where w.id = ws.WORKSPACE_ID
         and w.id = $workspaceId
         and ws.SETTING_TYPE = 'CompactDataTables'
         and ws.STATUS = 'Applied'
         """.as[Boolean].headOption

  def checkWorkspaceLastWorkflowRun(workspaceId: UUID): ReadAction[Option[Boolean]] =
    sql"""
         with LAST_WORKFLOW as (
           select s.WORKSPACE_ID as workspace_id, max(wf.status_last_changed) as workflow_last_run
           from WORKFLOW wf, SUBMISSION s
           where wf.SUBMISSION_ID = s.ID
           and s.WORKSPACE_ID = $workspaceId
           group by s.WORKSPACE_ID
         )
         select (lw.workflow_last_run > ws.LAST_UPDATED)
         from WORKSPACE w
          join WORKSPACE_SETTINGS ws on w.id = ws.WORKSPACE_ID
          left outer join LAST_WORKFLOW lw on w.id = lw.workspace_id
         where w.id = $workspaceId
          and ws.SETTING_TYPE = 'CompactDataTables'
          and ws.STATUS = 'Applied'
         """.as[Boolean].headOption

}
