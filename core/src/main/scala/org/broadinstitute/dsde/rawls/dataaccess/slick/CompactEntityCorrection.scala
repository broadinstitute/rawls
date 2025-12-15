package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeName
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

  /** count the outstanding corrections */
  def countOutstandingCorrections: ReadAction[Int] =
    sql"""select count(1) from ENTITY_CORRECTIONS where status = 'OUTSTANDING'""".as[Int].head

  /** retrieve the next N corrections from ENTITY_CORRECTIONS */
  def getNextCorrectionBatch(batchSize: Int): ReadAction[List[EntityCorrection]] =
    sql"""select id, workspace_id, entity_type, name, attributes
          from ENTITY_CORRECTIONS
          where status = 'OUTSTANDING'
          order by id
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
}
