package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.AttributeName
import slick.jdbc.MySQLProfile.api._

import java.sql.Timestamp
import java.util.UUID

/**
 * README:
 *    1. Restore a Rawls db backup to a temporary CloudSQL instance.
 *    2. Change the passwords for rawls, root, and readonly users in your temporary CloudSQL
 *        instance for safety.
 *    3. Start a local cloud-sql-proxy connecting to the temporary CloudSQL instance.
 *    4. Modify the `slick.db` section of rawls.conf to point at your local cloud-sql-proxy.
 *        You'll probably need to use `host.docker.internal` for the hostname in the url,
 *        and update the password to what you set in step 2.
 *    5. Start Rawls locally via `config/docker-rsync-local-rawls.sh`
 *    6. Call the `/api/workspaces/quicksilverValidation` API in your locally running Rawls
 *    7. Watch your logs
 *
 */
trait CompactEntityValidation {
  this: CompactEntityQuery =>

  def mostRecentMigration: ReadAction[Timestamp] =
    sql"""select max(LAST_UPDATED)
         from WORKSPACE_SETTINGS
         where SETTING_TYPE='CompactDataTables'""".as[Timestamp].head

  def getRecentlyMigratedWorkspaces(maxDate: Timestamp, hours: Int): ReadAction[Seq[UUID]] =
    sql"""select WORKSPACE_ID
          from WORKSPACE_SETTINGS
          where SETTING_TYPE='CompactDataTables'
          and LAST_UPDATED > DATE_SUB($maxDate, INTERVAL $hours HOUR)""".as[UUID]

  // ***** for tracking re-migration process
  def getCurrentMigration: ReadAction[Option[UUID]] =
    sql"""select workspace_id from CURRENT_MIGRATION""".as[UUID].headOption

  def bootstrapCurrentMigration: ReadWriteAction[Int] =
    sql"""insert into CURRENT_MIGRATION(workspace_id)
          select id from WORKSPACE order by id asc limit 1 """.asUpdate

  def nextMigration(previousWorkspaceId: UUID): ReadAction[Option[UUID]] =
    sql"""select id from WORKSPACE where id > $previousWorkspaceId order by id asc limit 1""".as[UUID].headOption

  def updateCurrentMigration(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update CURRENT_MIGRATION set workspace_id = $workspaceId""".asUpdate

  // ***** for persisting to the ENTITY_CORRECTIONS table
  def saveMigratedEntity(workspaceId: UUID,
                         entityType: String,
                         entityName: String,
                         serializedAttributes: String
  ): ReadWriteAction[Int] =
    sql"""insert into ENTITY_CORRECTIONS(workspace_id, entity_type, name, attributes)
       values($workspaceId, $entityType, $entityName, $serializedAttributes)""".asUpdate

}
