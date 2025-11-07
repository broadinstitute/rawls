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

}
