package org.broadinstitute.dsde.rawls.datamigration

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{RawlsTransaction, DbContainerDAO}
import org.broadinstitute.dsde.rawls.model.RawlsGroupEmail
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel

/**
 * Populates missing rawls group email addresses for workspace access groups
 */
object AddWorkspaceGroupEmailMigration extends DataMigration {
  override val migrationId: String = "AddWorkspaceGroupEmailMigration"

  override def migrate(config: Config, containerDAO: DbContainerDAO, txn: RawlsTransaction): Unit = {
    val appsDomain = config.getString("gcs.appsDomain")
    def toGroupId(bucketName: String, accessLevel: WorkspaceAccessLevel) = s"${bucketName}-${accessLevel.toString}@${appsDomain}"

    val accessGroups = for (
      workspace <- containerDAO.workspaceDAO.list(txn);
      (accessLevel, accessGroupRef) <- workspace.accessLevels;
      accessGroup <- containerDAO.authDAO.loadGroup(accessGroupRef, txn)
      if (accessGroup.groupEmail == RawlsGroupEmail(""))
    ) {
      containerDAO.authDAO.saveGroup(accessGroup.copy(groupEmail = RawlsGroupEmail(toGroupId(workspace.bucketName, accessLevel))), txn)
    }
  }

}
