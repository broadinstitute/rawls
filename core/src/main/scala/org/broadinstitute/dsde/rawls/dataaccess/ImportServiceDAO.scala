package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkspaceName}

import java.util.UUID
import scala.concurrent.Future

abstract class ImportServiceDAO {

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]]

  def getCwdsStatus(importId: UUID, workspaceId: UUID, userInfo: UserInfo): Future[Option[ImportStatus]]
}
