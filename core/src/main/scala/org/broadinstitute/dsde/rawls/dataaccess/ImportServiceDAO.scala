package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkspaceName}

import scala.concurrent.Future

abstract class ImportServiceDAO {

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]]
}
