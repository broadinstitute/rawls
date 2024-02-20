package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.dataaccess.ImportServiceDAO
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkspaceName}

import java.util.UUID
import scala.concurrent.Future

class DisabledImportServiceDAO
 extends ImportServiceDAO {
  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]] = {
    throw new NotImplementedError("curatorGroupName method is not implemented for Azure.")
  }
}

