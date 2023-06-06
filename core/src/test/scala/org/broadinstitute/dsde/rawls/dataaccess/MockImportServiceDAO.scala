package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkspaceName}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MockImportServiceDAO extends ImportServiceDAO {

  val imports: TrieMap[UUID, ImportStatus] = TrieMap()

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]] =
    Future.successful(imports.get(importId))

}
