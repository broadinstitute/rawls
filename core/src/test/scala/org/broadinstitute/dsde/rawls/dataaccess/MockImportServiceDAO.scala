package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.UserInfo

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MockImportServiceDAO extends ImportServiceDAO {

  val imports: TrieMap[UUID, ImportStatus] = TrieMap()

  override def getCwdsStatus(importId: UUID, workspaceId: UUID, userInfo: UserInfo): Future[Option[ImportStatus]] =
    Future.successful(imports.get(importId))
}
