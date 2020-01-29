package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{RawlsGroup, RawlsGroupRef, RawlsUser, UserInfo, WorkspaceName}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MockImportServiceDAO extends ImportServiceDAO {

  val groups: TrieMap[UUID, ImportStatus] = TrieMap()

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]] = {
    Future.successful(groups.get(importId))
  }

}
