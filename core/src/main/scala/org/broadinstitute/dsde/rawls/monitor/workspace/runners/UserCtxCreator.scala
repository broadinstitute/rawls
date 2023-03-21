package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import scala.concurrent.{ExecutionContext, Future}

trait UserCtxCreator {

  val samDAO: SamDAO
  val gcsDAO: GoogleServicesDAO

  def getUserCtx(userEmail: String)(implicit executionContext: ExecutionContext): Future[RawlsRequestContext] = for {
    petKey <- samDAO.getUserArbitraryPetServiceAccountKey(userEmail)
    userInfo <- gcsDAO.getUserInfoUsingJson(petKey)
  } yield RawlsRequestContext(userInfo)

}
