package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}

import scala.concurrent.ExecutionContext

trait AuthUtil {
  val samDAO: SamDAO
  val googleServicesDAO: GoogleServicesDAO

  def getPetServiceAccountUserInfo(workspaceNamespace: String, userEmail: RawlsUserEmail)(implicit executionContext: ExecutionContext) = {
    for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(workspaceNamespace, userEmail)
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
    } yield {
      petUserInfo
    }
  }

}
