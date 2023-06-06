package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsUserEmail}

import scala.concurrent.ExecutionContext

trait AuthUtil {
  val samDAO: SamDAO
  val googleServicesDAO: GoogleServicesDAO

  def getPetServiceAccountUserInfo(googleProjectId: GoogleProjectId, userEmail: RawlsUserEmail)(implicit
    executionContext: ExecutionContext
  ) =
    for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(googleProjectId, userEmail)
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
    } yield petUserInfo

}
