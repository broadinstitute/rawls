package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectRegistration}

import scala.concurrent.Future

class GoogleProjectRegistrationRepository(dataSource: SlickDataSource) {

  def registerGoogleProject(googleProjectRegistration: GoogleProjectRegistration): Future[GoogleProjectRegistration] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.googleProjectRegistrationQuery.create(googleProjectRegistration)
    }

  def getGoogleProjectRegistration(id: GoogleProjectId): Future[Option[GoogleProjectRegistration]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.googleProjectRegistrationQuery.findById(id)
    }
}
