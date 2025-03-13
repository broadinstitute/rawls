package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsGoogleProject}

import scala.concurrent.Future

class GoogleProjectRepository(dataSource: SlickDataSource) {

  def createGoogleProject(googleProject: RawlsGoogleProject): Future[RawlsGoogleProject] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGoogleProjectQuery.create(googleProject)
    }

  def getGoogleProject(id: GoogleProjectId): Future[Option[RawlsGoogleProject]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGoogleProjectQuery.findById(id)
    }
}
