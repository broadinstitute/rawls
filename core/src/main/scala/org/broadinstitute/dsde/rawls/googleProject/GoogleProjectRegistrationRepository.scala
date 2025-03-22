package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, GoogleProjectRegistration}
import slick.dbio.DBIO

import scala.concurrent.{ExecutionContext, Future}

class GoogleProjectRegistrationRepository(dataSource: SlickDataSource) {

  def registerGoogleProject(
    googleProjectRegistration: GoogleProjectRegistration
  )(implicit executionContext: ExecutionContext): Future[Option[GoogleProjectRegistration]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.googleProjectRegistrationQuery.findById(
        googleProjectRegistration.googleProjectId
      ) flatMap { existingGoogleProjectReg =>
        existingGoogleProjectReg match {
          case Some(existing) if existing.billingProjectId == googleProjectRegistration.billingProjectId =>
            DBIO.successful(None)
          case Some(existing) if existing.billingProjectId != googleProjectRegistration.billingProjectId =>
            DBIO.failed(
              new RawlsExceptionWithErrorReport(
                errorReport =
                  ErrorReport(StatusCodes.Conflict,
                              "This google project id is already registered with a different billing project."
                  )
              )
            )
          case _ =>
            dataAccess.googleProjectRegistrationQuery.create(googleProjectRegistration).map(Some(_))
        }
      }
    }

  def deleteGoogleProjectRegistration(id: GoogleProjectId): Future[Boolean] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.googleProjectRegistrationQuery.delete(id)
    }

  def getGoogleProjectRegistration(id: GoogleProjectId): Future[Option[GoogleProjectRegistration]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.googleProjectRegistrationQuery.findById(id)
    }

  def getGoogleProjectRegistrations(
    ids: Set[GoogleProjectId]
  ): Future[Seq[GoogleProjectRegistration]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.googleProjectRegistrationQuery.findByIds(ids)
    }
}
