package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  GoogleProjectId,
  GoogleProjectRegistration,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamGoogleProjectActions,
  SamResourceTypeNames,
  UnRegisteredGoogleProjectRegistration
}

import scala.concurrent.{ExecutionContext, Future}

object GoogleProjectRegistrationService {
  def constructor(
    samDAO: SamDAO,
    googleProjectRegRepository: GoogleProjectRegistrationRepository,
    billingRepository: BillingRepository,
    googleServicesDAO: GoogleServicesDAO
  )(
    ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): GoogleProjectRegistrationService =
    new GoogleProjectRegistrationService(ctx, samDAO, googleProjectRegRepository, billingRepository, googleServicesDAO)

}

class GoogleProjectRegistrationService(protected val ctx: RawlsRequestContext,
                                       val samDAO: SamDAO,
                                       val googleProjectRegRepo: GoogleProjectRegistrationRepository,
                                       val billingRepository: BillingRepository,
                                       val googleServicesDAO: GoogleServicesDAO
)(implicit
  protected val executionContext: ExecutionContext
) {

  def registerGoogleProject(googleProjectReg: GoogleProjectRegistration): Future[Option[GoogleProjectRegistration]] =
    for {
      billingProject <- billingRepository.getBillingProject(googleProjectReg.billingProjectId).map {
        maybeBillingProject =>
          maybeBillingProject.getOrElse(
            throw new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(
                StatusCodes.NotFound,
                "Billing project does not exist or you do not have permission to perform this action."
              )
            )
          )
      }
      _ <- samDAO
        .listUserActionsForResource(SamResourceTypeNames.billingProject, billingProject.projectName.value, ctx)
        .map { actions =>
          if (actions.isEmpty)
            throw new RawlsExceptionWithErrorReport(
              errorReport =
                ErrorReport(StatusCodes.NotFound,
                            "Billing project does not exist or you do not have permission to perform this action."
                )
            )
          else if (!actions.contains(SamBillingProjectActions.link)) {
            throw new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
            )
          }
        }
      canLinkGoogleProject <- samDAO
        .userHasAction(SamResourceTypeNames.googleProject,
                       googleProjectReg.googleProjectId.value,
                       SamGoogleProjectActions.link,
                       ctx
        )
      _ = if (!canLinkGoogleProject) {
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(
            StatusCodes.Forbidden,
            "Google project does not exist or you do not have permission to perform this action."
          )
        )
      }
      result <- googleProjectRegRepo.registerGoogleProject(
        googleProjectReg.copy(billingAccount = billingProject.billingAccount)
      )
      finalResult <- result match {
        case Some(project) =>
          googleServicesDAO
            .setBillingAccountName(googleProjectReg.googleProjectId,
                                   billingProject.billingAccount.get,
                                   ctx.toTracingContext
            )
            .map(_ => Some(project))
            .recoverWith { case ex: RawlsExceptionWithErrorReport =>
              googleProjectRegRepo
                .deleteGoogleProjectRegistration(googleProjectReg.googleProjectId)
                .flatMap(_ =>
                  Future.failed(
                    new RawlsExceptionWithErrorReport(
                      errorReport = ErrorReport(StatusCodes.InternalServerError,
                                                s"Failed to set billing account in Google: ${ex.errorReport.message}"
                      )
                    )
                  )
                )
            }
        case None => Future.successful(None)
      }
    } yield finalResult

  def getGoogleProjects(billingProjectName: Option[RawlsBillingProjectName]): Future[Seq[GoogleProjectRegistration]] = {
    val accessibleGoogleProjectsFuture = samDAO
      .listResourcesWithActions(SamResourceTypeNames.googleProject, SamGoogleProjectActions.readPolicies, ctx)
      .map(_.map(resource => GoogleProjectId(resource.getResourceId)).toSet)

    accessibleGoogleProjectsFuture.flatMap { accessibleGoogleProjects =>
      googleProjectRegRepo.getGoogleProjectRegistrations(accessibleGoogleProjects).map { registrations =>
        val filteredRegistrations = billingProjectName
          .map(name => registrations.filter(_.billingProjectId == name))
          .getOrElse(registrations)

        if (billingProjectName.isDefined && filteredRegistrations.isEmpty) {
          Seq.empty
        } else {
          val registeredProjectIds = filteredRegistrations.map(_.googleProjectId).toSet
          val unregisteredProjects =
            accessibleGoogleProjects.diff(registeredProjectIds).map(UnRegisteredGoogleProjectRegistration.apply)

          filteredRegistrations ++ unregisteredProjects
        }
      }
    }
  }

  def getGoogleProjectById(googleProjectId: GoogleProjectId): Future[Option[GoogleProjectRegistration]] =
    samDAO
      .userHasAction(SamResourceTypeNames.googleProject,
                     googleProjectId.value,
                     SamGoogleProjectActions.readPolicies,
                     ctx
      )
      .flatMap {
        case true =>
          googleProjectRegRepo.getGoogleProjectRegistration(googleProjectId).map {
            case Some(project) => Some(project)
            case None          => Some(UnRegisteredGoogleProjectRegistration(googleProjectId))
          }
        case false => Future.successful(None)
      }
}
