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
  SamResourceTypeNames
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
    // Retrieve accessible Google projects for the user
    val accessibleGoogleProjectsFuture = samDAO
      .listUserResources(SamResourceTypeNames.googleProject, ctx)
      .map(_.map(resource => GoogleProjectId(resource.resourceId)).toSet)

    accessibleGoogleProjectsFuture.flatMap { accessibleGoogleProjects =>
      // Fetch registrations for accessible projects
      googleProjectRegRepo.getGoogleProjectRegistrations(accessibleGoogleProjects).map { registrations =>
        // Filter registrations by billing project name if provided
        val filteredRegistrations = billingProjectName
          .map(name => registrations.filter(_.billingProjectId == name))
          .getOrElse(registrations)
        // If billing project name is specified and no registrations match, return empty sequence
        if (billingProjectName.isDefined && filteredRegistrations.isEmpty) {
          Seq.empty
        } else {
          // Combine registered and unregistered projects
          val registeredProjectIds = filteredRegistrations.map(_.googleProjectId).toSet
          val unregisteredProjects = accessibleGoogleProjects.diff(registeredProjectIds).map { projectId =>
            GoogleProjectRegistration(
              googleProjectId = projectId,
              billingAccount = None,
              message = None,
              billingProjectId = RawlsBillingProjectName("")
            )
          }
          // Return the combined result
          filteredRegistrations ++ unregisteredProjects
        }
      }
    }
  }

  def getGoogleProjectById(googleProjectId: GoogleProjectId): Future[Option[GoogleProjectRegistration]] =
    // Check if the user has the readPolicies action on the specified Google project
    samDAO
      .userHasAction(
        SamResourceTypeNames.googleProject,
        googleProjectId.value,
        SamGoogleProjectActions.readPolicies,
        ctx
      )
      .flatMap { hasAccess =>
        if (hasAccess) {
          // If the user has access, fetch the Google project registration
          googleProjectRegRepo.getGoogleProjectRegistration(googleProjectId).map {
            case Some(project) => Some(project)
            case None          =>
              // If no registration is found, create a default GoogleProjectRegistration
              Some(
                GoogleProjectRegistration(
                  googleProjectId = googleProjectId,
                  billingAccount = None,
                  message = None,
                  billingProjectId = RawlsBillingProjectName("")
                )
              )
          }
        } else {
          // If the user does not have access, return None
          Future.successful(None)
        }
      }
}
