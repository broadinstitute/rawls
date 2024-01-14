package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{
  GoogleBillingProjectDelete,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport,
  ErrorReportSource,
  RawlsBillingProjectName,
  RawlsRequestContext
}
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService.{
  deleteGoogleProjectIfChild,
  syncBillingProjectOwnerPolicyToGoogleAndGetEmail
}

import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}

class GoogleBillingProjectLifecycle(
  val billingRepository: BillingRepository,
  val billingProfileManagerDAO: BillingProfileManagerDAO,
  val samDAO: SamDAO,
  gcsDAO: GoogleServicesDAO
)(implicit
  executionContext: ExecutionContext
) extends BillingProjectLifecycle {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  override val deleteJobType: JobType = GoogleBillingProjectDelete

  /**
   * Validates that the desired billing account has granted Terra proper access as well as any needed service
   * perimeter access.
   * @return A successful future in the event of a passed validation, a failed future with an Exception in the event of
   *         validation failure.
   */
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                                     ctx: RawlsRequestContext
  ): Future[Unit] =
    for {
      _ <- ServicePerimeterService.checkServicePerimeterAccess(samDAO, createProjectRequest.servicePerimeter, ctx)
      hasAccess <- gcsDAO.testTerraAndUserBillingAccountAccess(createProjectRequest.billingAccount.get, ctx.userInfo)
      _ = if (!hasAccess) {
        throw new GoogleBillingAccountAccessException(
          ErrorReport(StatusCodes.BadRequest,
                      "Billing account does not exist, user does not have access, or Terra does not have access"
          )
        )
      }
    } yield {}

  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                 config: MultiCloudWorkspaceConfig,
                                 ctx: RawlsRequestContext
  ): Future[CreationStatus] = {
    val projectName = createProjectRequest.projectName

    // This is where we create a billing profile in the BPM case.
    def createBillingProfile: Future[ProfileModel] =
      Future(blocking {
        val policies: Map[String, List[(String, String)]] =
          if (createProjectRequest.protectedData.getOrElse(false)) Map("protected-data" -> List[(String, String)]())
          else Map.empty
        val profileModel = billingProfileManagerDAO.createBillingProfile(
          projectName.value,
          createProjectRequest.billingInfo,
          policies,
          ctx
        )
        logger.info(
          s"Creating BPM-backed billing project ${projectName.value}, created profile with ID ${profileModel.getId}."
        )
        profileModel
      })

    def addMembersToBillingProfile(profileModel: ProfileModel): Future[Set[Unit]] = {
      val members = createProjectRequest.members.getOrElse(Set.empty)
      Future.traverse(members) { member =>
        Future(blocking {
          billingProfileManagerDAO.addProfilePolicyMember(profileModel.getId,
                                                          ProfilePolicy.fromProjectRole(member.role),
                                                          member.email,
                                                          ctx
          )
        })
      }
    }

    for {
      - <- createBillingProfile.flatMap { profileModel =>
        addMembersToBillingProfile(profileModel)
      }
      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield CreationStatuses.Ready
  }

  override def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Option[UUID]] =
    // Note: GoogleBillingProjectLifecycleSpec does not test that this method is called because the method
    // lives in a companion object (which makes straight mocking impossible), and the method will be removed
    // once workspace migration is complete. Note also that the more "integration" level test BillingApiServiceV2Spec
    // does verify that code in this method is executed when a Google-based project is deleted.
    deleteGoogleProjectIfChild(projectName, ctx.userInfo, gcsDAO, samDAO, ctx).map(_ => None)

  override def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] =
    unregisterBillingProject(projectName, ctx)

}
