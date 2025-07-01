package org.broadinstitute.dsde.rawls.billing

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsRequestContext, SamResourceTypeNames}

import scala.concurrent.{ExecutionContext, Future}

class BillingProjectDeletion(
  val samDAO: SamDAO,
  val billingRepository: BillingRepository,
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging {

  /**
    * Delete the billing project and associated billing profile.
    *
    * @param projectName            the Rawls billing project name
    * @param ctx                    the Rawls request context
    */
  def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] = unregisterBillingProject(projectName, ctx)

  // This code also lives in UserService as unregisterBillingProjectWithUserInfo
  // if this was scala 3.x, we could just use a parameterized trait, and this would work basically everywhere
  def unregisterBillingProject(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] =
    for {
      _ <- billingRepository.deleteBillingProject(projectName)
      _ <- samDAO
        .deleteResource(SamResourceTypeNames.billingProject,
                        projectName.value,
                        ctx
        ) recoverWith { // Moving this to the end so that the rawls record is cleared even if there are issues clearing the Sam resource (theoretical workaround for https://broadworkbench.atlassian.net/browse/CA-1206)
        case t: Throwable =>
          logger.warn(
            s"Unexpected failure deleting billing project (while deleting billing project in Sam) for billing project `${projectName.value}`",
            t
          )
          throw t
      }
    } yield {}
}
