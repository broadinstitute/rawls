package org.broadinstitute.dsde.rawls.fastpass

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

import scala.concurrent.{ExecutionContext, Future}

object FastPassService {
  def constructor(dataSource: SlickDataSource,
                  googleIamDao: GoogleIamDAO,
                  samDAO: SamDAO,
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  terraBucketReaderRole: String,
                  terraBucketWriterRole: String,
                  workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): FastPassService =
    new FastPassService(
      ctx,
      dataSource,
      googleIamDao,
      samDAO,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      workbenchMetricBaseName
    )

}

class FastPassService(protected val ctx: RawlsRequestContext,
                      protected val dataSource: SlickDataSource,
                      protected val googleIamDao: GoogleIamDAO,
                      protected val samDAO: SamDAO,
                      protected val terraBillingProjectOwnerRole: String,
                      protected val terraWorkspaceCanComputeRole: String,
                      protected val terraWorkspaceNextflowRole: String,
                      protected val terraBucketReaderRole: String,
                      protected val terraBucketWriterRole: String,
                      override val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends LazyLogging
    with RawlsInstrumented {

  import dataSource.dataAccess.driver.api._


  def addUserAndPetToProjectIamRole(googleProjectId: GoogleProjectId, organizationRole: String): Future[Unit] = {
    // Call Sam to get user's Pet
    // Add user and pet
    Future.successful()
  }
  def addUserAndPetToBucketIamRole(gcsBucketName: GcsBucketName, organizationRole: String): Future[Unit] = {
    // Call Sam to get user's Pet
    // Add user and pet to
    Future.successful()
  }

}
