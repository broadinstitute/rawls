package org.broadinstitute.dsde.rawls.fastpass

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO

import scala.concurrent.ExecutionContext

object FastPassService {
  def constructor(dataSource: SlickDataSource,
                  googleIamDao: GoogleIamDAO,
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
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      workbenchMetricBaseName
    )
}

class FastPassService(protected val ctx: RawlsRequestContext,
                      val dataSource: SlickDataSource,
                      val googleIamDao: GoogleIamDAO,
                      val terraBillingProjectOwnerRole: String,
                      val terraWorkspaceCanComputeRole: String,
                      val terraWorkspaceNextflowRole: String,
                      val terraBucketReaderRole: String,
                      val terraBucketWriterRole: String,
                      override val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends LazyLogging
    with RawlsInstrumented {

  import dataSource.dataAccess.driver.api._

}
