package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, RawlsBillingProjectName}

import java.util.UUID
import scala.concurrent.Future

/**
 * Data access for rawls billing projects
 */
class BillingRepository(dataSource: SlickDataSource) {

  def setBillingProfileId(projectName: RawlsBillingProjectName, billingProfileId: UUID) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.updateBillingProfileId(projectName, Some(billingProfileId))
    }
  }

  def createBillingProject(billingProject: RawlsBillingProject): Future[RawlsBillingProject] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.create(billingProject)
    }
  }

  def getBillingProject(projectName: RawlsBillingProjectName): Future[Option[RawlsBillingProject]] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.load(projectName)
    }
  }
}
