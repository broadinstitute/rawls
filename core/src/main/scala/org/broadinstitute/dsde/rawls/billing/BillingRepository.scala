package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, RawlsBillingProjectName}

import scala.concurrent.Future

/**
 * Data access for rawls billing projects
 */
class BillingRepository(dataSource: SlickDataSource) {

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
