package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import scala.concurrent.{ExecutionContext, Future}

trait RawlsSAContextCreator {

  val samDAO: SamDAO

  def getRawlsSAContext()(implicit executionContext: ExecutionContext): Future[RawlsRequestContext] = Future(
    samDAO.rawlsSAContext
  )

}
