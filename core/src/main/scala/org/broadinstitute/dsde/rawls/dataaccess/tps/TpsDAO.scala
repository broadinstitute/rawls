package org.broadinstitute.dsde.rawls.dataaccess.tps

import bio.terra.policy.model.TpsPaoCreateRequest
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import scala.concurrent.Future

trait TpsDAO {
  def createPao(request: TpsPaoCreateRequest, ctx: RawlsRequestContext): Future[Unit]
}
