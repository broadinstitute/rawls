package org.broadinstitute.dsde.rawls.dataaccess.tps

import bio.terra.policy.model.{TpsPaoCreateRequest, TpsPaoSourceRequest}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import java.util.UUID
import scala.concurrent.Future

trait TpsDAO {
  def createPao(request: TpsPaoCreateRequest, ctx: RawlsRequestContext): Future[Unit]

  def mergePao(request: TpsPaoSourceRequest, destPaoId: UUID, ctx: RawlsRequestContext): Future[Unit]
}
