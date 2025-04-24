package org.broadinstitute.dsde.rawls.dataaccess.tps

import bio.terra.policy.model.{TpsPaoCreateRequest, TpsPaoGetResult, TpsPaoSourceRequest}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import java.util.UUID
import scala.concurrent.Future

trait TpsDAO {
  def createPao(request: TpsPaoCreateRequest, ctx: RawlsRequestContext): Future[Unit]

  def mergePao(request: TpsPaoSourceRequest, objectId: UUID, ctx: RawlsRequestContext): Future[Unit]

  def getPao(objectId: UUID, ctx: RawlsRequestContext): Future[TpsPaoGetResult]

  def deletePao(objectId: UUID, ctx: RawlsRequestContext): Future[Unit]

  def linkPao(request: TpsPaoSourceRequest, objectId: UUID, ctx: RawlsRequestContext): Future[Unit]
}
