package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import bio.terra.workspace.client.{ApiException => WsmApiException}
import org.broadinstitute.dsde.workbench.client.leonardo.{ApiException => LeoApiException}

import javax.ws.rs.ProcessingException

object DeletionAction {
  def when500OrProcessingException(throwable: Throwable): Boolean =
    throwable match {
      case t: WsmApiException     => t.getCode / 100 == 5
      case t: LeoApiException     => t.getCode / 100 == 5
      case _: ProcessingException => true
      case _                      => false
    }
}
