package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import bio.terra.workspace.client.{ApiException => WsmApiException}
import org.broadinstitute.dsde.workbench.client.leonardo.{ApiException => LeoApiException}

object DeletionAction {
  def when500(throwable: Throwable): Boolean =
    throwable match {
      case t: WsmApiException => t.getCode / 100 == 5
      case t: LeoApiException => t.getCode / 100 == 5
      case _ => false
    }
}
