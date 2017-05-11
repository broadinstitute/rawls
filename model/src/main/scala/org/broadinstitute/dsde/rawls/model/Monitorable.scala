package org.broadinstitute.dsde.rawls.model

import scala.concurrent.Future

/**
  * Created by rtitle on 5/11/17.
  */
trait Monitorable {

  def systemName: String

  def test: Future[Unit]

}