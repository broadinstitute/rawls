package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.status.StatusService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._

import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 5/20/17.
  */
trait StatusApiService {
  implicit val executionContext: ExecutionContext
  import PerRequest.requestCompleteMarshaller
  val statusServiceConstructor: () => StatusService

  val statusRoute: server.Route = {
    path("status") {
      get {
        complete { statusServiceConstructor().getStatus }
      }
    }
  }

}
