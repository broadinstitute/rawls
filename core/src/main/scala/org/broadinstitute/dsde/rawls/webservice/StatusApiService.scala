package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.status.StatusService

import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 5/20/17.
  */
trait StatusApiService {
  implicit val executionContext: ExecutionContext
  val statusServiceConstructor: () => StatusService

  val statusRoute: server.Route =
    path("status") {
      get {
        complete {
          statusServiceConstructor().getStatus.map { case (httpStatus, statusCheckResponse) =>
            httpStatus -> statusCheckResponse
          }
        }
      }
    }

}
