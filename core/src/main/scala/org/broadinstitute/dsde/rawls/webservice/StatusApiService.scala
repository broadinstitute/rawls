package org.broadinstitute.dsde.rawls.webservice

import akka.actor.ActorRef
import org.broadinstitute.dsde.rawls.status.StatusService
import spray.routing.{HttpService, Route}

/**
  * Created by rtitle on 5/20/17.
  */
trait StatusApiService extends HttpService with PerRequestCreator {
  val statusServiceConstructor: () => StatusService

  val statusRoute: Route = {
    path("status") {
      get {
        requestContext => perRequest(requestContext,
          StatusService.props(statusServiceConstructor),
          StatusService.GetStatus)
      }
    }
  }

}
