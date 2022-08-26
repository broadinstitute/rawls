package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opencensus.scala.akka.http.TracingDirective.traceRequest
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService

import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.ExecutionContext

/**
  * Created with dvoet on 6/12/19.
  */

trait ServicePerimeterApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val userServiceConstructor: RawlsRequestContext => UserService
  val servicePerimeterRoutes: server.Route = traceRequest { span =>
    requireUserInfo(Option(span)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(span))
      path("servicePerimeters" / Segment / "projects" / Segment) { (servicePerimeterName, projectId) =>
        put {
          complete {
            userServiceConstructor(ctx)
              .addProjectToServicePerimeter(ServicePerimeterName(URLDecoder.decode(servicePerimeterName, UTF_8.name)),
                                            RawlsBillingProjectName(projectId)
              )
              .map(_ => StatusCodes.NoContent)
          }
        }
      }
    }
  }
}
