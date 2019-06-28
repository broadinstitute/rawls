package org.broadinstitute.dsde.rawls.webservice


import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created with dvoet on 6/12/19.
  */

trait ServicePerimeterApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import PerRequest.requestCompleteMarshaller

  val userServiceConstructor: UserInfo => UserService
  val servicePerimeterRoutes: server.Route = requireUserInfo() { userInfo =>
    path("servicePerimeters" / Segment / "projects" / Segment) { (servicePerimeterName, projectId) =>
      put {
        complete {
          userServiceConstructor(userInfo).AddProjectToServicePerimeter(ServicePerimeterName(URLDecoder.decode(servicePerimeterName, UTF_8.name)), RawlsBillingProjectName(projectId))
        }
      }
    }
  }
}
