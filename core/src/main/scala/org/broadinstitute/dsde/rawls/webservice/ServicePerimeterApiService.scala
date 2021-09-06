package org.broadinstitute.dsde.rawls.webservice


import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._

import scala.concurrent.ExecutionContext

/**
  * Created with dvoet on 6/12/19.
  */

trait ServicePerimeterApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import PerRequest.requestCompleteMarshaller

  val userServiceConstructor: UserInfo => UserService
  val servicePerimeterRoutes: server.Route = requireUserInfo() { userInfo =>
    path("servicePerimeters" / Segment / "projects" / Segment) { (servicePerimeterName, projectId) =>
      put {
        complete {
          userServiceConstructor(userInfo).addProjectToServicePerimeter(ServicePerimeterName(URLDecoder.decode(servicePerimeterName, UTF_8.name)), RawlsBillingProjectName(projectId)) // todo: the `URLDecoder.decode` on this line decodes url-encoded chars. is this ok or could it lead to security issues, such as stored/persistent XSS?
        }
      }
    }
  }
}
