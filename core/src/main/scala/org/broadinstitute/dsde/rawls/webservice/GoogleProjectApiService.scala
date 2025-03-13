package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.googleProject.GoogleProjectService
import org.broadinstitute.dsde.rawls.model.GoogleProjectJsonSupport._
import org.broadinstitute.dsde.rawls.model.{RawlsGoogleProject, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives

import scala.concurrent.ExecutionContext

trait GoogleProjectApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val googleProjectServiceConstructor: RawlsRequestContext => GoogleProjectService

  def googleProjectRoutes(otelContext: Context = Context.root()): server.Route =
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      path("googleProjects") {
        put {
          entity(as[RawlsGoogleProject]) { entity =>
            complete {
              googleProjectServiceConstructor(ctx)
                .createGoogleProject(
                  entity
                )
                .map(w => StatusCodes.Created -> w)
            }
          }
        }
      }
    }

}
