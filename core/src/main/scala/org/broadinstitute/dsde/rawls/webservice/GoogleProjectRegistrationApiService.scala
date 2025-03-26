package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.googleProject.GoogleProjectRegistrationService
import org.broadinstitute.dsde.rawls.model.GoogleProjectRegistrationJsonSupport$._
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectRegistration, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives

import scala.concurrent.ExecutionContext

trait GoogleProjectRegistrationApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val googleProjectRegServiceConstructor: RawlsRequestContext => GoogleProjectRegistrationService

  def googleProjectRegistrationRoutes(otelContext: Context = Context.root()): server.Route =
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      pathPrefix("googleProjects") {
        pathEnd {
          put {
            entity(as[GoogleProjectRegistration]) { entity =>
              complete {
                googleProjectRegServiceConstructor(ctx)
                  .registerGoogleProject(
                    entity
                  )
                  .map {
                    case None          => StatusCodes.OK -> None
                    case Some(project) => StatusCodes.Created -> Some(project)
                  }
              }
            }
          }
        } ~
          path(Segment) { googleProjectId =>
            delete {
              complete {
                googleProjectRegServiceConstructor(ctx)
                  .unregisterGoogleProject(GoogleProjectId(googleProjectId))
                  .map(_ => StatusCodes.NoContent)
              }
            }
          }
      }
    }

}
