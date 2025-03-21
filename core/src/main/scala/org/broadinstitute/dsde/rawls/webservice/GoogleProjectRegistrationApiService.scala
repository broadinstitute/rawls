package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.googleProject.GoogleProjectRegistrationService
import org.broadinstitute.dsde.rawls.model.GoogleProjectRegistrationJsonSupport$._
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  GoogleProjectRegistration,
  RawlsBillingProjectName,
  RawlsRequestContext
}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives

import scala.concurrent.ExecutionContext

trait GoogleProjectRegistrationApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val googleProjectRegServiceConstructor: RawlsRequestContext => GoogleProjectRegistrationService

  def googleProjectRegistrationRoutes(otelContext: Context = Context.root()): server.Route =
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      pathPrefix("googleProjects") {
        pathEndOrSingleSlash {
          get {
            parameters(
              "billingProjectId".as[String]
            ) { billingProjectId =>
              complete {
                googleProjectRegServiceConstructor(ctx)
                  .getGoogleProjects(Option(RawlsBillingProjectName(billingProjectId)))
                  .map {
                    case Some(googleProjectsResponse) => StatusCodes.OK -> Option(googleProjectsResponse)
                    case None                         => StatusCodes.NotFound -> None
                  }
              }
            }
          }
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
            get {
              complete {
                googleProjectRegServiceConstructor(ctx).getBillingProjectById(GoogleProjectId(googleProjectId)).map {
                  case Some(googleProjectResponse) => StatusCodes.OK -> Option(googleProjectResponse)
                  case None                        => StatusCodes.NotFound -> None
                }
              }
            }

          }
      }
    }
}
