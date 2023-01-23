package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import io.opencensus.scala.akka.http.TracingDirective.traceRequest
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProjectOrchestrator
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.user.UserService
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 11/2/2020.
  */

trait BillingApiServiceV2 extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.SpendReportingJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.json.DefaultJsonProtocol._

  val userServiceConstructor: RawlsRequestContext => UserService
  val spendReportingConstructor: RawlsRequestContext => SpendReportingService
  val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator

  implicit def aggregationKeyParameterUnmarshaller: Unmarshaller[String, SpendReportingAggregationKeyWithSub] =
    Unmarshaller.strict { parameter =>
      val delimitedParameter = parameter.split("~").toList

      delimitedParameter match {
        case key :: subKey :: Nil =>
          SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.withName(key),
                                              Option(SpendReportingAggregationKeys.withName(subKey))
          )
        case key :: Nil => SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.withName(key))
        case _ =>
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest,
                        s"Error parsing aggregation key(s). Found ${delimitedParameter.mkString(",")}"
            )
          )
      }
    }

  implicit def dateTimeUnmarshaller: Unmarshaller[String, DateTime] = Unmarshaller.strict(DateTime.parse)

  val billingRoutesV2: server.Route = traceRequest { span =>
    requireUserInfo(Option(span)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(span))
      pathPrefix("billing" / "v2") {

        pathPrefix(Segment) { projectId =>
          pathEnd {
            get {
              complete {
                import spray.json._
                userServiceConstructor(ctx).getBillingProject(RawlsBillingProjectName(projectId)).map {
                  case Some(projectResponse) => StatusCodes.OK -> Option(projectResponse).toJson
                  case None => StatusCodes.NotFound -> Option(StatusCodes.NotFound.defaultMessage).toJson
                }
              }
            } ~
              delete {
                complete {
                  billingProjectOrchestratorConstructor(ctx)
                    .deleteBillingProjectV2(RawlsBillingProjectName(projectId))
                    .map(_ => StatusCodes.NoContent)
                }
              }
          } ~
            pathPrefix("spendReport") {
              pathEndOrSingleSlash {
                get {
                  parameters(
                    "startDate".as[DateTime],
                    "endDate".as[DateTime],
                    "aggregationKey"
                      .as[SpendReportingAggregationKeyWithSub](aggregationKeyParameterUnmarshaller)
                      .repeated
                  ) { (startDate, endDate, aggregationKeyParameters) =>
                    complete {
                      spendReportingConstructor(ctx).getSpendForBillingProject(
                        RawlsBillingProjectName(projectId),
                        startDate,
                        endDate.plusDays(1).minusMillis(1),
                        aggregationKeyParameters.toSet
                      )
                    }
                  }
                }
              }
            } ~
            pathPrefix("spendReportConfiguration") {
              pathEnd {
                put {
                  entity(as[BillingProjectSpendConfiguration]) { spendConfiguration =>
                    complete {
                      userServiceConstructor(ctx)
                        .setBillingProjectSpendConfiguration(RawlsBillingProjectName(projectId), spendConfiguration)
                        .map(_ => StatusCodes.NoContent)
                    }
                  }
                } ~
                  delete {
                    complete {
                      userServiceConstructor(ctx)
                        .clearBillingProjectSpendConfiguration(RawlsBillingProjectName(projectId))
                        .map(_ => StatusCodes.NoContent)
                    }
                  } ~
                  get {
                    complete {
                      userServiceConstructor(ctx)
                        .getBillingProjectSpendConfiguration(RawlsBillingProjectName(projectId))
                        .map {
                          case Some(config) => StatusCodes.OK -> Option(config)
                          case None         => StatusCodes.NoContent -> None
                        }
                    }
                  }
              }
            } ~
            pathPrefix("billingAccount") {
              pathEnd {
                put {
                  entity(as[UpdateRawlsBillingAccountRequest]) { updateProjectRequest =>
                    complete {
                      userServiceConstructor(ctx)
                        .updateBillingProjectBillingAccount(RawlsBillingProjectName(projectId), updateProjectRequest)
                        .map {
                          case Some(billingProject) => StatusCodes.OK -> Option(billingProject)
                          case None                 => StatusCodes.NoContent -> None
                        }
                    }
                  }
                } ~
                  delete {
                    complete {
                      userServiceConstructor(ctx).deleteBillingAccount(RawlsBillingProjectName(projectId)).map {
                        case Some(billingProject) => StatusCodes.OK -> Option(billingProject)
                        case None                 => StatusCodes.NoContent -> None
                      }
                    }
                  }
              }
            } ~
            pathPrefix("members") {
              pathEnd {
                get {
                  complete {
                    userServiceConstructor(ctx).getBillingProjectMembers(RawlsBillingProjectName(projectId))
                  }
                } ~
                  patch {
                    parameter(Symbol("inviteUsersNotFound").?) { inviteUsersNotFound =>
                      entity(as[BatchProjectAccessUpdate]) { batchProjectAccessUpdate =>
                        complete {
                          userServiceConstructor(ctx)
                            .batchUpdateBillingProjectMembers(RawlsBillingProjectName(projectId),
                                                              batchProjectAccessUpdate,
                                                              inviteUsersNotFound.getOrElse("false").toBoolean
                            )
                            .map(_ => StatusCodes.NoContent -> None)
                        }
                      }
                    }
                  }
              } ~
                // these routes are for adding/removing users from projects
                path(Segment / Segment) { (workbenchRole, userEmail) =>
                  put {
                    complete {
                      userServiceConstructor(ctx)
                        .addUserToBillingProjectV2(RawlsBillingProjectName(projectId),
                                                   ProjectAccessUpdate(userEmail, ProjectRoles.withName(workbenchRole))
                        )
                        .map(_ => StatusCodes.OK)
                    }
                  } ~
                    delete {
                      complete {
                        userServiceConstructor(ctx)
                          .removeUserFromBillingProjectV2(RawlsBillingProjectName(projectId),
                                                          ProjectAccessUpdate(userEmail,
                                                                              ProjectRoles.withName(workbenchRole)
                                                          )
                          )
                          .map(_ => StatusCodes.OK)
                      }
                    }
                }
            }
        } ~
          pathEnd {
            get {
              complete {
                userServiceConstructor(ctx).listBillingProjectsV2()
              }
            } ~
              post {
                entity(as[CreateRawlsV2BillingProjectFullRequest]) { createProjectRequest =>
                  complete {
                    billingProjectOrchestratorConstructor(ctx)
                      .createBillingProjectV2(createProjectRequest)
                      .map(_ => StatusCodes.Created)
                  }
                }
              }
          }
      }
    }
  }
}
