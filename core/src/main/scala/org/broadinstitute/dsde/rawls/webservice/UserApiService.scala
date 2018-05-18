package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import CustomDirectives._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait UserApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

  val userServiceConstructor: UserInfo => UserService

  // these routes have different access control and their paths have /register instead of /api

  val createUserRoute: server.Route = requireUserInfo() { userInfo =>
    path("user") {
      addLocationHeader(s"/user/${userInfo.userSubjectId.value}") {
        post {
          complete { userServiceConstructor(userInfo).CreateUser.map(_ => StatusCodes.Created) }
        }
      }
    }
  }

  // standard /api routes begin here

  val userRoutes: server.Route = requireUserInfo() { userInfo =>
    path("user" / "refreshToken") {
      put {
        entity(as[UserRefreshToken]) { token =>
          complete { userServiceConstructor(userInfo).SetRefreshToken(token) }
        }
      }
    } ~
      path("user" / "refreshTokenDate") {
        get {
          complete { userServiceConstructor(userInfo).GetRefreshTokenDate }
        }
      } ~
      path("user" / "billing") {
        get {
          complete { userServiceConstructor(userInfo).ListBillingProjects }
        }
      } ~
      path("user" / "role" / "admin") {
        get {
          complete { userServiceConstructor(userInfo).IsAdmin(userInfo.userEmail) }
        }
      } ~
      path("user" / "role" / "curator") {
        get {
          complete { userServiceConstructor(userInfo).IsLibraryCurator(userInfo.userEmail) }
        }
      } ~
      path("user" / "billingAccounts") {
        get {
          complete { userServiceConstructor(userInfo).ListBillingAccounts }
        }
      } ~
      path("user" / "groups") {
        get {
          complete { userServiceConstructor(userInfo).ListGroupsForUser(userInfo.userEmail) }
        }
      } ~
      path("user" / "group" / Segment) { groupName =>
        get {
          complete { userServiceConstructor(userInfo).GetUserGroup(RawlsGroupRef(RawlsGroupName(groupName))) }
        }
      } ~
      pathPrefix("groups") {
        pathEnd {
          get {
            complete { userServiceConstructor(userInfo).ListManagedGroupsForUser }
          }
        } ~
          pathPrefix(Segment) { groupName =>
            val groupRef = ManagedGroupRef(RawlsGroupName(groupName))
            path("requestAccess") {
              post {
                complete { userServiceConstructor(userInfo).RequestAccessToManagedGroup(groupRef) }
              }
            } ~
              pathEnd {
                get {
                  complete { userServiceConstructor(userInfo).GetManagedGroup(groupRef) }
                } ~
                  post {
                    complete { userServiceConstructor(userInfo).CreateManagedGroup(groupRef) }
                  } ~
                  delete {
                    complete { userServiceConstructor(userInfo).DeleteManagedGroup(groupRef) }
                  }
              } ~
              path(Segment) { role =>
                put {
                  entity(as[RawlsGroupMemberList]) { memberList =>
                    complete {
                      userServiceConstructor(userInfo).OverwriteManagedGroupMembers(groupRef, ManagedRoles.withName(role), memberList)
                    }
                  }
                }
              } ~
              path(Segment / Segment) { (role, email) =>
                put {
                  complete { userServiceConstructor(userInfo).AddManagedGroupMembers(groupRef, ManagedRoles.withName(role), email) }

                } ~
                  delete {
                    complete { userServiceConstructor(userInfo).RemoveManagedGroupMembers(groupRef, ManagedRoles.withName(role), email) }

                  }
              }
          }
      }
  }
}
