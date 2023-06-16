package org.broadinstitute.dsde.rawls.consumer

import cats.effect.kernel.Concurrent
import cats.syntax.all._
import org.broadinstitute.dsde.rawls.consumer.Decoders._
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client

trait BpmClient[F[_]] {

  def fetchSystemStatus(): F[StatusCheckResponse]
}

/*
 This class represents the consumer (Rawls) view of the BPM provider that implements the following endpoints:
 - GET /status serviceStatus
 - GET /version serviceVersion -- NOT ACTUALLY USED!
 -
 - GET /api/azure/v1/managedApps createBillingProfile, args azureSubscriptionId: UUID, includeAssignedApplications: Boolean
 -
 - POST /api/profiles/v1/{profileId}/policies/{policyName}/members addProfilePolicyMember, PolicyMemberRequest body, UUID profileId, String policyName
 _ POST /api/profiles/v1 createProfile CreateProfileRequest body
 - DELETE /api/profiles/v1/{profileId} deleteProfile UUID profileId
 - DELETE /api/resources/v1/profiles/{profileId}/policies/{policyName}/members/{memberEmail} deleteProfilePolicyMember profileId: UUID, policyName: String, memberEmail: String
 - GET "/api/profiles/v1/{profileId} getProfile UUID profileId
 - GET /api/profiles/v1/{profileId}/policies  getProfilePolicies UUID profileId
 - GET /api/profiles/v1 listProfiles offset: Integer, limit: Integer
 - PATCH /api/profiles/v1/{profileId} updateProfile UpdateProfileRequest body, UUID profileId
 -
 - GET /api/profiles/v1/{profileId}/spendReport getSpendReport profileId: UUID, spendReportStartDate: Date, spendReportEndDate: Date
 -
 */
class BpmClientImpl[F[_]: Concurrent](client: Client[F], baseUrl: Uri) extends BpmClient[F] {

  override def fetchSystemStatus()
  : F[StatusCheckResponse] = {
    val request = Request[F](uri = baseUrl / "status").withHeaders(
      org.http4s.headers.Accept(MediaType.application.json)
    )
    client.run(request).use { resp =>
      resp.status match {
        case Status.Ok                  => resp.as[StatusCheckResponse]
        case Status.InternalServerError => resp.as[StatusCheckResponse]
        case _                          => UnknownError.raiseError
      }
    }
  }
}
