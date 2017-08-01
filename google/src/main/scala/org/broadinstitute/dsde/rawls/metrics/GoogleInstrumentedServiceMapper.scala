package org.broadinstitute.dsde.rawls.metrics

import com.google.api.services.admin.directory.model.{Group, Member, Members}
import com.google.api.services.cloudbilling.model.{BillingAccount, ListBillingAccountsResponse, ProjectBillingInfo}
import com.google.api.services.genomics.model.{ListOperationsResponse, Operation}
import com.google.api.services.pubsub.model.{Empty => PubSubEmpty, _}
import com.google.api.services.storage.model._
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService._

import scala.annotation.implicitNotFound

/**
  * Typeclass which maps a Google request type (e.g. Topic, Bucket, etc) to a specific value of the
  * [[GoogleInstrumentedService]] enumeration.
  */
@implicitNotFound(msg = "Cannot map type ${A} to a GoogleInstrumentedService. Please provide one explicitly.")
sealed trait GoogleInstrumentedServiceMapper[A] {
  def service: GoogleInstrumentedService
}

object GoogleInstrumentedServiceMapper {
  def apply[A](svc: GoogleInstrumentedService): GoogleInstrumentedServiceMapper[A] = new GoogleInstrumentedServiceMapper[A] {
    override def service: GoogleInstrumentedService = svc
  }

  // Buckets

  implicit object BucketMapper extends GoogleInstrumentedServiceMapper[Bucket] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Buckets
  }

  implicit object BucketAccessControlMapper extends GoogleInstrumentedServiceMapper[BucketAccessControl] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Buckets
  }

  implicit object BucketAccessControlsMapper extends GoogleInstrumentedServiceMapper[BucketAccessControls] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Buckets
  }

  implicit object StorageObjectControlMapper extends GoogleInstrumentedServiceMapper[StorageObject] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Buckets
  }

  implicit object ObjectsMapper extends GoogleInstrumentedServiceMapper[Objects] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Buckets
  }

  // Google PubSub

  implicit object TopicMapper extends GoogleInstrumentedServiceMapper[Topic] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.PubSub
  }

  implicit object PubSubEmptyMapper extends GoogleInstrumentedServiceMapper[PubSubEmpty] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.PubSub
  }

  implicit object SubscriptionMapper extends GoogleInstrumentedServiceMapper[Subscription] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.PubSub
  }

  implicit object PublishResponseMapper extends GoogleInstrumentedServiceMapper[PublishResponse] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.PubSub
  }

  implicit object PullResponseMapper extends GoogleInstrumentedServiceMapper[PullResponse] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.PubSub
  }

  implicit object PolicySubsystem extends GoogleInstrumentedServiceMapper[Policy] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.PubSub
  }

  // Google Groups

  implicit object MemberMapper extends GoogleInstrumentedServiceMapper[Member] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Groups
  }

  implicit object MembersMapper extends GoogleInstrumentedServiceMapper[Members] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Groups
  }

  implicit object GroupMapper extends GoogleInstrumentedServiceMapper[Group] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Groups
  }

  // Google Billing

  implicit object BillingAccountMapper extends GoogleInstrumentedServiceMapper[BillingAccount] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Billing
  }

  implicit object ListBillingAccountsResponseMapper extends GoogleInstrumentedServiceMapper[ListBillingAccountsResponse] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Billing
  }

  implicit object ProjectBillingInfoMapper extends GoogleInstrumentedServiceMapper[ProjectBillingInfo] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Billing
  }

  // Google Genomics

  implicit object GenomicsOperationMapper extends GoogleInstrumentedServiceMapper[Operation] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Genomics
  }

  implicit object ListOperationsResponseMapper extends GoogleInstrumentedServiceMapper[ListOperationsResponse] {
    override def service: GoogleInstrumentedService = GoogleInstrumentedService.Genomics
  }
}