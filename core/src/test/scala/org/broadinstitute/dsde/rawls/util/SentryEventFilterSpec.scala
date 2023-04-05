package org.broadinstitute.dsde.rawls.util

import com.google.cloud.storage.StorageException
import io.sentry.SentryEvent
import io.sentry.protocol.Message
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SentryEventFilterSpec extends AnyFlatSpec with Matchers {
  private def evt(msg: Option[String]): SentryEvent = {
    val event = new SentryEvent()

    msg foreach { m =>
      val message = new Message()
      message.setMessage(m)
      event.setMessage(message)
    }

    event
  }

  val requesterPaysMessage = "Bucket is a requester pays bucket but no user project provided"
  val billingDisabledMessage = "The billing account for the owning project is disabled in state absent"
  val httpGoogleServicesDao = "org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO"
  val rawlsApiService = "org.broadinstitute.dsde.rawls.webservice.RawlsApiService$"

  behavior of "SentryEventFilter"

  it should "filter out slick spam" in {
    val e = evt(Some("requirement failed: count cannot be decreased"))
    val result = SentryEventFilter.filterEvent(e)

    result shouldBe null
  }

  it should "filter out 'pet service account not found' errors" in {
    val msg = "org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport: ErrorReport(rawls,HTTP error calling URI " +
      "https://sam/api/google/petServiceAccount/abc@terra.iam.gserviceaccount.com. Response: {\"causes\":[]," +
      "\"message\":\"pet service account not found\",\"source\":\"sam\",\"stackTrace\":[],\"statusCode\":404}," +
      "Some(404 Not Found),List(),List(),None)"
    val e = evt(Some(msg))

    val result = SentryEventFilter.filterEvent(e)

    result shouldBe null
  }

  it should "filter out 'requester pays bucket' errors from direct HttpGoogleServicesDAO logging" in {
    val throwable = new StorageException(400, requesterPaysMessage)
    val e = new SentryEvent(throwable)
    e.setLogger(httpGoogleServicesDao)

    SentryEventFilter.filterEvent(e) shouldBe null
  }

  it should "not filter out 'requester pays bucket' errors from other loggers" in {
    val throwable = new StorageException(400, requesterPaysMessage)
    val e = evt(Some(requesterPaysMessage))
    e.setLogger(rawlsApiService)
    // In reality will have only the message and no throwable if from RawlsApiService,
    // but check that no filtering happens based on either field.
    e.setThrowable(throwable)

    SentryEventFilter.filterEvent(e) shouldBe e
  }

  it should "filter out 'billing disabled' errors from direct HttpGoogleServicesDAO logging" in {
    val throwable = new StorageException(403, billingDisabledMessage)
    val e = new SentryEvent(throwable)
    e.setLogger(httpGoogleServicesDao)

    SentryEventFilter.filterEvent(e) shouldBe null
  }

  it should "not filter out 'billing disabled' errors from other loggers" in {
    val throwable = new StorageException(403, billingDisabledMessage)
    val e = evt(Some(requesterPaysMessage))
    e.setLogger(rawlsApiService)
    // In reality will have only the message and no throwable if from RawlsApiService,
    // but check that no filtering happens based on either field.
    e.setThrowable(throwable)

    SentryEventFilter.filterEvent(e) shouldBe e
  }

  it should "not filter out other events" in {
    val e = evt(Some("other event"))

    val result = SentryEventFilter.filterEvent(e)

    result should not be null
  }

  it should "pass through events with no message" in {
    val e = evt(None)

    val result = SentryEventFilter.filterEvent(e)

    result should not be null
  }
}
