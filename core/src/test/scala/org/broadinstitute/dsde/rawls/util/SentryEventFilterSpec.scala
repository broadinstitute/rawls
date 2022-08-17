package org.broadinstitute.dsde.rawls.util

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
