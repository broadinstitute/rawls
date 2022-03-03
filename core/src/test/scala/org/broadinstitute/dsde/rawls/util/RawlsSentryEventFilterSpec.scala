package org.broadinstitute.dsde.rawls.util

import io.sentry.event.{Event, EventBuilder}
import org.scalatest.freespec.AnyFreeSpec

class RawlsSentryEventFilterSpec extends AnyFreeSpec {

  private def evt(msg: String): Event = {
    new EventBuilder().withMessage(msg)
        .build()
  }

  "RawlsSentryEventFilter" - {

    // https://github.com/slick/slick/issues/1856
    "should filter out Slick spam" in {
      val e = evt("requirement failed: count cannot be decreased")
      assertResult(false) {
        new RawlsSentryEventFilter().shouldSend(e)
      }
    }

    // https://broadworkbench.atlassian.net/browse/BW-1125
    "should filter out 'pet service account not found' errors" in {
      val e = evt("org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport: ErrorReport(rawls,HTTP error calling URI " +
        "https://sam/api/google/petServiceAccount/abc@terra.iam.gserviceaccount.com. Response: {\"causes\":[]," +
        "\"message\":\"pet service account not found\",\"source\":\"sam\",\"stackTrace\":[],\"statusCode\":404}," +
        "Some(404 Not Found),List(),List(),None)")
      assertResult(false) {
        new RawlsSentryEventFilter().shouldSend(e)
      }
    }

    "should NOT filter any other messages" in {
      val e = evt("This is some other error message that should be sent to Sentry")
      assertResult(true) {
        new RawlsSentryEventFilter().shouldSend(e)
      }
    }

  }

}
