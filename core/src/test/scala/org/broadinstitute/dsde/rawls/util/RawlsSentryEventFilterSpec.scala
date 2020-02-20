package org.broadinstitute.dsde.rawls.util

import io.sentry.event.{Event, EventBuilder}
import org.scalatest.FreeSpec

class RawlsSentryEventFilterSpec extends FreeSpec {

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

    "should NOT filter any other messages" in {
      val e = evt("This is some other error message that should be sent to Sentry")
      assertResult(true) {
        new RawlsSentryEventFilter().shouldSend(e)
      }
    }

  }

}
