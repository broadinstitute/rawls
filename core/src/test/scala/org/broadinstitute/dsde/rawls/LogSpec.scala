package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.ErrorReport
import org.scalatest.Assertions
import org.scalatest.freespec.AnyFreeSpec
import net.logstash.logback.argument.StructuredArguments
import spray.json._

class LogSpec extends AnyFreeSpec with Assertions with LazyLogging {

  "RawlsExceptionWithErrorReport" - {
    "should have a reasonable toString value" in {
      val errorReport = new ErrorReport(source = "someservice",
        message = "somemessage",
        Some(StatusCodes.NotFound),
        causes = List.empty,
        stackTrace = List.empty,
        exceptionClass = None
      );
      val rawlsExceptionWithErrorReport = new RawlsExceptionWithErrorReport(errorReport)

      logger.info("Call failed", StructuredArguments.raw("detail", rawlsExceptionWithErrorReport.toJson.compactPrint))

      assertResult(
        "org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport: ErrorReport(someservice,somemessage,Some(404 Not Found),List(),List(),None)"
      )(
        rawlsExceptionWithErrorReport.toString
      );
    }
  }
}
