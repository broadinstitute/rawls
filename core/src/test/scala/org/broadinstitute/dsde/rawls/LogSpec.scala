package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.StatusCodes
import ch.qos.logback.core.OutputStreamAppender
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.ErrorReport
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, spy}
import org.scalatest.Assertions
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

class LogSpec extends AnyFreeSpec with Assertions with LazyLogging {

  // @param loggerName name of the logger to spy on, as specified in logback.xml
  // @param appenderName name of the appender to spy on, as specified in logback.xml
  // @return a buffer that will accumulate messages logged to the given appender of the given logger
  def spyOnAppender(loggerName: String, appenderName: String): ListBuffer[String] = {
    val rootLogger =
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]

    val consoleAppender = rootLogger.getAppender("console").asInstanceOf[OutputStreamAppender[?]]
    // swap in our spy for the output stream used by the console appender, so we can watch its invocations
    val outputSpy = spy(consoleAppender.getOutputStream)

    val loggedMessages = new ListBuffer[String]()
    // spy on the bytes written, and record them as Strings for easy assertion later on
    doAnswer { invocation =>
      val actualBytesWritten = invocation.getArguments.head.asInstanceOf[Array[Byte]]
      loggedMessages += actualBytesWritten.map(_.toChar).mkString
    }.when(outputSpy).write(ArgumentMatchers.any[Array[Byte]])
    consoleAppender.setOutputStream(outputSpy)

    loggedMessages
  }

  "RawlsExceptionWithErrorReport" - {
    "prints its toString value and the stackTrace when passed as the 2nd parameter to logger" in {
      val messageAccumulator = spyOnAppender(org.slf4j.Logger.ROOT_LOGGER_NAME, "console")
      val errorReport = new ErrorReport(source = "someservice",
                                        message = "somemessage",
                                        Some(StatusCodes.NotFound),
                                        causes = List.empty,
                                        stackTrace = List.empty,
                                        exceptionClass = None
      );
      val rawlsExceptionWithErrorReport = new RawlsExceptionWithErrorReport(errorReport)

      logger.info("Call failed", rawlsExceptionWithErrorReport)

      messageAccumulator.size shouldEqual 1 // only one message should be logged
      val loggedMessage = messageAccumulator.head
      loggedMessage should include(
        "ErrorReport(someservice,somemessage,Some(404 Not Found),List(),List(),None)" // the toString of the report
      )
      loggedMessage should include("\n\tat org.broadinstitute.dsde.rawls.LogSpec") // expect stacktrace frames
    }
  }
}
