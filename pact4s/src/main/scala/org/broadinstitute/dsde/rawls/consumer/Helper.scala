package org.broadinstitute.dsde.rawls.consumer

import au.com.dius.pact.consumer.dsl.{DslPart, PactDslResponse, PactDslWithProvider}
import io.circe.{Decoder, KeyDecoder}
import org.broadinstitute.dsde.workbench.util.health.Subsystems.Subsystem
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus, Subsystems}

case object UnknownError extends Exception

object Decoders {

  val subsystemStatusDecoder: Decoder[SubsystemStatus] = Decoder.instance { c =>
    for {
      ok <- c.downField("ok").as[Boolean]
      messages <- c.downField("messages").as[Option[List[String]]]
    } yield SubsystemStatus(ok, messages)
  }

  implicit val systemsDecoder: Decoder[Map[Subsystem, SubsystemStatus]] = Decoder
    .decodeMap[Subsystem, SubsystemStatus](KeyDecoder.decodeKeyString.map(Subsystems.withName), subsystemStatusDecoder)

  implicit val statusCheckResponseDecoder: Decoder[StatusCheckResponse] = Decoder.instance { c =>
    for {
      ok <- c.downField("ok").as[Boolean]
      systems <- c.downField("systems").as[Map[Subsystem, SubsystemStatus]]
    } yield StatusCheckResponse(ok, systems)
  }
}

object PactHelper {

  def buildInteraction(builder: PactDslWithProvider,
                       state: String,
                       stateParams: Map[String, Any],
                       uponReceiving: String,
                       method: String,
                       path: String,
                       requestHeaders: Seq[(String, String)],
                       status: Int,
                       responseHeaders: Seq[(String, String)],
                       body: DslPart
                      ): PactDslResponse =
    builder
      .`given`(state, scala.jdk.CollectionConverters.MapHasAsJava(stateParams).asJava)
      .uponReceiving(uponReceiving)
      .method(method)
      .path(path)
      .headers(scala.jdk.CollectionConverters.MapHasAsJava(requestHeaders.toMap).asJava)
      .willRespondWith()
      .status(status)
      .headers(scala.jdk.CollectionConverters.MapHasAsJava(responseHeaders.toMap).asJava)
      .body(body)
}
