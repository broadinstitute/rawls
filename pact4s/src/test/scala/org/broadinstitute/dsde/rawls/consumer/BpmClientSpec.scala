package org.broadinstitute.dsde.rawls.consumer

import au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody
import au.com.dius.pact.consumer.dsl._
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import cats.effect.IO
import cats.effect.unsafe.implicits._
import org.broadinstitute.dsde.rawls.consumer.PactHelper.buildInteraction
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.http4s.Uri
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pact4s.scalatest.RequestResponsePactForger

import scala.concurrent.ExecutionContext

class BpmClientSpec extends AnyFlatSpec with Matchers with RequestResponsePactForger {
  /*
    Define the folder that the pact contracts get written to upon completion of this test suite.
   */
  override val pactTestExecutionContext: PactTestExecutionContext =
    new PactTestExecutionContext(
      "./target/pacts"
    )

  // Uncomment this so that mock server will run on specific port (e.g. 9003) instead of dynamically generated port.
  // override val mockProviderConfig: MockProviderConfig = MockProviderConfig.httpConfig("localhost", 9003)

  // {"ok":true,"systems":{"CloudSQL":{"ok":true},"Sam":{"ok":true}}}
  val subsystems = List(Custom("CloudSQL"), Sam)
  val okSystemStatus: StatusCheckResponse = StatusCheckResponse(
    ok = true,
    systems = subsystems.map(s => (s, SubsystemStatus(ok = true, messages = None))).toMap
  )

  // --- End of fixtures section

  // ---- Dsl for specifying pacts between consumer and provider
  // Lambda Dsl: required for generating matching rules.
  // Favored over old-style Pact Dsl using PactDslJsonBody.
  // This rule expects BPM to respond with
  // 1. ok status
  // 2. ok statuses matching the given subsystem states
  val okSystemStatusDsl: DslPart = newJsonBody { o =>
    o.booleanType("ok", true)
    o.`object`("systems",
               s =>
                 for (subsystem <- subsystems)
                   s.`object`(subsystem.value, o => o.booleanType("ok", true))
    )
  }.build()

  val consumerPactBuilder: ConsumerPactBuilder = ConsumerPactBuilder
    .consumer("rawls-consumer")

  val pactProvider: PactDslWithProvider = consumerPactBuilder
    .hasPactWith("bpm-provider")

  // stateParams provides the desired subsystem states
  // for BPM provider to generate the expected response
  var pactDslResponse: PactDslResponse = buildInteraction(
    pactProvider,
    state = "BPM is ok",
    stateParams = subsystems.map(s => s.toString() -> "ok").toMap,
    uponReceiving = "Request to BPM ok status",
    method = "GET",
    path = "/status",
    requestHeaders = Seq("Accept" -> "application/json"),
    status = 200,
    responseHeaders = Seq("Content-type" -> "application/json"),
    okSystemStatusDsl
  )

  override val pact: RequestResponsePact = pactDslResponse.toPact

  val client: Client[IO] =
    BlazeClientBuilder[IO](ExecutionContext.global).resource.allocated.unsafeRunSync()._1

  /*
  we should use these tests to ensure that our client class correctly handles responses from the provider
   */
  it should "get BPM ok status" in {
    new BpmClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl))
      .fetchSystemStatus()
      .attempt
      .unsafeRunSync() shouldBe Right(okSystemStatus)
  }
}
