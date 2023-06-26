package org.broadinstitute.dsde.rawls.consumer

import au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody
import au.com.dius.pact.consumer.dsl._
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import bio.terra.profile.model.{SystemStatus, SystemStatusSystems}
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAOImpl, HttpBillingProfileManagerClientProvider}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.consumer.PactHelper.buildInteraction
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pact4s.scalatest.RequestResponsePactForger

class BpmClientSpec extends AnyFlatSpec with Matchers with RequestResponsePactForger {
  /*
    Define the folder that the pact contracts get written to upon completion of this test suite.
   */
  override val pactTestExecutionContext: PactTestExecutionContext =
    new PactTestExecutionContext(
      "./target/pacts"
    )

  private val subSystems = List("CloudSQL", "Sam")
  val subSystemStatus = new java.util.HashMap[String, SystemStatusSystems]()
  for (s <- subSystems) subSystemStatus.put(s, new SystemStatusSystems().ok(true))
  val okSystemStatus: SystemStatus = new SystemStatus().ok(true).systems(subSystemStatus)

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
                 for (subsystem <- subSystems)
                   s.`object`(subsystem, o => o.booleanType("ok", true))
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
    stateParams = subSystems.map(s => s -> "ok").toMap,
    uponReceiving = "Request to BPM ok status",
    method = "GET",
    path = "/status",
    requestHeaders = Seq("Accept" -> "application/json"),
    status = 200,
    responseHeaders = Seq("Content-type" -> "application/json"),
    okSystemStatusDsl
  )

  override val pact: RequestResponsePact = pactDslResponse.toPact

  it should "get BPM ok status" in {
    val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
    val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig.apply(conf)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    val systemStatus = billingProfileManagerDAO.getStatus()
    systemStatus.isOk shouldBe true
  }
}
