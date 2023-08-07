package org.broadinstitute.dsde.rawls.consumer

import au.com.dius.pact.consumer.dsl.{DslPart, PactDslResponse, PactDslWithProvider}

object PactHelper {

  def buildInteraction(builder: PactDslWithProvider,
                       state: String,
                       stateParams: Map[String, Any],
                       uponReceiving: String,
                       method: String,
                       path: String,
                       status: Int,
                       body: DslPart
  ): PactDslResponse =
    builder
      .`given`(state, scala.jdk.CollectionConverters.MapHasAsJava(stateParams).asJava)
      .uponReceiving(uponReceiving)
      .method(method)
      .path(path)
      .headers(jsonRequestHeaders)
      .willRespondWith()
      .status(status)
      .headers(jsonResponseHeaders)
      .body(body)

  def jsonRequestHeaders = scala.jdk.CollectionConverters.MapHasAsJava(Seq("Accept" -> "application/json").toMap).asJava
  def jsonRequestHeadersWithBody = scala.jdk.CollectionConverters
    .MapHasAsJava(Seq("Accept" -> "application/json", "Content-type" -> "application/json").toMap)
    .asJava
  def jsonResponseHeaders =
    scala.jdk.CollectionConverters.MapHasAsJava(Seq("Content-type" -> "application/json").toMap).asJava

}
