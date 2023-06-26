package org.broadinstitute.dsde.rawls.consumer

import au.com.dius.pact.consumer.dsl.{DslPart, PactDslResponse, PactDslWithProvider}

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
