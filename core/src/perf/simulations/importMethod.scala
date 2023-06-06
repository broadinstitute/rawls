package default

import scala.concurrent.duration._

class importMethod extends RawlsSimulation {

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}", "Content-Type" -> "application/json")

  val scn = scenario(s"importConfig_request_${numUsers}")
    .feed(tsv(s"../user-files/data/<NAMES FILE FROM CREATE WORKSPACES>.tsv"))
    .exec(
      http("importConfig_request")
        .post("/api/methodconfigs/copyFromMethodRepo")
        .headers(headers)
        .body(
          StringBody(
            """{"methodRepoNamespace": "broad-dsde-dev","methodRepoName": "EddieFastaCounterPUBLISH","methodRepoSnapshotId": 1,"destination": {"name": "FastaCounter_gatling5","namespace": "broad-dsde-dev","workspaceName": {"namespace": "broad-dsde-dev","name": "${workspaceName}"}}}"""
          )
        )
        .asJSON
    )

  // NOTE: be sure to re-configure time if needed
  setUp(scn.inject(rampUsers(numUsers) over (10 seconds))).protocols(httpProtocol)
}
