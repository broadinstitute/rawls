package default

import scala.concurrent.duration._

class listWorkspaces extends RawlsSimulation {

  // The run itself

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}", "Content-Type" -> "application/json")

  val scn = scenario(s"listWorkspaces_${numUsers}")
    .exec(
      http("list_request")
        .get("/api/workspaces")
        .headers(headers)
    )

  // NOTE: be sure to re-configure time if needed
  setUp(
    scn.inject(constantUsersPerSec(numUsers) during (30 seconds)).protocols(httpProtocol)
  )
}
