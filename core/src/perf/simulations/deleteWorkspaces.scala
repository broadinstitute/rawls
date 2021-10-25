package default

import scala.concurrent.duration._

class deleteWorkspaces extends RawlsSimulation {

  val workspaceListPath = "WORKSPACE_LIST.tsv" //list of workspaces to delete

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}",
    "Content-Type" -> "application/json")

  val scn = scenario(s"deleteWorkspaces_${numUsers}")
    .feed(tsv(workspaceListPath))
    .exec(http("delete_request")
    .delete("/api/workspaces/broad-dsde-dev/${workspaceName}")
    .headers(headers))

  setUp(scn.inject(rampUsers(numUsers) over(30 seconds))).protocols(httpProtocol)
}