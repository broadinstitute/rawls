package default

import scala.concurrent.duration._
import java.io._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

class deleteWorkspaces extends Simulation {

  val lines = scala.io.Source.fromFile("../user-files/config.txt").getLines
  val accessToken = lines.next
  val numUsers = lines.next.toInt

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