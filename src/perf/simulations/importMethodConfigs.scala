package default

import scala.concurrent.duration._
import java.io._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

class importMethod extends Simulation {

  //Helpers to set up the run

  val lines = scala.io.Source.fromFile("../user-files/config.txt").getLines
  val accessToken = lines.next
  val numUsers = lines.next.toInt

  def fileGenerator(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  val r = scala.util.Random
  val runID = s"gatling_creations_${r.nextInt}"

  //The run itself

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}",
    "Content-Type" -> "application/json")

  val scn = scenario(s"importConfig_request_${numUsers}")
    .feed(tsv(s"../user-files/data/<NAMES FILE FROM CREATE WORKSPACES>.tsv"))
    .exec(http("importConfig_request")
    .post("/api/methodconfigs/copyFromMethodRepo")
    .headers(headers)
    .body(StringBody("""{"methodRepoNamespace": "broad-dsde-dev","methodRepoName": "EddieFastaCounterPUBLISH","methodRepoSnapshotId": 1,"destination": {"name": "FastaCounter_gatling5","namespace": "broad-dsde-dev","workspaceName": {"namespace": "broad-dsde-dev","name": "${workspaceName}"}}}""")).asJSON)

  //NOTE: be sure to re-configure time if needed
  setUp(scn.inject(rampUsers(numUsers) over(10 seconds))).protocols(httpProtocol)
}