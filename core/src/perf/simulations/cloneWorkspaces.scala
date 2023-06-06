package default

import java.io._
import scala.concurrent.duration._

class cloneWorkspaces extends RawlsSimulation {

  val r = scala.util.Random
  val runID = s"gatling_clones_${r.nextInt}"

  // generates a tsv with json bodies to create workspaces
  fileGenerator(new File(s"../user-files/data/cloneWorkspaces_${runID}.tsv")) { p =>
    p.println("workspaceJson")
    val i = 0
    for (i <- 1 to numUsers)
      p.println(s""""{""namespace"":""broad-dsde-dev"",""name"":""${runID}_${i}"",""attributes"":{}}"""")
  }

  // generates a list of workspaceNames that are to be created. optionally feed this into deleteWorkspaces.scala to cleanup
  fileGenerator(new File(s"../user-files/data/cloneWorkspaces_NAMES_${runID}.tsv")) { p =>
    p.println("workspaceName")
    val i = 0
    for (i <- 1 to numUsers)
      p.println(s"${runID}_${i}")
  }

  // The gatling run

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}", "Content-Type" -> "application/json")

  val scn = scenario(s"cloneWorkspaces_${numUsers}")
    .feed(tsv(s"../user-files/data/cloneWorkspaces_${runID}.tsv")) // the tsv from generateTSV
    .exec(
      http("clone_request")
        .post("/api/workspaces/broad-dsde-dev/Dec8thish/clone") // our workshop model workspace
        .headers(headers)
        .body(StringBody("${workspaceJson}"))
    ) // feeds off of the workspaceJson column in the tsv file

  setUp(scn.inject(rampUsers(numUsers) over (60 seconds))).protocols(httpProtocol) // ramp up n users over 60 seconds
}
