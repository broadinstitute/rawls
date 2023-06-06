package default

import java.io._
import scala.concurrent.duration._

class createWorkspaces extends RawlsSimulation {

  val r = scala.util.Random
  val runID = s"gatling_creations_${r.nextInt}"

  // generates a tsv with json bodies to create workspaces
  fileGenerator(new File(s"../user-files/data/createWorkspaces_${runID}.tsv")) { p =>
    p.println("workspaceJson")
    val i = 0
    for (i <- 1 to numUsers)
      p.println(s""""{""namespace"":""broad-dsde-dev"",""name"":""${runID}_${i}"",""attributes"":{}}"""")
  }

  // generates a list of workspaceNames that are to be created. optionally feed this into deleteWorkspaces.scala to cleanup
  fileGenerator(new File(s"../user-files/data/createWorkspaces_NAMES_${runID}.tsv")) { p =>
    p.println("workspaceName")
    val i = 0
    for (i <- 1 to numUsers)
      p.println(s"${runID}_${i}")
  }

  // The run itself

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}", "Content-Type" -> "application/json")

  val scn = scenario(s"createWorkspaces_${numUsers}")
    .feed(tsv(s"../user-files/data/createWorkspaces_${runID}.tsv"))
    .exec(
      http("create_request")
        .post("/api/workspaces")
        .headers(headers)
        .body(StringBody("${workspaceJson}"))
    )

  // NOTE: be sure to re-configure time if needed
  setUp(scn.inject(rampUsers(numUsers) over (30 seconds))).protocols(httpProtocol)
}
