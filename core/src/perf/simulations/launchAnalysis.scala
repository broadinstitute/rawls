package default

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.duration._

class launchAnalysis extends RawlsSimulation {

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()
  //	.extraInfoExtractor(extraInfo => List(extraInfo.response)) //for when we want to extract additional info for the simulation.log

  val headers = Map("Authorization" -> s"Bearer ${accessToken}", "Content-Type" -> "application/json")

  val submissionBody =
    """{"methodConfigurationNamespace":"alex_methods","methodConfigurationName":"cancer_exome_pipeline_v2","entityType":"pair_set","entityName":"pair_set_1","expression":"this.pairs"}"""

  // var b/c I don't know how to do exec() callbacks
  var workspaceSubmissionsMap: Map[String, String] = Map()

  val scn = scenario(s"launchAnalysis_${numUsers}")
    .feed(tsv("{LIST_OF_WORKSPACE_NAMES}")) // feed the list of clones created for this test
    .exec(
      http("submission_request")
        .post("/api/workspaces/broad-dsde-dev/${workspaceName}/submissions")
        .headers(headers)
        .body(StringBody(submissionBody)) // launch the same submission in each workspace
        .check(jsonPath("$.submissionId").saveAs("submissionId"))
    )
    .exec { session =>
      val attrs = session.attributes
      workspaceSubmissionsMap += (attrs
        .get("workspaceName")
        .get
        .asInstanceOf[String] -> attrs.get("submissionId").get.asInstanceOf[String])
      session
    }
    .doIf(session => workspaceSubmissionsMap.size == numUsers) {
      exec { session =>
        val timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date)
        val filename = s"../user-files/data/launchAnalysis_${timeStamp}.tsv"

        fileGenerator(new File(filename)) { p =>
          p.println("workspaceName\tsubmissionId")
          workspaceSubmissionsMap.foreach { case (workspaceName, submissionId) =>
            p.println(s"${workspaceName}\t${submissionId}")
          }
          println("Submission ID mapping file: " + filename)
        }
        session
      }
    }

  setUp(scn.inject(rampUsers(numUsers) over (60 seconds))).protocols(httpProtocol) // ramp up n users over 60 seconds
}
