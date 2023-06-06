package default

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

class monitorAnalysis_multiuser extends RawlsSimulation {
  val numSeconds = lines.next.toInt

  val httpProtocol = http
    .baseURL("https://rawls.dsde-dev.broadinstitute.org")
    .inferHtmlResources()

  val submissionBody =
    """{"methodConfigurationNamespace":"alex_methods","methodConfigurationName":"cancer_exome_pipeline_v2","entityType":"pair_set","entityName":"pair_set_1","expression":"this.pairs"}"""

  // var b/c I don't know how to do exec() callbacks
  var monitorResults: Map[String, SubmissionStatus] = Map()

  val scn = scenario(s"monitorAnalysis_${numUsers}")
    // input format requirements
    // accessToken, project, submissionId
    .feed(tsv("{INPUT TSV FILE}"))
    .exec(
      http("submission_monitor")
        .get("/api/workspaces/${project}/${project}/submissions/${submissionId}")
        .headers(Map("Authorization" -> "Bearer ${accessToken}", "Content-Type" -> "application/json"))
        .check(jsonPath("$.status").saveAs("submissionStatus"))
        .check(jsonPath("$.workflows[*].status").findAll.saveAs("workflowStatuses"))
    )
    .exec { session =>
      val attrs = session.attributes
      val status = SubmissionStatus(
        attrs.get("submissionId").get.asInstanceOf[String],
        attrs.get("submissionStatus").get.asInstanceOf[String],
        attrs.get("workflowStatuses").get.asInstanceOf[Vector[String]]
      )
      monitorResults += (attrs.get("project").get.asInstanceOf[String] -> status)
      session
    }
    .doIf(session => monitorResults.size == numUsers) {
      exec { session =>
        val timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date)
        val filename = s"../user-files/data/monitorAnalysis_multiuser_${timeStamp}.tsv"

        fileGenerator(new File(filename)) { p =>
          p.println("project\tsubmissionId\tsubmissionStatus\tworkflowStatuses")
          monitorResults.foreach { case (project, status) =>
            p.println(
              s"${project}\t${status.submissionId}\t${status.submissionStatus}\t${status.workflowStatuses mkString ","}"
            )
          }
          println("Monitoring Results: " + filename)
        }
        session
      }
    }

  setUp(scn.inject(rampUsers(numUsers) over (numSeconds seconds)))
    .protocols(httpProtocol) // ramp up N users over M seconds
}
