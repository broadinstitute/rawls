package default

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.duration._

class monitorAnalysis extends RawlsSimulation {

	val httpProtocol = http
		.baseURL("https://rawls.dsde-dev.broadinstitute.org")
		.inferHtmlResources()

	val headers = Map("Authorization" -> s"Bearer ${accessToken}",
						"Content-Type" -> "application/json") 

	val submissionBody = """{"methodConfigurationNamespace":"alex_methods","methodConfigurationName":"cancer_exome_pipeline_v2","entityType":"pair_set","entityName":"pair_set_1","expression":"this.pairs"}"""

	// var b/c I don't know how to do exec() callbacks
	var monitorResults: Map[String, SubmissionStatus] = Map()

	val scn = scenario(s"monitorAnalysis_${numUsers}")
		.feed(tsv("{MAPPING OUTPUT TSV FROM launchAnalysis}")) //feed the workspaceName/submissionId mapping
		.exec(http("submission_monitor")
			.get("/api/workspaces/broad-dsde-dev/${workspaceName}/submissions/${submissionId}")
			.headers(headers)
			.check(jsonPath("$.status").saveAs("submissionStatus"))
			.check(jsonPath("$.workflows[*].status").findAll.saveAs("workflowStatuses"))
			)
		.exec(session => {
			val attrs = session.attributes
			val status = SubmissionStatus(
				attrs.get("submissionId").get.asInstanceOf[String],
				attrs.get("submissionStatus").get.asInstanceOf[String],
				attrs.get("workflowStatuses").get.asInstanceOf[Vector[String]]
			)
			monitorResults += (attrs.get("workspaceName").get.asInstanceOf[String] -> status)
			session
		})
		.doIf(session => monitorResults.size == numUsers) {
			exec(session => {
				val timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date)
				val filename = s"../user-files/data/monitorAnalysis_${timeStamp}.tsv"

				fileGenerator(new File(filename)) { p =>
					p.println("workspaceName\tsubmissionId\tsubmissionStatus\tworkflowStatuses")
					monitorResults.foreach {
						case (workspaceName, status) => p.println(s"${workspaceName}\t${status.submissionId}\t${status.submissionStatus}\t${status.workflowStatuses mkString ","}")
					}
					println ("Monitoring Results: " + filename)
				}
				session
			})
		}

	setUp(scn.inject(rampUsers(numUsers) over(60 seconds))).protocols(httpProtocol) //ramp up n users over 60 seconds
}