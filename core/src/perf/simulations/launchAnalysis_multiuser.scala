package default

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

class launchAnalysis_multiuser extends RawlsSimulation {
	val numSeconds = lines.next.toInt

	val httpProtocol = http
		.baseURL("https://rawls.dsde-dev.broadinstitute.org")
		.inferHtmlResources()

	val submissionBody = """{"methodConfigurationNamespace":"alex_methods","methodConfigurationName":"cancer_exome_pipeline_v2","entityType":"pair_set","entityName":"pair_set_1","expression":"this.pairs"}"""

	// var b/c I don't know how to do exec() callbacks
	var workspaceSubmissionsMap: Map[String, (String, String)] = Map()

	val scn = scenario(s"launchAnalysis_${numUsers}")
		// input format requirements
	  // accessToken, project
		.feed(csv("{INPUT CSV FILE}"))		// note: the access token script outputs CSV, not TSV.
		.exec(http("submission_request")
			.post("/api/workspaces/${project}/${project}/submissions")
			.headers(Map("Authorization" -> "Bearer ${accessToken}", "Content-Type" -> "application/json"))
			.body(StringBody(submissionBody)) 	//launch the same submission in each workspace
			.check(jsonPath("$.submissionId").saveAs("submissionId"))
			)
		.exec(session => {
			val attrs = session.attributes
			val accessToken = attrs.get("accessToken").get.asInstanceOf[String]
			val project = attrs.get("project").get.asInstanceOf[String]
			val submissionId = attrs.get("submissionId").get.asInstanceOf[String]
			workspaceSubmissionsMap += (accessToken -> (project, submissionId))
			session
		})
	.doIf(session => workspaceSubmissionsMap.size == numUsers) {
		exec(session => {
			val timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date)
			val filename = s"../user-files/data/launchAnalysis_multiuser_${timeStamp}.tsv"

			fileGenerator(new File(filename)) { p =>
				p.println("accessToken\tproject\tsubmissionId")
				workspaceSubmissionsMap.foreach {
					case (accessToken, (project, submissionId)) => p.println(s"${accessToken}\t${project}\t${submissionId}")
				}
				println ("Submission ID mapping file: " + filename)
			}
			session
		})
	}

	setUp(scn.inject(rampUsers(numUsers) over(numSeconds seconds))).protocols(httpProtocol) //ramp up N users over M seconds
}
