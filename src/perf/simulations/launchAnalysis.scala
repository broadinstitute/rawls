package default

import scala.concurrent.duration._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

class launchAnalysis extends Simulation {

	val lines = scala.io.Source.fromFile("../user-files/config.txt").getLines
	val accessToken = lines.next
	val numUsers = lines.next.toInt

	val httpProtocol = http
		.baseURL("https://rawls.dsde-dev.broadinstitute.org")
		.inferHtmlResources()
	//	.extraInfoExtractor(extraInfo => List(extraInfo.response)) //for when we want to extract additional info for the simulation.log

	val headers = Map("Authorization" -> s"Bearer ${accessToken}",
						"Content-Type" -> "application/json") 

	val submissionBody = """{"methodConfigurationNamespace":"alex_methods","methodConfigurationName":"cancer_exome_pipeline_v2","entityType":"pair_set","entityName":"pair_set_1","expression":"this.pairs"}"""

	val scn = scenario(s"launchAnalysis_${numUsers}")
		.feed(tsv("{LIST_OF_WORKSPACE_NAMES}")) //feed the list of clones created for this test
		.exec(http("submission_request")
			.post("/api/workspaces/broad-dsde-dev/${workspaceName}/submissions")
			.headers(headers)
			.body(StringBody(submissionBody))) //launch the same submission in each workspace

	setUp(scn.inject(rampUsers(numUsers) over(60 seconds))).protocols(httpProtocol) //ramp up n users over 60 seconds
}