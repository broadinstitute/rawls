package default

import scala.concurrent.duration._

class importTSV extends RawlsSimulation {

  val httpProtocol = http
    .baseURL(
      "https://firecloud.dsde-dev.broadinstitute.org"
    ) // hit orchestration instead of rawls. this functionality doesn't quite exist in rawls
    .inferHtmlResources()

  val headers = Map("Authorization" -> s"Bearer ${accessToken}")

  val scn = scenario(s"importTSV_${numUsers}")
    .feed(tsv("{LIST_OF_WORKSPACE_NAMES}"))
    .exec(
      http("tsv_upload_request")
        .post("/service/api/workspaces/broad-dsde-dev/${workspaceName}/importEntities")
        .headers(headers)
        .bodyPart(RawFileBodyPart("entities", "YOUR_TSV_FILE_PATH").contentType("text/plain"))
    ) // encodes into the multipart/form-data that orchestration wants

  setUp(scn.inject(rampUsers(numUsers) over (60 seconds))).protocols(httpProtocol) // ramp up n users over 60sec
}
