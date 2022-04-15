package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.service.{Rawls, RestException}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID

class MultiCloudWorkspaceSpec extends AnyFreeSpecLike {
  "MC Workspace creation" - {
    "should fail with a not implemented code if conf flag is not set" in {
      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()

      val targetRawlsUrl = Uri(Rawls.url).withPath(Path(s"/api/workspaces/mc"))
      val testNs = "testing_ns_" + UUID.randomUUID().toString
      val name = "test_name" + UUID.randomUUID().toString
      val payload = Map("namespace" -> testNs, "name" -> name, "attributes" -> Map.empty, "cloudPlatform" -> "AZURE", "region" -> "eastus")

      val thrown = intercept[RestException](Rawls.postRequest(
        uri = targetRawlsUrl.toString(),
        content = payload)
      ).message.parseJson.asJsObject

      thrown.fields("statusCode").convertTo[Int] shouldBe 501
    }
  }
}
