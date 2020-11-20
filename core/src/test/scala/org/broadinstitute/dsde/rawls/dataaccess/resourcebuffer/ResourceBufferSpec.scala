package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.mock.MockResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolType, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.scalatest.AsyncFlatSpec

class ResourceBufferSpec extends AsyncFlatSpec {


  val userInfo = UserInfo(RawlsUserEmail("owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345"))
  val resourceBufferDAO = new MockResourceBufferDAO
  val testConf = ConfigFactory.load()
  val resourceBufferConfig = ResourceBufferConfig(testConf)
  val resourceBufferService = new ResourceBufferService(resourceBufferDAO, resourceBufferConfig)


  "getGoogleProjectFromRBS" should "get a Google Project ID" in {
    val expected = GoogleProjectId("project-from-rbs")

    resourceBufferService.getGoogleProjectFromRBS(ProjectPoolType.Regular, "workspaceId") map {projectId =>
      assert(projectId == expected)
    }
  }

}
