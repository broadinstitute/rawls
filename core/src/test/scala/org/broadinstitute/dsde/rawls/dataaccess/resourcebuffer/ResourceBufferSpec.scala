package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.mock.MockResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.scalatest.AsyncFlatSpec

class ResourceBufferSpec extends AsyncFlatSpec {


  val userInfo = UserInfo(RawlsUserEmail("owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345"))
  val resourceBufferDAO = new MockResourceBufferDAO
  val resourceBufferService = new ResourceBufferService(resourceBufferDAO, userInfo)


  "ResourceBuffer" should "do the happy path" in {
    val expected = GoogleProjectId("project-from-rbs")

    resourceBufferService.getGoogleProjectFromRBS() map {projectId =>
      assert(projectId == expected)
    }
  }

}
