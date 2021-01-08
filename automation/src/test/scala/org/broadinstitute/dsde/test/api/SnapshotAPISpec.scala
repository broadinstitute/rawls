package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.model.Uri.{Path, Query}
import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import bio.terra.workspace.model.{DataReferenceList, ReferenceTypeEnum}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.ServiceTestConfig.FireCloud
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.Rawls
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SnapshotAPISpec extends AnyFreeSpecLike with Matchers
  with WorkspaceFixtures with BillingFixtures
  with SprayJsonSupport with LazyLogging {

  private val dataRepoBaseUrl = FireCloud.dataRepoApiUrl

  "TDR Snapshot integration" - {

    "should have proper test config" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      assert(Try(Uri.parseAbsolute(dataRepoBaseUrl)).isSuccess,
        s"Tests in this suite will fail because [$dataRepoBaseUrl] is not a valid url for data repo")
    }

    "should be able to contact Data Repo" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      // status API is unauthenticated, but all our utility methods expect a token.
      // so, we'll send a token to the unauthenticated API to make this code path easier.
      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()
      val statusRequest = Rawls.getRequest(dataRepoBaseUrl + "status")

      withClue(s"Data Repo status API returned ${statusRequest.status.intValue()} ${statusRequest.status.reason()}!") {
        statusRequest.status shouldBe StatusCodes.OK // compare
      }
    }

    "should allow snapshot references to be added to workspaces" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      // as of this writing, hermione.owner is the user with access to snapshots
      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")

      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()

      withCleanBillingProject(owner) { projectName =>
        withWorkspace(projectName, s"${UUID.randomUUID().toString}-snapshot references") { workspaceName =>

          // call data repo to list snapshots
          // this gets the most recent snapshots in TDR (to which we have read access). This can cause tests to change
          // over time, if the snapshots keep changing. It's here for convenience - we can always add/remove snapshots from
          // TDR and this code will always pick up the latest. But if it causes flakiness we could change to sorting by
          // created_date ASC - that should be more stable.
          val dataRepoDAO = new TestDataRepoDAO("terra", dataRepoBaseUrl).getRepositoryApi(ownerAuthToken)

          logger.info(s"calling data repo at $dataRepoBaseUrl as user ${owner.email} ... ")
          val drSnapshots = Try(dataRepoDAO.enumerateSnapshots(
            0, 2, "created_date", "desc", "")) match {
            case Success(s) => s
            case Failure(ex) =>
              logger.error(s"data repo call as user ${owner.email} failed: ${ex.getMessage}", ex)
              throw ex
          }
          assume(drSnapshots.getItems.size() == 2,
            s"TDR at $dataRepoBaseUrl did not have 2 snapshots for this test to use!")

          val dataRepoSnapshotId = drSnapshots.getItems.get(0).getId
          val anotherDataRepoSnapshotId = drSnapshots.getItems.get(1).getId

          logger.info(s"found 2 snapshots from $dataRepoBaseUrl as user ${owner.email}: $dataRepoSnapshotId, $anotherDataRepoSnapshotId")

          // add snapshot reference to the workspace. Under the covers, this creates the workspace in WSM and adds the ref
          createSnapshotReference(projectName, workspaceName, dataRepoSnapshotId, "firstSnapshot")

          // validate the snapshot was added correctly: list snapshots in Rawls, should return 1, which we just added.
          // if we can successfully list snapshot references, it means WSM created its copy of the workspace
          val firstListResponse = listSnapshotReferences(projectName, workspaceName)
          val firstResources = Rawls.parseResponseAs[DataReferenceList](firstListResponse).getResources.asScala
          firstResources.size shouldBe 1
          firstResources.head.getName shouldBe "firstSnapshot"
          firstResources.head.getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
          firstResources.head.getReference.getSnapshot shouldBe dataRepoSnapshotId

          // add a second snapshot reference to the workspace. Under the covers, this recognizes the workspace
          // already exists in WSM, so it just adds the ref
          createSnapshotReference(projectName, workspaceName, anotherDataRepoSnapshotId, "secondSnapshot")

          // validate the second snapshot was added correctly: list snapshots in Rawls, should return 2, which we just added
          val secondListResponse = listSnapshotReferences(projectName, workspaceName)
          // sort by reference name for easy predictability inside this test: "firstSnapshot" is before "secondSnapshot"
          val secondResources = Rawls.parseResponseAs[DataReferenceList](secondListResponse)
            .getResources.asScala.sortBy(_.getName)
          secondResources.size shouldBe 2
          secondResources.head.getName shouldBe "firstSnapshot"
          secondResources.head.getReference.getSnapshot shouldBe dataRepoSnapshotId
          secondResources.head.getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
          secondResources(1).getName shouldBe "secondSnapshot"
          secondResources(1).getReference.getSnapshot shouldBe anotherDataRepoSnapshotId
          secondResources(1).getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
        }
      }
    }

  }

  private def listSnapshotReferences(projectName: String, workspaceName: String, offset: Int = 0, limit: Int = 10)(implicit authToken: AuthToken) = {
    val targetRawlsUrl  = Uri(Rawls.url)
      .withPath(Path(s"/api/workspaces/$projectName/$workspaceName/snapshots"))
      .withQuery(Query(Map("offset" -> offset.toString, "limit" ->  limit.toString)))
    Rawls.getRequest(uri = targetRawlsUrl.toString)
  }

  private def createSnapshotReference(projectName: String, workspaceName: String, snapshotId: String, snapshotName: String)(implicit authToken: AuthToken) = {
    val targetRawlsUrl  = Uri(Rawls.url).withPath(Path(s"/api/workspaces/$projectName/$workspaceName/snapshots"))
    val payload = Map("snapshotId" -> snapshotId, "name" -> snapshotName)
    Rawls.postRequest(
      uri = targetRawlsUrl.toString(),
      content = payload)
  }

  // a bastardized version of the HttpDataRepoDAO in the main rawls codebase
  class TestDataRepoDAO(dataRepoInstanceName: String, dataRepoInstanceBasePath: String) {

    private def getApiClient(accessToken: String): ApiClient = {
      val client: ApiClient = new ApiClient()
      client.setBasePath(dataRepoInstanceBasePath)
      client.setAccessToken(accessToken)

      client
    }

    def getRepositoryApi(accessToken: AuthToken): RepositoryApi = {
      new RepositoryApi(getApiClient(accessToken.value))
    }
  }

}
