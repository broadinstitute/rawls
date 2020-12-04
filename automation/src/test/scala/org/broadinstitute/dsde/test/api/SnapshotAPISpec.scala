package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import bio.terra.workspace.model.{DataReferenceList, ReferenceTypeEnum}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.Rawls
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SnapshotAPISpec extends AnyFreeSpecLike with Matchers
  with WorkspaceFixtures with BillingFixtures
  with SprayJsonSupport with LazyLogging {

  // TODO: don't read conf directly
//  val conf = ConfigFactory.load()
//  assume(conf.hasPath("fireCloud.dataRepoUrl"), "fireCloud.dataRepoUrl does not exist in conf")
//  val dataRepoBaseUrl = conf.getString("fireCloud.dataRepoUrl")

  // tOdO: don't hardcode
  val dataRepoBaseUrl = "https://jade.datarepo-dev.broadinstitute.org"

  "TDR Snapshot integration" - {
    "should allow snapshot references to be added to workspaces" in {
      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()

      withCleanBillingProject(owner) { projectName =>
        withWorkspace(projectName, s"${UUID.randomUUID().toString}-snapshot references") { workspaceName =>


          // call data repo to list snapshots
          // this gets the most recent snapshots in TDR (to which we have read access). This can cause tests to change
          // over time, if the snapshots keep changing. It's here for convenience - we can always add/remove snapshots from
          // TDR and this code will always pick up the latest. But if it causes flakiness we could change to sorting by
          // created_date ASC - that should be more stable.
          val dataRepoDAO = new TestDataRepoDAO("terra", dataRepoBaseUrl).getRepositoryApi(ownerAuthToken)

          logger.info(s"!!!!!!!!!!!! calling data repo at $dataRepoBaseUrl as user ${owner.email} ... ")
          val drSnapshots = Try(dataRepoDAO.enumerateSnapshots(
            0, 2, "created_date", "desc", "")) match {
            case Success(s) => s
            case Failure(ex) =>
              logger.error(s"!!!!!!!!!!!! data repo call as user ${owner.email} failed: ${ex.getMessage}", ex)
              throw(ex)
          }
          assume(drSnapshots.getItems.size() == 2,
            s"TDR at $dataRepoBaseUrl did not have 2 snapshots for this test to use!")

          val dataRepoSnapshotId = drSnapshots.getItems.get(0).getId
          val anotherDataRepoSnapshotId = drSnapshots.getItems.get(1).getId

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


  private def listSnapshotReferences(projectName: String, workspaceName: String)(implicit authToken: AuthToken) = {
    Rawls.getRequest(
      uri = s"${Rawls.url}api/workspaces/$projectName/$workspaceName/snapshots")
  }

  private def createSnapshotReference(projectName: String, workspaceName: String, snapshotId: String, snapshotName: String)(implicit authToken: AuthToken) = {
    Rawls.postRequest(
      uri = s"${Rawls.url}api/workspaces/$projectName/$workspaceName/snapshots",
      content = Map("snapshotId" -> snapshotId, "name" -> snapshotName))
  }

  // a bastardized version of the HttpDataRepoDAO in the main rawls codebase
  class TestDataRepoDAO(dataRepoInstanceName: String, dataRepoInstanceBasePath: String) { // extends DataRepoDAO {

    private def getApiClient(accessToken: String): ApiClient = {
      val client: ApiClient = new ApiClient()
      client.setBasePath(dataRepoInstanceBasePath)
      client.setAccessToken(accessToken)

      client
    }

    def getRepositoryApi(accessToken: AuthToken) = {
      new RepositoryApi(getApiClient(accessToken.value))
    }
  }



}
