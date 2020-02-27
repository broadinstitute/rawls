package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchGroupName
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class BondSpec extends TestKit(ActorSystem("HttpSamDAOSpec"))
  with FlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
//    mockServer.startServer()
  }

  override def afterAll(): Unit = {
//    mockServer.stopServer
//    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  // todo: create bond testcases
  // todo: this is an integration test lol
  it should "connect to Bond to get provider list" in {
    val bond = new BondTalker()
    assertResult(Array("fence", "dcf-fence")) {
      Await.result(bond.getBondProviders(), Duration.Inf)
    }
  }


  it should "connect to Bond to get the user's SA key for a provider" in {
    val bond = new BondTalker()
    assertResult(Some("onuikytszok7ckyqgljwlozc-5803@dcf-staging-202214.iam.gserviceaccount.com")) {
      Await.result(bond.getBondProviderServiceAccountKey(UserInfo(RawlsUserEmail("b.adm.firec@gmail.com"), OAuth2BearerToken("token"), 6000000, RawlsUserSubjectId("-1"))), Duration.Inf)
    }
  }
}
