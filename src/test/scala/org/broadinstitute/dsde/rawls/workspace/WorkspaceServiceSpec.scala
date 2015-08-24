package org.broadinstitute.dsde.rawls.workspace

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockOpenAmDirectives
import org.broadinstitute.dsde.rawls.webservice._
import AttributeUpdateOperations._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import spray.http.{StatusCodes, ContentTypes, HttpEntity, HttpCookie}
import spray.testkit.ScalatestRouteTest


class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(testData.wsName.namespace, testData.wsName.name))
  val workspace = Workspace(
    testData.wsName.namespace,
    testData.wsName.name,
    "aBucket",
    DateTime.now().withMillis(0),
    "test",
    Map.empty
  )

  case class TestApiService(dataSource: DataSource) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with GoogleAuthApiService with MockOpenAmDirectives {
    def actorRefFactory = system
    val cookie = HttpCookie("iPlanetDirectoryPro", "test_token")
    val userInfo = UserInfo("test_token", cookie)
    lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor, userInfo)).underlyingActor
    val mockServer = RemoteServicesMockServer()

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      new MockContainerDAO(null, mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-ws-submission-supervisor")

    val workspaceServiceConstructor = WorkspaceService.constructor(
      dataSource,
      new MockContainerDAO(mockServer.mockServerBaseUrl, mockServer.mockServerBaseUrl),
      submissionSupervisor)_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withTestDataServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      val apiService = new TestApiService(dataSource)
      try {
        testCode(apiService)
      } finally {
        apiService.cleanupSupervisor
      }
    }
  }


  "WorkspaceService" should "add attribute to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("foo"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute("newAttribute", AttributeString("foo")))).attributes.get("newAttribute")
    }
  }

  it should "update attribute in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("biz"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute("foo", AttributeString("biz")))).attributes.get("foo")
    }
  }

  it should "remove attribute from entity" in withTestDataServices { services =>
    assertResult(None) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveAttribute("foo"))).attributes.get("foo")
    }
  }

  it should "add item to existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("splat", AttributeString("new")))).attributes.get("splat")
    }
  }

  it should "add item to non-existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("bob", AttributeString("new")))).attributes.get("bob")
    }
  }

  it should "create an empty list when inserting null via AddListMember" in withTestDataServices { services =>
    assertResult(Some(AttributeEmptyList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("nolisthere", AttributeNull))).attributes.get("nolisthere")
    }
  }

  it should "do nothing to existing lists when adding AttributeNull" in withTestDataServices { services =>
    assertResult(Some(attributeList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("splat", AttributeNull))).attributes.get("splat")
    }
  }

  it should "remove item from existing listing entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("splat", AttributeString("a")))).attributes.get("splat")
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in withTestDataServices { services =>
    intercept[AttributeNotFoundException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("bingo", AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("foo", AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("foo", AttributeString("a"))))
    }
  }

  it should "apply attribute updates in order to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("splat"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(
        AddUpdateAttribute("newAttribute", AttributeString("foo")),
        AddUpdateAttribute("newAttribute", AttributeString("bar")),
        AddUpdateAttribute("newAttribute", AttributeString("splat"))
      )).attributes.get("newAttribute")
    }
  }

  it should "return conflicts during an entity copy" in {
    val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), testData.wsName)
    val s2 = Entity("s3", "child", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), testData.wsName)
    //println("hello " + workspaceService.getCopyConflicts(wsns, wsname, Seq(s1, s2)).size)
    //still needs to be implemented fully
    assertResult(true) {
      true
    }
  }
}
