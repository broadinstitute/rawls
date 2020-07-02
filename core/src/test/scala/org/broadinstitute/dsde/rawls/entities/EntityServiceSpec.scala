package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddListMember, AddUpdateAttribute, CreateAttributeEntityReferenceList, CreateAttributeValueList, RemoveAttribute, RemoveListMember}
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeEntityReference, AttributeEntityReferenceEmptyList, AttributeEntityReferenceList, AttributeName, AttributeNull, AttributeNumber, AttributeString, AttributeValueEmptyList, AttributeValueList, Entity, RawlsUser, UserInfo, Workspace}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.broadinstitute.dsde.rawls.workspace.{AttributeNotFoundException, AttributeUpdateOperationException}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class EntityServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with TestDriverComponent with RawlsTestUtils with Eventually with MockitoTestUtils with RawlsStatsDTestUtils with BeforeAndAfterAll {

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map(
    AttributeName.withDefaultNS("foo") -> AttributeString("x"),
    AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
    AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(Seq(AttributeEntityReference("participant", "p1"))),
    AttributeName.withDefaultNS("splat") -> attributeList))
  val workspace = Workspace(
    testData.wsName.namespace,
    testData.wsName.name,
    "aWorkspaceId",
    "aBucket",
    Some("workflow-collection"),
    currentTime(),
    currentTime(),
    "test",
    Map.empty
  )

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  //noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit val executionContext: ExecutionContext) extends EntityApiService with MockUserInfoDirectivesWithUser {
    private val userInfo1 = UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId)
    lazy val entityService: EntityService = entityServiceConstructor(userInfo1)

    def actorRefFactory = system
    val samDAO = new MockSamDAO(dataSource)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    val testConf = ConfigFactory.load()

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      EntityManager.defaultEntityManager(dataSource, new MockWorkspaceManagerDAO(), new MockDataRepoDAO(), samDAO, bigQueryServiceFactory, DataRepoEntityProviderConfig(100, 10))
    )_
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }
  }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }

  "EntityService" should "add attribute to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("foo"))) {
      services.entityService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")))).attributes.get(AttributeName.withDefaultNS("newAttribute"))
    }
  }

  it should "update attribute in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("biz"))) {
      services.entityService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("biz")))).attributes.get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "remove attribute from entity" in withTestDataServices { services =>
    assertResult(None) {
      services.entityService.applyOperationsToEntity(s1, Seq(RemoveAttribute(AttributeName.withDefaultNS("foo")))).attributes.get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "add item to existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      services.entityService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeString("new")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "add item to non-existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      services.entityService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("bob"), AttributeString("new")))).attributes.get(AttributeName.withDefaultNS("bob"))
    }
  }

  it should "throw AttributeUpdateOperationException when trying to create a new empty list by inserting AttributeNull" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.entityService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("nolisthere"), AttributeNull))).attributes.get(AttributeName.withDefaultNS("nolisthere"))
    }
  }

  it should "create empty AttributeEntityReferenceList" in withTestDataServices { services =>
    assertResult(Some(AttributeEntityReferenceEmptyList)) {
      services.entityService.applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("emptyRefList")))).attributes.get(AttributeName.withDefaultNS("emptyRefList"))
    }
  }

  it should "not wipe existing AttributeEntityReferenceList when calling CreateAttributeEntityReferenceList" in withTestDataServices { services =>
    assertResult(Some(s1.attributes(AttributeName.withDefaultNS("refs")))) {
      services.entityService.applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refs")))).attributes.get(AttributeName.withDefaultNS("refs"))
    }
  }

  it should "create empty AttributeValueList" in withTestDataServices { services =>
    assertResult(Some(AttributeValueEmptyList)) {
      services.entityService.applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("emptyValList")))).attributes.get(AttributeName.withDefaultNS("emptyValList"))
    }
  }

  it should "not wipe existing AttributeValueList when calling CreateAttributeValueList" in withTestDataServices { services =>
    assertResult(Some(s1.attributes(AttributeName.withDefaultNS("splat")))) {
      services.entityService.applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("splat")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "do nothing to existing lists when adding AttributeNull" in withTestDataServices { services =>
    assertResult(Some(attributeList)) {
      services.entityService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeNull))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "remove item from existing listing entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      services.entityService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("splat"), AttributeString("a")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in withTestDataServices { services =>
    intercept[AttributeNotFoundException] {
      services.entityService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.entityService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.entityService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("foo"), AttributeString("a"))))
    }
  }

  it should "apply attribute updates in order to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("splat"))) {
      services.entityService.applyOperationsToEntity(s1, Seq(
        AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")),
        AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")),
        AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("splat"))
      )).attributes.get(AttributeName.withDefaultNS("newAttribute"))
    }
  }


}
