package org.broadinstitute.dsde.rawls

import java.io.File
import java.util.logging.{Logger, LogManager}

import akka.testkit.TestActorRef
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.WorkspaceApiService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.rawls.workspace.EntityUpdateOperations._
import org.broadinstitute.dsde.rawls.WorkspaceSimulation._
import org.scalatest.{Matchers, FlatSpec}
import spray.http._
import spray.json._
import spray.httpx.SprayJsonSupport
import SprayJsonSupport._
import WorkspaceJsonSupport._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.duration._

object WorkspaceSimulation {
  // TODO move these to config?
  // TODO Rawls seems to become unresponsive if the numbers are much higher,
  // TODO so we should try to gain a better understanding of the performance issues,
  // TODO before trying to increase these numbers
  val numSamples = 20
  val numPairs = 10
  val numSampleSets = 5
  val numPairSets = 2
  val numMethodConfigs = 20

  val numAnnotationsSmall = 5
  val numSetMembersSmall = 5

  val numObjectUpdates = 10
  val numAnnotationUpdatesSmall = 1

  val numAnnotationsLarge = 1000
  val numAnnotationUpdatesLarge = 200
}

class WorkspaceSimulation extends FlatSpec with WorkspaceApiService with ScalatestRouteTest with Matchers {
  implicit val routeTestTimeout = RouteTestTimeout(600.seconds) // this is a load test, so response times may be slow
  def actorRefFactory = system

  // convenience methods - TODO add these to unit tests too?
  def addCookie = addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token")))
  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = HttpEntity(ContentTypes.`application/json`, obj.toJson.toString())
  def repeat[T](n: Int)(exp: => T) = (1 to n) map (_ => exp)

  // setup DB. if it already exists, drop and then re-create it.
  val dbName = "integration-test-latest" // TODO move this into config?
  val orientConfig = ConfigFactory.parseFile(new File("/etc/rawls.conf")).getConfig("orientdb")
  val dbUrl = s"remote:${orientConfig.getString("server")}/${dbName}"
  val admin = new OServerAdmin(dbUrl).connect(orientConfig.getString("rootUser"), orientConfig.getString("rootPassword"))
  if (admin.existsDatabase()) admin.dropDatabase(dbName)
  admin.createDatabase("graph", "plocal") // storage type is 'plocal' even though this is a remote server
  val dataSource = DataSource(dbUrl, orientConfig.getString("rootUser"), orientConfig.getString("rootPassword"), 0, 30)

  // suppress Java logging (otherwise OrientDB will produce a ton of useless log messages)
  LogManager.getLogManager().reset()
  Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)

  // setup workspace service
  val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, new GraphWorkspaceDAO(), new GraphEntityDAO(), new GraphMethodConfigurationDAO())
  lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor)).underlyingActor
  val gen = new WorkspaceGenerator("foo", System.currentTimeMillis().toString())

  "WorkspaceSimulation" should "create a workspace" in {
    val postWorkspaceTimer = new Timer("Post workspace")

    postWorkspaceTimer.timedOperation {
      Post(s"/workspaces", httpJson(gen.workspace)) ~>
        addCookie ~> sealRoute(postWorkspaceRoute) ~>
        check { assertResult(StatusCodes.Created) {status} }
    }

    postWorkspaceTimer.printElapsed
  }

  it should "CRUD many small objects" in {
    // create some entities
    val postEntitiesTimer = new Timer("Post entities")

    repeat(numSamples) {
      val sample = gen.createSample(numAnnotationsSmall)
      postEntitiesTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(sample)) ~>
          addCookie ~> sealRoute(createEntityRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    repeat(numPairs) {
      val pair = gen.createPair(numAnnotationsSmall)
      postEntitiesTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(pair)) ~>
          addCookie ~> sealRoute(createEntityRoute) ~>
          check {assertResult(StatusCodes.Created) {status}}
      }
    }

    postEntitiesTimer.printElapsed

    // create some entity sets
    val postEntitySetTimer = new Timer("Post entity sets")

    repeat(numSampleSets) {
      val sampleSet = gen.createEntitySet("sample", numSetMembersSmall)
      postEntitySetTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(sampleSet)) ~>
          addCookie ~> sealRoute(createEntityRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    repeat(numPairSets) {
      val pairSet = gen.createEntitySet("pair", numSetMembersSmall)
      postEntitySetTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(pairSet)) ~>
          addCookie ~> sealRoute(createEntityRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    postEntitySetTimer.printElapsed

    // create some method configs
    val postMethodConfigTimer = new Timer("Post method configs")

    repeat(numMethodConfigs) {
      val methodConfig = gen.createMethodConfig(numAnnotationsSmall)
      postMethodConfigTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs", httpJson(methodConfig)) ~>
          addCookie ~> sealRoute(createMethodConfigurationRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    postMethodConfigTimer.printElapsed

    // list the method configs
    val listMethodConfigTimer = new Timer("List method configs")

    listMethodConfigTimer.timedOperation {
      Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs") ~>
        addCookie ~> sealRoute(listMethodConfigurationsRoute) ~>
        check { assertResult(StatusCodes.OK) {status} }
    }

    listMethodConfigTimer.printElapsed

    // get + update some entities at random
    val getEntityTimer = new Timer("Get entities")
    val updateEntityTimer = new Timer("Update entities")

    repeat(numObjectUpdates) {
      val name = gen.pickEntity("sample")
      val sample: Entity = getEntityTimer.timedOperation {
        Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/sample/${name}") ~>
          addCookie ~> sealRoute(getEntityRoute) ~>
          check {
            assertResult(StatusCodes.OK) {status}
            responseAs[Entity]
          }
      }
      val updateOps = gen.updateEntityAnnotations(
        entity = sample,
        nDelete = numAnnotationUpdatesSmall,
        nModify = numAnnotationUpdatesSmall,
        nCreate = numAnnotationUpdatesSmall
      )
      updateEntityTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/sample/${name}", httpJson(updateOps)) ~>
          addCookie ~> sealRoute(updateEntityRoute) ~>
          check { assertResult(StatusCodes.OK) {status} }
      }
    }

    getEntityTimer.printElapsed
    updateEntityTimer.printElapsed

    // get + update some method configs at random
    val getMethodConfigTimer = new Timer("Get method configs")
    val updateMethodConfigTimer = new Timer("Update method configs")

    repeat(numObjectUpdates) {
      val name = gen.pickMethodConfig
      val methodConfig: MethodConfiguration = getMethodConfigTimer.timedOperation {
        Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs/${gen.methodConfigNamespace}/${name}") ~>
          addCookie ~> sealRoute(getMethodConfigurationRoute) ~>
          check {
            assertResult(StatusCodes.OK) {status}
            responseAs[MethodConfiguration]
          }
      }
      val updatedConfig = gen.createUpdatedMethodConfig(
        config = methodConfig,
        nDelete = numAnnotationUpdatesSmall,
        nModify = numAnnotationUpdatesSmall,
        nCreate = numAnnotationUpdatesSmall
      )
      updateMethodConfigTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs/update", httpJson(updatedConfig)) ~>
          addCookie ~> sealRoute(updateMethodConfigurationRoute) ~>
          check { assertResult(StatusCodes.OK) {status} }
      }
    }

    getMethodConfigTimer.printElapsed
    updateMethodConfigTimer.printElapsed

    // remove entities + configs at random
    // TODO
  }

  it should "CRUD a large object" in {
    val sample = gen.createSample(numAnnotationsLarge)

    val postEntityTimer = new Timer("Post large entity")
    postEntityTimer.timedOperation {
      Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(sample)) ~>
        addCookie ~> sealRoute(createEntityRoute) ~>
        check { assertResult(StatusCodes.Created) {status} }
    }
    postEntityTimer.printElapsed

    val getEntityTimer = new Timer("Get large entity")
    getEntityTimer.timedOperation {
      Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}") ~>
        addCookie ~> sealRoute(getEntityRoute) ~>
        check { assertResult(StatusCodes.OK) {status} }
    }
    getEntityTimer.printElapsed

    val updateEntityTimer = new Timer("Update large entity")
    val sampleUpdates = gen.updateEntityAnnotations(
      entity = sample,
      nDelete = numAnnotationUpdatesLarge,
      nModify = numAnnotationUpdatesLarge,
      nCreate = numAnnotationUpdatesLarge
    )

    updateEntityTimer.timedOperation {
      Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}", httpJson(sampleUpdates)) ~>
        addCookie ~> sealRoute(updateEntityRoute) ~>
        check { assertResult(StatusCodes.OK) {status} }
    }
    updateEntityTimer.printElapsed

    val deleteEntityTimer = new Timer("Delete large entity")
    deleteEntityTimer.timedOperation {
      Delete(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}") ~>
        addCookie ~> sealRoute(deleteEntityRoute) ~>
        check { assertResult(StatusCodes.NoContent) {status} }
    }
    deleteEntityTimer.printElapsed
  }

  it should "clone a large workspace" in {
    // TODO
  }

  it should "create + list many workspaces" in {
    // TODO
  }
}
