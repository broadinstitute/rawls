package org.broadinstitute.dsde.rawls.integrationtest

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.integrationtest.WorkspaceSimulation._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.webservice.{MethodConfigApiService, EntityApiService, WorkspaceApiService, SubmissionApiService}
import spray.http._
import spray.httpx.SprayJsonSupport._

import scala.concurrent.duration._

object WorkspaceSimulation {
  // full simulation, for load testing
  val fullSimulation = SimulationParams(
    numSamples = 200,
    numPairs = 100,
    numSampleSets = 50,
    numPairSets = 20,
    numMethodConfigs = 200,
    numAnnotationsSmall = 5,
    numSetMembersSmall = 5,
    numObjectUpdates = 100,
    numAnnotationUpdatesSmall = 1,
    numAnnotationsLarge = 10000,
    numAnnotationUpdatesLarge = 2000
  )

  // lite version, for running the simulation locally
  val liteSimulation = SimulationParams(
    numSamples = 1,
    numPairs = 1,
    numSampleSets = 1,
    numPairSets = 1,
    numMethodConfigs = 1,
    numAnnotationsSmall = 1,
    numSetMembersSmall = 1,
    numObjectUpdates = 1,
    numAnnotationUpdatesSmall = 1,
    numAnnotationsLarge = 1,
    numAnnotationUpdatesLarge = 1
  )
}

case class SimulationParams(
  numSamples: Int,
  numPairs: Int,
  numSampleSets: Int,
  numPairSets: Int,
  numMethodConfigs: Int,
  numAnnotationsSmall: Int,
  numSetMembersSmall: Int,
  numObjectUpdates: Int,
  numAnnotationUpdatesSmall: Int,
  numAnnotationsLarge: Int,
  numAnnotationUpdatesLarge: Int
)

class WorkspaceSimulation extends IntegrationTestBase with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService {
  implicit val routeTestTimeout = RouteTestTimeout(600.seconds) // this is a load test, so response times may be slow
  def actorRefFactory = system

  val workspaceServiceConstructor = workspaceServiceWithDbName("integration-test-latest")

  val params = if (integrationRunFullLoadTest) fullSimulation else liteSimulation

  val gen = new WorkspaceGenerator("broad-dsde-dev", System.currentTimeMillis().toString())

  "WorkspaceSimulation" should "create a workspace" in {
    val postWorkspaceTimer = new Timer("Post workspace")

    postWorkspaceTimer.timedOperation {
      Post(s"/workspaces", httpJson(gen.workspace)) ~>
        addSecurityHeaders ~> sealRoute(workspaceRoutes) ~>
        check { assertResult(StatusCodes.Created, response.entity.asString) {status} }
    }

    postWorkspaceTimer.printElapsed
  }

  it should "CRUD many small objects" in {
    // create some entities
    val postEntitiesTimer = new Timer("Post entities")

    repeat(params.numSamples) {
      val sample = gen.createSample(params.numAnnotationsSmall)
      postEntitiesTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(sample)) ~>
          addSecurityHeaders ~> sealRoute(entityRoutes) ~>
          check { assertResult(StatusCodes.Created, response.entity.asString) {status} }
      }
    }

    repeat(params.numPairs) {
      val pair = gen.createPair(params.numAnnotationsSmall)
      postEntitiesTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(pair)) ~>
          addSecurityHeaders ~> sealRoute(entityRoutes) ~>
          check {assertResult(StatusCodes.Created, response.entity.asString) {status}}
      }
    }

    postEntitiesTimer.printElapsed

    // create some entity sets
    val postEntitySetTimer = new Timer("Post entity sets")

    repeat(params.numSampleSets) {
      val sampleSet = gen.createEntitySet("sample", params.numSetMembersSmall)
      postEntitySetTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(sampleSet)) ~>
          addSecurityHeaders ~> sealRoute(entityRoutes) ~>
          check { assertResult(StatusCodes.Created, response.entity.asString) {status} }
      }
    }

    repeat(params.numPairSets) {
      val pairSet = gen.createEntitySet("pair", params.numSetMembersSmall)
      postEntitySetTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(pairSet)) ~>
          addSecurityHeaders ~> sealRoute(entityRoutes) ~>
          check { assertResult(StatusCodes.Created, response.entity.asString) {status} }
      }
    }

    postEntitySetTimer.printElapsed

    // create some method configs
    val postMethodConfigTimer = new Timer("Post method configs")

    repeat(params.numMethodConfigs) {
      val methodConfig = gen.createMethodConfig(params.numAnnotationsSmall)
      postMethodConfigTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs", httpJson(methodConfig)) ~>
          addSecurityHeaders ~> sealRoute(methodConfigRoutes) ~>
          check { assertResult(StatusCodes.Created, response.entity.asString) {status} }
      }
    }

    postMethodConfigTimer.printElapsed

    // list the method configs
    val listMethodConfigTimer = new Timer("List method configs")

    listMethodConfigTimer.timedOperation {
      Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs") ~>
        addSecurityHeaders ~> sealRoute(methodConfigRoutes) ~>
        check { assertResult(StatusCodes.OK, response.entity.asString) {status} }
    }

    listMethodConfigTimer.printElapsed

    // get + update some entities at random
    val getEntityTimer = new Timer("Get entities")
    val updateEntityTimer = new Timer("Update entities")

    repeat(params.numObjectUpdates) {
      val name = gen.pickEntity("sample")
      val sample: Entity = getEntityTimer.timedOperation {
        Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/sample/${name}") ~>
          addSecurityHeaders ~> sealRoute(entityRoutes) ~>
          check {
            assertResult(StatusCodes.OK, response.entity.asString) {status}
            responseAs[Entity]
          }
      }
      val updateOps = gen.updateEntityAnnotations(
        entity = sample,
        nDelete = params.numAnnotationUpdatesSmall,
        nModify = params.numAnnotationUpdatesSmall,
        nCreate = params.numAnnotationUpdatesSmall
      )
      updateEntityTimer.timedOperation {
        Patch(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/sample/${name}", httpJson(updateOps)) ~>
          addSecurityHeaders ~> sealRoute(entityRoutes) ~>
          check { assertResult(StatusCodes.OK, response.entity.asString) {status} }
      }
    }

    getEntityTimer.printElapsed
    updateEntityTimer.printElapsed

    // get + update some method configs at random
    val getMethodConfigTimer = new Timer("Get method configs")
    val updateMethodConfigTimer = new Timer("Update method configs")

    repeat(params.numObjectUpdates) {
      val name = gen.pickMethodConfig
      val methodConfig: MethodConfiguration = getMethodConfigTimer.timedOperation {
        Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs/${gen.methodConfigNamespace}/${name}") ~>
          addSecurityHeaders ~> sealRoute(methodConfigRoutes) ~>
          check {
            assertResult(StatusCodes.OK, response.entity.asString) {status}
            responseAs[MethodConfiguration]
          }
      }
      val updatedConfig = gen.createUpdatedMethodConfig(
        config = methodConfig,
        nDelete = params.numAnnotationUpdatesSmall,
        nModify = params.numAnnotationUpdatesSmall,
        nCreate = params.numAnnotationUpdatesSmall
      )
      updateMethodConfigTimer.timedOperation {
        Put(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs/${gen.methodConfigNamespace}/${name}", httpJson(updatedConfig)) ~>
          addSecurityHeaders ~> sealRoute(methodConfigRoutes) ~>
          check { assertResult(StatusCodes.OK, response.entity.asString) {status} }
      }
    }

    getMethodConfigTimer.printElapsed
    updateMethodConfigTimer.printElapsed

    // remove entities + configs at random
    // TODO
  }

  it should "CRUD a large object" in {
    val sample = gen.createSample(params.numAnnotationsLarge)

    val postEntityTimer = new Timer("Post large entity")
    postEntityTimer.timedOperation {
      Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(sample)) ~>
        addSecurityHeaders ~> sealRoute(entityRoutes) ~>
        check { assertResult(StatusCodes.Created, response.entity.asString) {status} }
    }
    postEntityTimer.printElapsed

    val getEntityTimer = new Timer("Get large entity")
    getEntityTimer.timedOperation {
      Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}") ~>
        addSecurityHeaders ~> sealRoute(entityRoutes) ~>
        check { assertResult(StatusCodes.OK, response.entity.asString) {status} }
    }
    getEntityTimer.printElapsed

    val updateEntityTimer = new Timer("Update large entity")
    val sampleUpdates = gen.updateEntityAnnotations(
      entity = sample,
      nDelete = params.numAnnotationUpdatesLarge,
      nModify = params.numAnnotationUpdatesLarge,
      nCreate = params.numAnnotationUpdatesLarge
    )

    updateEntityTimer.timedOperation {
      Patch(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}", httpJson(sampleUpdates)) ~>
        addSecurityHeaders ~> sealRoute(entityRoutes) ~>
        check { assertResult(StatusCodes.OK, response.entity.asString) {status} }
    }
    updateEntityTimer.printElapsed

    val deleteEntityTimer = new Timer("Delete large entity")
    deleteEntityTimer.timedOperation {
      Delete(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}") ~>
        addSecurityHeaders ~> sealRoute(entityRoutes) ~>
        check { assertResult(StatusCodes.NoContent, response.entity.asString) {status} }
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
