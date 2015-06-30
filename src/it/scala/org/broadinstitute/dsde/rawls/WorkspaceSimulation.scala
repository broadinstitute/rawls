package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.{MethodConfigApiService, EntityApiService, WorkspaceApiService, SubmissionApiService}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.WorkspaceSimulation._
import spray.http._
import spray.httpx.SprayJsonSupport
import SprayJsonSupport._
import WorkspaceJsonSupport._
import scala.concurrent.duration._

object WorkspaceSimulation {
  // TODO move these to config?
  val numSamples = 200
  val numPairs = 100
  val numSampleSets = 50
  val numPairSets = 20
  val numMethodConfigs = 200

  val numAnnotationsSmall = 5
  val numSetMembersSmall = 5

  val numObjectUpdates = 100
  val numAnnotationUpdatesSmall = 1

  val numAnnotationsLarge = 10000
  val numAnnotationUpdatesLarge = 2000
}

class WorkspaceSimulation extends IntegrationTestBase with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService {
  implicit val routeTestTimeout = RouteTestTimeout(600.seconds) // this is a load test, so response times may be slow
  def actorRefFactory = system

  val workspaceServiceConstructor = workspaceServiceWithDbName("integration-test-latest") // TODO move this into config?

  val gen = new WorkspaceGenerator("foo", System.currentTimeMillis().toString())

  "WorkspaceSimulation" should "create a workspace" in {
    val postWorkspaceTimer = new Timer("Post workspace")

    postWorkspaceTimer.timedOperation {
      Post(s"/workspaces", httpJson(gen.workspace)) ~>
        addMockOpenAmCookie ~> sealRoute(postWorkspaceRoute) ~>
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
          addMockOpenAmCookie ~> sealRoute(createEntityRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    repeat(numPairs) {
      val pair = gen.createPair(numAnnotationsSmall)
      postEntitiesTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(pair)) ~>
          addMockOpenAmCookie ~> sealRoute(createEntityRoute) ~>
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
          addMockOpenAmCookie ~> sealRoute(createEntityRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    repeat(numPairSets) {
      val pairSet = gen.createEntitySet("pair", numSetMembersSmall)
      postEntitySetTimer.timedOperation {
        Post(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities", httpJson(pairSet)) ~>
          addMockOpenAmCookie ~> sealRoute(createEntityRoute) ~>
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
          addMockOpenAmCookie ~> sealRoute(createMethodConfigurationRoute) ~>
          check { assertResult(StatusCodes.Created) {status} }
      }
    }

    postMethodConfigTimer.printElapsed

    // list the method configs
    val listMethodConfigTimer = new Timer("List method configs")

    listMethodConfigTimer.timedOperation {
      Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs") ~>
        addMockOpenAmCookie ~> sealRoute(listMethodConfigurationsRoute) ~>
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
          addMockOpenAmCookie ~> sealRoute(getEntityRoute) ~>
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
        Patch(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/sample/${name}", httpJson(updateOps)) ~>
          addMockOpenAmCookie ~> sealRoute(updateEntityRoute) ~>
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
          addMockOpenAmCookie ~> sealRoute(getMethodConfigurationRoute) ~>
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
        Put(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/methodconfigs/${gen.methodConfigNamespace}/${name}", httpJson(updatedConfig)) ~>
          addMockOpenAmCookie ~> sealRoute(updateMethodConfigurationRoute) ~>
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
        addMockOpenAmCookie ~> sealRoute(createEntityRoute) ~>
        check { assertResult(StatusCodes.Created) {status} }
    }
    postEntityTimer.printElapsed

    val getEntityTimer = new Timer("Get large entity")
    getEntityTimer.timedOperation {
      Get(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}") ~>
        addMockOpenAmCookie ~> sealRoute(getEntityRoute) ~>
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
      Patch(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}", httpJson(sampleUpdates)) ~>
        addMockOpenAmCookie ~> sealRoute(updateEntityRoute) ~>
        check { assertResult(StatusCodes.OK) {status} }
    }
    updateEntityTimer.printElapsed

    val deleteEntityTimer = new Timer("Delete large entity")
    deleteEntityTimer.timedOperation {
      Delete(s"/workspaces/${gen.wn.namespace}/${gen.wn.name}/entities/${sample.entityType}/${sample.name}") ~>
        addMockOpenAmCookie ~> sealRoute(deleteEntityRoute) ~>
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
