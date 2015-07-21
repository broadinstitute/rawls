package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.{PoisonPill, ActorSystem}
import akka.testkit.{TestKit, TestActorRef}
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{UserInfo, Submission, SubmissionRequest, WorkspaceName}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.{StatusCode, StatusCodes, HttpCookie}
import scala.concurrent.duration._
import spray.http.HttpHeaders.Cookie

import scala.concurrent.Await

/**
 * Created with IntelliJ IDEA.
 * User: hussein
 * Date: 07/02/2015
 * Time: 11:06
 */
class SubmissionSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with OrientDbTestFixture {
  def this() = this(ActorSystem("SubmissionSpec"))

  val testDbName = "SubmissionSpec"
  val cookie = HttpCookie("iPlanetDirectoryPro", "test_token")
  val userId = UserInfo("test_token", "test_token")
  val submissionSupervisorActorName = "test-subspec-submission-supervisor"

  val mockServer = RemoteServicesMockServer()
  override def beforeAll() = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
    mockServer.stopServer
  }

  def withWorkspaceService(testCode: WorkspaceService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
        new GraphSubmissionDAO(new GraphWorkflowDAO()),
        new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
        new GraphWorkflowDAO(),
        dataSource
      ).withDispatcher("submission-monitor-dispatcher"), submissionSupervisorActorName)
      val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, workspaceDAO, entityDAO, methodConfigDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor, submissionDAO)_
      lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor, userId)).underlyingActor
      testCode(workspaceService)
      submissionSupervisor ! PoisonPill
    }
  }

  "Submission requests" should "400 when given an unparseable entity expression" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "Individual", "indiv1", Some("this.is."))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, String)]]
    val (status, _) = rqComplete.response
    assertResult(StatusCodes.BadRequest) {
      status
    }
  }

  it should "400 when given an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "PairSet", "ps1", Some("this.pairs"))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, String)]]
    val (status, _) = rqComplete.response
    assertResult(StatusCodes.BadRequest) {
      status
    }
  }

  it should "400 when given no entity expression and an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "PairSet", "ps1", None)
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, String)]]
    val (status, _) = rqComplete.response
    assertResult(StatusCodes.BadRequest) {
      status
    }
  }

  it should "return a successful Submission and spawn a submission monitor actor when given an entity expression that evaluates to a single entity" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "Pair", "pair1", Some("this.case"))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, Submission)]]
    val (status, data) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    val newSubmission = data.asInstanceOf[Submission]
    val monitorActor = Await.result(system.actorSelection("/user/" + submissionSupervisorActorName + "/" + newSubmission.id).resolveOne(5.seconds), Timeout(5.seconds).duration )
    assert( monitorActor != None ) //not really necessary, failing to find the actor above will throw an exception and thus fail this test

    assert( newSubmission.notstarted.size == 0 )
    assert( newSubmission.workflows.size == 1 )
  }

  it should "return a successful Submission when given an entity expression that evaluates to a set of entites" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "SampleSet", "sset1", Some("this.samples"))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, Submission)]]
    val (status, data) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    val newSubmission = data.asInstanceOf[Submission]
    assert( newSubmission.notstarted.size == 0 )
    assert( newSubmission.workflows.size == 3 )
  }

  //TODO: re-enable when empty sample sets are serialized out of Orient correctly
  /*"Submission requests"*/ ignore should "return a successful Submission when given an entity expression that evaluates to an empty set of entites" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "SampleSet", "sset_empty", Some("this.samples"))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, Submission)]]
    val (status, data) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    val newSubmission = data.asInstanceOf[Submission]
    assert( newSubmission.notstarted.size == 0 )
    assert( newSubmission.workflows.size == 0 )
  }

  it should "return a successful Submission but no started workflows when given a method configuration with unparseable inputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, Submission)]]

    val (status, data) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }
    val newSubmission = data.asInstanceOf[Submission]

    assert( newSubmission.notstarted.size == 3 )
    assert( newSubmission.workflows.size == 0 )
  }

  it should "return a successful Submission with unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val rqComplete = workspaceService.createSubmission( testData.wsName, submissionRq, cookie ).asInstanceOf[RequestComplete[(StatusCode, Submission)]]

    val (status, data) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }
    val newSubmission = data.asInstanceOf[Submission]

    assert( newSubmission.notstarted.size == 1 )
    assert( newSubmission.workflows.size == 2 )
  }
}
