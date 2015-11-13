package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{FlatSpec, Matchers}
import spray.http.OAuth2BearerToken

import scala.collection.JavaConversions._

class GraphBillingDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {

  def getMatchingUserVertices(graph: Graph, user: RawlsUser): Iterable[Vertex] =
    graph.getVertices.filter(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.User) &&
        v.getProperty[String]("userSubjectId") == user.userSubjectId.value
    })

  def getMatchingBillingProjectVertices(graph: Graph, project: RawlsBillingProject): Iterable[Vertex] =
    graph.getVertices.filter(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.BillingProject) &&
        v.getProperty[String]("projectName") == project.projectName.value
    })

  val testUserInfo = UserInfo("dummy-email@example.com", OAuth2BearerToken("dummy-token"), 0, "dummy-ID")
  val testUser = RawlsUser(testUserInfo)
  val testUserRef: RawlsUserRef = testUser

  def userFromId(subjectId: String) =
    RawlsUser(RawlsUserSubjectId(subjectId), RawlsUserEmail("dummy@example.com"))

  def projectFromName(name: String) =
    RawlsBillingProject(RawlsBillingProjectName(name), Set.empty)

  val testProject = projectFromName("Test Billing Project")

  "GraphBillingDAO" should "save a new billing project" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      billingDAO.saveProject(testProject, txn)

      txn.withGraph { graph =>
        assert {
          getMatchingBillingProjectVertices(graph, testProject).nonEmpty
        }
      }
    }
  }

  it should "load a billing project" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      assert {
        billingDAO.loadProject(testProject.projectName, txn).isEmpty
      }
    }

    dataSource.inTransaction() { txn =>
      billingDAO.saveProject(testProject, txn)
    }

    dataSource.inTransaction() { txn =>
      assertResult(testProject) {
        billingDAO.loadProject(testProject.projectName, txn).get
      }
    }
  }

  it should "delete a billing project" in withEmptyTestDatabase { dataSource =>
    val p1 = projectFromName("Project One")
    val p2 = projectFromName("Project Two")

    dataSource.inTransaction() { txn =>
      billingDAO.saveProject(p1, txn)
      billingDAO.saveProject(p2, txn)
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingBillingProjectVertices(graph, p1).nonEmpty
        }
        assert {
          getMatchingBillingProjectVertices(graph, p2).nonEmpty
        }
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          billingDAO.deleteProject(p1, txn)
        }
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingBillingProjectVertices(graph, p1).isEmpty
        }
        assert {
          getMatchingBillingProjectVertices(graph, p2).nonEmpty
        }
      }
    }
  }

  it should "not delete a non-existent billing project" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      assert {
        ! billingDAO.deleteProject(projectFromName("404 Group Not Found LOL"), txn)
      }
    }
  }

  it should "not delete a billing project twice" in withEmptyTestDatabase { dataSource =>
    val project = projectFromName("Group To Delete")

    dataSource.inTransaction() { txn =>
      billingDAO.saveProject(project, txn)
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingBillingProjectVertices(graph, project).nonEmpty
        }
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          billingDAO.deleteProject(project, txn)
        }
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingBillingProjectVertices(graph, project).isEmpty
        }
      }
    }

    dataSource.inTransaction() { txn =>
      assert {
        ! billingDAO.deleteProject(project, txn)
      }
    }
  }

  it should "add a User to a billing project" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      billingDAO.saveProject(testProject, txn)
    }

    dataSource.inTransaction() { txn =>
      val noUserProject = billingDAO.loadProject(testProject.projectName, txn).get
      assert {
        noUserProject.users.isEmpty
      }
    }

    dataSource.inTransaction() { txn =>
      authDAO.saveUser(testUser, txn)
      billingDAO.addUserToProject(testUser, testProject, txn)
    }

    dataSource.inTransaction() { txn =>
      val userProject = billingDAO.loadProject(testProject.projectName, txn).get
      assertResult(Set(testUserRef)) {
        userProject.users
      }
    }
  }

  it should "remove a User from a billing project" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      authDAO.saveUser(testUser, txn)
      billingDAO.saveProject(testProject.copy(users = Set(testUser)), txn)
    }

    dataSource.inTransaction() { txn =>
      val userProject = billingDAO.loadProject(testProject.projectName, txn).get
      assertResult(Set(testUserRef)) {
        userProject.users
      }
    }

    dataSource.inTransaction() { txn =>
      billingDAO.removeUserFromProject(testUser, testProject, txn)
    }

    dataSource.inTransaction() { txn =>
      val noUserProject = billingDAO.loadProject(testProject.projectName, txn).get
      assert {
        noUserProject.users.isEmpty
      }
    }
  }

  it should "list users in a billing project" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      authDAO.saveUser(testUser, txn)
      billingDAO.saveProject(RawlsBillingProject(RawlsBillingProjectName("1"), Set(testUser)), txn)
      billingDAO.saveProject(RawlsBillingProject(RawlsBillingProjectName("2"), Set(testUser)), txn)
      billingDAO.saveProject(RawlsBillingProject(RawlsBillingProjectName("3"), Set.empty), txn)
      billingDAO.saveProject(RawlsBillingProject(RawlsBillingProjectName("4"), Set(testUser)), txn)
    }

    dataSource.inTransaction() { txn =>
      val userProjects = billingDAO.listUserProjects(testUser, txn)
      assertResult(Seq(RawlsBillingProjectName("1"), RawlsBillingProjectName("2"), RawlsBillingProjectName("4"))) {
        userProjects
      }
    }
  }
}