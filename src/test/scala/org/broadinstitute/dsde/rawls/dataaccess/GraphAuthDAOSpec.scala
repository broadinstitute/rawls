package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Vertex, Graph, Direction}
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{FlatSpec, Matchers}
import spray.http.OAuth2BearerToken

import scala.collection.JavaConversions._

class GraphAuthDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {

  def getMatchingUserVertices(graph: Graph, user: RawlsUser): Iterable[Vertex] =
    graph.getVertices.filter(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.User) &&
        v.getProperty[String]("userSubjectId") == user.userSubjectId
    })

  def getMatchingGroupVertices(graph: Graph, group: RawlsGroup): Iterable[Vertex] =
    graph.getVertices.filter(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.Group) &&
        v.getProperty[String]("groupName") == group.groupName
    })

  val testUserInfo = UserInfo("dummy-emal@example.com", OAuth2BearerToken("dummy-token"), 0, "dummy-ID")
  val testUser = RawlsUser(testUserInfo.userSubjectId)

  "GraphAuthDAO" should "save a new User" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      authDAO.saveUser(testUser, txn)

      txn.withGraph { graph =>
        assert {
          getMatchingUserVertices(graph, testUser).nonEmpty
        }
      }
    }
  }

  it should "save a new Group" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val group = RawlsGroup("Empty Group", Set.empty, Set.empty)
      authDAO.saveGroup(group, txn)

      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group).nonEmpty
        }
      }
    }
  }

  it should "not save two copies of the same User" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      authDAO.saveUser(testUser, txn)
      authDAO.saveUser(testUser, txn)

      txn.withGraph { graph =>
        assertResult(1) {
          getMatchingUserVertices(graph, testUser).size
        }
      }
    }
  }

  it should "not save two copies of the same Group" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val group = RawlsGroup("Empty Group", Set.empty, Set.empty)
      authDAO.saveGroup(group, txn)
      authDAO.saveGroup(group, txn)

      txn.withGraph { graph =>
        assertResult(1) {
          getMatchingGroupVertices(graph, group).size
        }
      }
    }
  }

  it should "save a User to multiple Groups but not save two copies of the same User" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      authDAO.saveUser(testUser, txn)

      val group1 = RawlsGroup("Group 1 For User", Set(testUser), Set.empty)
      val group2 = RawlsGroup("Group 2 For User", Set(testUser), Set.empty)
      authDAO.saveGroup(group1, txn)
      authDAO.saveGroup(group2, txn)

      txn.withGraph { graph =>
        assertResult(1) {
          getMatchingUserVertices(graph, testUser).size
        }
        assertResult(1) {
          getMatchingGroupVertices(graph, group1).size
        }
        assertResult(1) {
          getMatchingGroupVertices(graph, group2).size
        }
      }
    }
  }

  it should "not save a new Group with missing users" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val user1 = RawlsUser("subjectId1")
      val user2 = RawlsUser("subjectId2")
      val group = RawlsGroup("Two User Group", Set(user1, user2), Set.empty)

      intercept[RawlsException] {
        // note that the users have not first been saved
        authDAO.saveGroup(group, txn)
      }

      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group).isEmpty
        }
      }
    }
  }

  it should "not save a new Group with missing groups" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val group1 = RawlsGroup("Group One", Set.empty, Set.empty)
      val group2 = RawlsGroup("Group Two", Set.empty, Set(group1))

      intercept[RawlsException] {
        // note that the first group has not first been saved
        authDAO.saveGroup(group2, txn)
      }

      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group2).isEmpty
        }
      }
    }
  }

  it should "save a subgroup hierarchy" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val group1 = RawlsGroup("Group One", Set.empty, Set.empty)
      val group2 = RawlsGroup("Group Two", Set.empty, Set(group1))
      val group3 = RawlsGroup("Group Three", Set.empty, Set(group1, group2))

      authDAO.saveGroup(group1, txn)
      authDAO.saveGroup(group2, txn)
      authDAO.saveGroup(group3, txn)

      txn.withGraph { graph =>
        Seq(group1, group2, group3) foreach { group =>
          assert {
            getMatchingGroupVertices(graph, group).nonEmpty
          }
        }
      }
    }
  }

  it should "save Workspace Access Groups as map properties on a Workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels)
      workspaceDAO.save(workspace, txn)
    }

    // separate transaction so we aren't checking un-saved vertices
    dataSource.inTransaction() { txn =>
      withWorkspaceContext(testData.workspace, txn, bSkipLockCheck = true) { wc =>
        txn.withGraph { graph =>
          val mapVertex = authDAO.getVertices(wc.workspaceVertex, Direction.OUT, EdgeSchema.Own, "accessLevels").head

          Seq(WorkspaceAccessLevel.Owner, WorkspaceAccessLevel.Write, WorkspaceAccessLevel.Read) foreach { level =>
            val levelFromWs = authDAO.getVertices(mapVertex, Direction.OUT, EdgeSchema.Ref, level.toString).head

            val levelGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(testData.workspace.toWorkspaceName, level), Set.empty, Set.empty)
            val levelVertices = getMatchingGroupVertices(graph, levelGroup)

            assertResult(1) {
              levelVertices.size
            }
            assert {
              levelVertices.head == levelFromWs
            }
          }
        }
      }
    }
  }

  it should "save the user to the Owner Workspace Access Group on a Workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels)
      workspaceDAO.save(workspace, txn)
    }

    // separate transaction so we aren't checking un-saved vertices
    dataSource.inTransaction() { txn =>
      withWorkspaceContext(testData.workspace, txn, bSkipLockCheck = true) { wc =>
        txn.withGraph { graph =>
          val vAccessLevels = authDAO.getVertices(wc.workspaceVertex, Direction.OUT, EdgeSchema.Own, "accessLevels").head
          val vOwnerGroup = authDAO.getVertices(vAccessLevels, Direction.OUT, EdgeSchema.Ref, WorkspaceAccessLevel.Owner.toString).head
          val vOwnerGroupUsers = authDAO.getVertices(vOwnerGroup, Direction.OUT, EdgeSchema.Own, "users").head
          val vOwnerGroupUser0 = authDAO.getVertices(vOwnerGroupUsers, Direction.OUT, EdgeSchema.Ref, "0").head

          val userVertices = getMatchingUserVertices(graph, testUser)

          assertResult(1) {
            userVertices.size
          }
          assert {
            userVertices.head == vOwnerGroupUser0
          }

        }
      }
    }
  }

  it should "not allow Workspace Access Groups to be created twice" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      intercept[RawlsException] {
        authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
        authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      }
    }
  }

}