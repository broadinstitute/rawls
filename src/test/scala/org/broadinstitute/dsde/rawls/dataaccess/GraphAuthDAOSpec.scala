package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Vertex, Graph, Direction}
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
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

          Seq(WorkspaceAccessLevels.Owner, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Read) foreach { level =>
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
          val vOwnerGroup = authDAO.getVertices(vAccessLevels, Direction.OUT, EdgeSchema.Ref, WorkspaceAccessLevels.Owner.toString).head
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

  it should "load a group" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels)
      workspaceDAO.save(workspace, txn)
    }

    // separate transaction so we aren't checking un-saved vertices
    dataSource.inTransaction() { txn =>
      withWorkspaceContext(testData.workspace, txn, bSkipLockCheck = true) { wc =>
        txn.withGraph { graph =>
          val ownerGroup = authDAO.loadGroup(UserAuth.toWorkspaceAccessGroupName(testData.workspace.toWorkspaceName, WorkspaceAccessLevels.Owner), txn)
          val vAccessLevels = authDAO.getVertices(wc.workspaceVertex, Direction.OUT, EdgeSchema.Own, "accessLevels").head
          val vOwnerGroup = authDAO.getVertices(vAccessLevels, Direction.OUT, EdgeSchema.Ref, WorkspaceAccessLevels.Owner.toString).head

          assert {
            (ownerGroup.groupName).equals(
              vOwnerGroup.getProperty("groupName"))
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

  it should "return the ACL for a user in a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val user = RawlsUser("obama@whitehouse.gov")
      val group = RawlsGroup("TopSecret", Set(user), Set.empty)

      authDAO.saveUser(user, txn)
      authDAO.saveGroup(group, txn)

      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels.updated(WorkspaceAccessLevels.Owner, group))
      workspaceDAO.save(workspace, txn)

      assertResult( WorkspaceAccessLevels.Owner ) {
        authDAO.getMaximumAccessLevel(user.userSubjectId, workspace.workspaceId, txn)
      }
    }
  }

  it should "choose the maximum access level for a user with multiple ACLs in a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val obama = RawlsUser("obama@whitehouse.gov")
      val group = RawlsGroup("TopSecret", Set(obama), Set.empty)
      val group2 = RawlsGroup("NotSoSecret", Set(obama), Set.empty)

      authDAO.saveUser(obama, txn)
      authDAO.saveGroup(group, txn)
      authDAO.saveGroup(group2, txn)

      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group, WorkspaceAccessLevels.Read -> group2))
      workspaceDAO.save(workspace, txn)

      assertResult( WorkspaceAccessLevels.Owner ) {
        authDAO.getMaximumAccessLevel(obama.userSubjectId, workspace.workspaceId, txn)
      }
    }
  }

  it should "return NoAccess when a user isn't associated with a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val obama = RawlsUser("obama@whitehouse.gov")
      val snowden = RawlsUser("snowden@iminurfiles.lol")
      val group = RawlsGroup("TopSecret", Set(obama), Set.empty)
      val group2 = RawlsGroup("NotSoSecret", Set(obama), Set.empty)

      authDAO.saveUser(obama, txn)
      authDAO.saveUser(snowden, txn)
      authDAO.saveGroup(group, txn)
      authDAO.saveGroup(group2, txn)

      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group, WorkspaceAccessLevels.Read -> group2))
      workspaceDAO.save(workspace, txn)

      assertResult(WorkspaceAccessLevels.NoAccess) {
        authDAO.getMaximumAccessLevel(snowden.userSubjectId, workspace.workspaceId, txn)
      }
    }
  }

  it should "find ACLs for users in subgroups" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val user = RawlsUser("obama@whitehouse.gov")
      val group2 = RawlsGroup("TotallySuperSecret", Set(user), Set.empty)
      val group = RawlsGroup("TopSecret", Set.empty, Set(group2))

      authDAO.saveUser(user, txn)
      authDAO.saveGroup(group2, txn)
      authDAO.saveGroup(group, txn)

      val levels = authDAO.createWorkspaceAccessGroups(testData.workspace.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspace.copy(accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group))
      workspaceDAO.save(workspace, txn)

      assertResult( WorkspaceAccessLevels.Owner ) {
        authDAO.getMaximumAccessLevel(user.userSubjectId, workspace.workspaceId, txn)
      }
    }
  }
}