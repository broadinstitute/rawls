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
        v.getProperty[String]("userSubjectId") == user.userSubjectId.value
    })

  def getMatchingGroupVertices(graph: Graph, group: RawlsGroup): Iterable[Vertex] =
    graph.getVertices.filter(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.Group) &&
        v.getProperty[String]("groupName") == group.groupName.value
    })

  val testUserInfo = UserInfo("dummy-emal@example.com", OAuth2BearerToken("dummy-token"), 0, "dummy-ID")
  val testUser = RawlsUser(testUserInfo)

  def userFromId(subjectId: String) =
    RawlsUser(RawlsUserSubjectId(subjectId), RawlsUserEmail("dummy@example.com"))

  def groupFromName(name: String) =
    RawlsGroup(RawlsGroupName(name), RawlsGroupEmail("dummy@example.com"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

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
      val group = groupFromName("Empty Group")
      authDAO.saveGroup(group, txn)

      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group).nonEmpty
        }
      }
    }
  }

  it should "delete a Group" in withDefaultTestDatabase { dataSource =>
    val group1 = groupFromName("Group One")
    val group2 = groupFromName("Group Two")

    dataSource.inTransaction() { txn =>
      authDAO.saveGroup(group1, txn)
      authDAO.saveGroup(group2, txn)
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group1).nonEmpty
        }
        assert {
          getMatchingGroupVertices(graph, group2).nonEmpty
        }
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        authDAO.deleteGroup(group1, txn)
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group1).isEmpty
        }
        assert {
          getMatchingGroupVertices(graph, group2).nonEmpty
        }
      }
    }
  }

  it should "not delete a non-existent group" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      intercept[RawlsException] {
        authDAO.deleteGroup(groupFromName("404 Group Not Found LOL"), txn)
      }
    }
  }

  it should "not delete a group twice" in withDefaultTestDatabase { dataSource =>
    val group = groupFromName("Group To Delete")

    dataSource.inTransaction() { txn =>
      authDAO.saveGroup(group, txn)
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group).nonEmpty
        }
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        authDAO.deleteGroup(group, txn)
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group).isEmpty
        }
      }
    }

    dataSource.inTransaction() { txn =>
      intercept[RawlsException] {
        authDAO.deleteGroup(group, txn)
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
      val group = groupFromName("Empty Group")
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

      val group1 = makeRawlsGroup("Group 1 For User", Set(testUser), Set.empty)
      val group2 = makeRawlsGroup("Group 2 For User", Set(testUser), Set.empty)
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
    val user1 = userFromId("subjectId1")
    val user2 = userFromId("subjectId2")
    val group = makeRawlsGroup("Two User Group", Set(user1, user2), Set.empty)

    intercept[RawlsException] {
      dataSource.inTransaction() { txn =>
        // note that the users have not first been saved
        authDAO.saveGroup(group, txn)
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group).isEmpty
        }
      }
    }
  }

  it should "not save a new Group with missing groups" in withDefaultTestDatabase { dataSource =>
    val group1 = makeRawlsGroup("Group One", Set.empty, Set.empty)
    val group2 = makeRawlsGroup("Group Two", Set.empty, Set(group1))

    intercept[RawlsException] {
      dataSource.inTransaction() { txn =>
        // note that the first group has not first been saved
        authDAO.saveGroup(group2, txn)
      }
    }

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assert {
          getMatchingGroupVertices(graph, group2).isEmpty
        }
      }
    }
  }

  it should "save a subgroup hierarchy" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val group1 = makeRawlsGroup("Group One", Set.empty, Set.empty)
      val group2 = makeRawlsGroup("Group Two", Set.empty, Set(group1))
      val group3 = makeRawlsGroup("Group Three", Set.empty, Set(group1, group2))

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
      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels)
      workspaceDAO.save(workspace, txn)
    }

    // separate transaction so we aren't checking un-saved vertices
    dataSource.inTransaction() { txn =>
      withWorkspaceContext(testData.workspaceNoGroups, txn, bSkipLockCheck = true) { wc =>
        txn.withGraph { graph =>
          val mapVertex = authDAO.getVertices(wc.workspaceVertex, Direction.OUT, EdgeSchema.Own, "accessLevels").head

          WorkspaceAccessLevels.groupAccessLevelsAscending foreach { level =>
            val levelFromWs = authDAO.getVertices(mapVertex, Direction.OUT, EdgeSchema.Ref, level.toString).head

            val levelGroup = newRawlsGroup(testData.workspaceNoGroups.toWorkspaceName, level)
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
      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels)
      workspaceDAO.save(workspace, txn)
    }

    // separate transaction so we aren't checking un-saved vertices
    dataSource.inTransaction() { txn =>
      withWorkspaceContext(testData.workspaceNoGroups, txn, bSkipLockCheck = true) { wc =>
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

  it should "delete Workspace Access Groups" in withEmptyTestDatabase { dataSource =>
    val context = dataSource.inTransaction() { txn =>
      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels)
      workspaceDAO.save(workspace, txn)
    }

    // first confirm the groups exist, and save them

    val groups = dataSource.inTransaction() { txn =>
      withWorkspaceContext(testData.workspaceNoGroups, txn, bSkipLockCheck = true) { wc =>
        txn.withGraph { graph =>
          val mapVertex = authDAO.getVertices(wc.workspaceVertex, Direction.OUT, EdgeSchema.Own, "accessLevels").head

          WorkspaceAccessLevels.groupAccessLevelsAscending map { level =>
            val levelFromWs = authDAO.getVertices(mapVertex, Direction.OUT, EdgeSchema.Ref, level.toString).head

            val levelGroup = newRawlsGroup(testData.workspaceNoGroups.toWorkspaceName, level, Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
            val levelVertices = getMatchingGroupVertices(graph, levelGroup)

            assertResult(1) {
              levelVertices.size
            }
            assert {
              levelVertices.head == levelFromWs
            }

            levelGroup
          }
        }
      }
    }

    dataSource.inTransaction() { txn =>
      authDAO.deleteWorkspaceAccessGroups(context.workspace, txn)
    }

    // then confirm they do not

    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        assertResult(0) {
          groups map { group =>
            getMatchingGroupVertices(graph, group).size
          } sum
        }
      }
    }

    // and also confirm that we have not left any stray vertices besides Workspace and User

    dataSource.inTransaction() { txn =>
      workspaceDAO.delete(context.workspace.toWorkspaceName, txn)

      txn.withGraph { graph =>
        val wsOwnerVertex = getMatchingUserVertices(graph, testUser).head
        authDAO.removeObject(wsOwnerVertex, graph)

        assertResult(0) {
          graph.getVertices.toList.size
        }
      }
    }

  }

  it should "return the ACL for a user in a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val user = userFromId("obama@whitehouse.gov")
      val group = makeRawlsGroup("TopSecret", Set(user), Set.empty)

      authDAO.saveUser(user, txn)
      authDAO.saveGroup(group, txn)

      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels.updated(WorkspaceAccessLevels.Owner, group))
      workspaceDAO.save(workspace, txn)

      assertResult( WorkspaceAccessLevels.Owner ) {
        authDAO.getMaximumAccessLevel(user, workspace.workspaceId, txn)
      }
    }
  }

  it should "choose the maximum access level for a user with multiple ACLs in a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val obama = userFromId("obama@whitehouse.gov")
      val group = makeRawlsGroup("TopSecret", Set(obama), Set.empty)
      val group2 = makeRawlsGroup("NotSoSecret", Set(obama), Set.empty)

      authDAO.saveUser(obama, txn)
      authDAO.saveGroup(group, txn)
      authDAO.saveGroup(group2, txn)

      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group, WorkspaceAccessLevels.Read -> group2))
      workspaceDAO.save(workspace, txn)

      assertResult( WorkspaceAccessLevels.Owner ) {
        authDAO.getMaximumAccessLevel(obama, workspace.workspaceId, txn)
      }
    }
  }

  it should "return NoAccess when a user isn't associated with a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val obama = userFromId("obama@whitehouse.gov")
      val snowden = userFromId("snowden@iminurfiles.lol")
      val group = makeRawlsGroup("TopSecret", Set(obama), Set.empty)
      val group2 = makeRawlsGroup("NotSoSecret", Set(obama), Set.empty)

      authDAO.saveUser(obama, txn)
      authDAO.saveUser(snowden, txn)
      authDAO.saveGroup(group, txn)
      authDAO.saveGroup(group2, txn)

      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group, WorkspaceAccessLevels.Read -> group2))
      workspaceDAO.save(workspace, txn)

      assertResult(WorkspaceAccessLevels.NoAccess) {
        authDAO.getMaximumAccessLevel(snowden, workspace.workspaceId, txn)
      }
    }
  }

  it should "find ACLs for users in subgroups" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val user = userFromId("obama@whitehouse.gov")
      val group2 = makeRawlsGroup("TotallySuperSecret", Set(user), Set.empty)
      val group = makeRawlsGroup("TopSecret", Set.empty, Set(group2))

      authDAO.saveUser(user, txn)
      authDAO.saveGroup(group2, txn)
      authDAO.saveGroup(group, txn)

      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace = testData.workspaceNoGroups.copy(accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group))
      workspaceDAO.save(workspace, txn)

      assertResult( WorkspaceAccessLevels.Owner ) {
        authDAO.getMaximumAccessLevel(user, workspace.workspaceId, txn)
      }
    }
  }

  it should "find workspaces for users in subgroups" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction() { txn =>
      val user = userFromId("obama@whitehouse.gov")
      val group2 = makeRawlsGroup("TotallySuperSecret", Set(user), Set.empty)
      val group = makeRawlsGroup("TopSecret", Set.empty, Set(group2))

      authDAO.saveUser(user, txn)
      authDAO.saveGroup(group2, txn)
      authDAO.saveGroup(group, txn)

      val levels = createWorkspaceAccessGroups(authDAO, testData.workspaceNoGroups.toWorkspaceName, testUserInfo, txn)
      val workspace1 = testData.workspaceNoGroups.copy(name = "1", workspaceId = "1", accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Owner -> group))
      val workspace2 = testData.workspaceNoGroups.copy(name = "2", workspaceId = "2", accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Write -> group))
      val workspace3 = testData.workspaceNoGroups.copy(name = "3", workspaceId = "3", accessLevels = levels ++ Map[WorkspaceAccessLevel, RawlsGroupRef](WorkspaceAccessLevels.Read -> group))
      val workspace4 = testData.workspaceNoGroups.copy(name = "4", workspaceId = "4", accessLevels = levels)
      workspaceDAO.save(workspace1, txn)
      workspaceDAO.save(workspace2, txn)
      workspaceDAO.save(workspace3, txn)
      workspaceDAO.save(workspace4, txn)

      val result = authDAO.listWorkspaces(user, txn)
      assertResult( Set(WorkspacePermissionsPair(workspace1.workspaceId, WorkspaceAccessLevels.Owner), WorkspacePermissionsPair(workspace2.workspaceId, WorkspaceAccessLevels.Write), WorkspacePermissionsPair(workspace3.workspaceId, WorkspaceAccessLevels.Read)) ) {
        result.toSet
      }
      assertResult( 3 ) {
        result.size
      }
    }
  }

  def createWorkspaceAccessGroups(authDAO: AuthDAO, workspaceName: WorkspaceName, userInfo: UserInfo, txn: RawlsTransaction): Map[WorkspaceAccessLevel, RawlsGroupRef] = {
    val user = RawlsUser(userInfo)
    authDAO.saveUser(user, txn)

    // add user to Owner group only
    val oGroup = newRawlsGroup(workspaceName, WorkspaceAccessLevels.Owner, Set[RawlsUserRef](user), Set.empty[RawlsGroupRef])
    val wGroup = newRawlsGroup(workspaceName, WorkspaceAccessLevels.Write)
    val rGroup = newRawlsGroup(workspaceName, WorkspaceAccessLevels.Read)
    authDAO.saveGroup(oGroup, txn)
    authDAO.saveGroup(wGroup, txn)
    authDAO.saveGroup(rGroup, txn)

    Map(
      WorkspaceAccessLevels.Owner -> oGroup,
      WorkspaceAccessLevels.Write -> wGroup,
      WorkspaceAccessLevels.Read -> rGroup)
  }


  // for Workspace Access Groups
  def newRawlsGroup(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel): RawlsGroup =
    newRawlsGroup(workspaceName, accessLevel, Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

  // for Workspace Access Groups
  def newRawlsGroup(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, users: Set[RawlsUserRef], groups: Set[RawlsGroupRef]): RawlsGroup = {
    val name = RawlsGroupName(s"${workspaceName.namespace}-${workspaceName.name}-${accessLevel.toString}")
    RawlsGroup(name, RawlsGroupEmail(""), users, groups)
  }

}