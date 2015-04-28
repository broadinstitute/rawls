package org.broadinstitute.dsde.rawls.dataaccess

import java.io.File
import java.util.UUID

import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by dvoet on 4/24/15.
 */
class FileSystemWorkspaceDAOSpec extends FlatSpec with Matchers {
  val s1 = Entity("s1", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> AttributeList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))))
  val workspace = Workspace(
    "namespace",
    UUID.randomUUID().toString,
    DateTime.now().withMillis(0),
    "test",
    Map(
      "samples" -> Map("s1" -> s1),
      "individuals" -> Map("i" -> Entity("i", Map("samples" -> AttributeList(Seq(AttributeReference("samples", "s2"), AttributeReference("samples", "s1"))))))
    )
  )

  val storageDir = new File(System.getProperty("java.io.tmpdir"))
  val dao = new FileSystemWorkspaceDAO(storageDir)
  val namespaceDir = new File(storageDir, workspace.namespace)
  val workspaceFile = new File(namespaceDir, workspace.name)
  workspaceFile.deleteOnExit()

  "WorkspaceDAO" should "save a workspace" in {
    assertResult(false) { workspaceFile.exists() }
    dao.save(workspace)
    assertResult(true) { workspaceFile.exists() }
  }

  it should "load a workspace" in {
    assertResult(workspace) { dao.load(workspace.namespace, workspace.name) }
    assertResult(Seq(None, Option(s1))) {
      for(("samples", AttributeList(x)) <- workspace.entities("individuals")("i").attributes;
        AttributeReference(a,b) <- x
      ) yield (AttributeReference(a,b).resolve(workspace))
    }
  }

  it should "throw an exception when a workspace does not exist" in {
    intercept[WorkspaceDoesNotExistException] { dao.load(workspace.namespace, workspace.name+"x") }
  }
}
