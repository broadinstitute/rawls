package org.broadinstitute.dsde.rawls.dataaccess

import java.nio.file.{Files, Paths}
import java.util.UUID

import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by dvoet on 4/24/15.
 */
class FileSystemWorkspaceDAOSpec extends FlatSpec with Matchers {
  val wsns = "namespace"
  val wsname = UUID.randomUUID().toString
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))), WorkspaceName(wsns, wsname))
  val workspace = Workspace(
    wsns,
    wsname,
    DateTime.now().withMillis(0),
    "test",
    Map(
      "samples" -> Map("s1" -> s1),
      "individuals" -> Map("i" -> Entity("i", "individuals", Map("samples" -> AttributeReferenceList(Seq(AttributeReferenceSingle("samples", "s2"), AttributeReferenceSingle("samples", "s1")))), WorkspaceName(wsns, wsname)))
    )
  )

  val storageDir = Paths.get(System.getProperty("java.io.tmpdir"), "rawls-test")
  val dao = new FileSystemWorkspaceDAO(storageDir)
  val namespaceDir = storageDir.resolve(workspace.namespace)
  val workspaceFile = namespaceDir.resolve(workspace.name)
  workspaceFile.toFile.deleteOnExit()

  "WorkspaceDAO" should "save a workspace" in {
    assertResult(false) { Files.exists(workspaceFile) }
    dao.save(workspace)
    assertResult(true) { Files.exists(workspaceFile) }
  }

  it should "load a workspace" in {
    assertResult(Some(workspace)) { dao.load(workspace.namespace, workspace.name) }
    assertResult(Seq(None, Option(s1))) {
      for(("samples", AttributeReferenceList(x)) <- workspace.entities("individuals")("i").attributes;
        AttributeReferenceSingle(a,b) <- x
      ) yield (AttributeReferenceSingle(a,b).resolve(workspace))
    }
  }

  it should "return None when a workspace does not exist" in {
    assertResult(None) { dao.load(workspace.namespace, workspace.name+"x") }
  }

  it should "show workspace in list" in {
    val results = dao.list()
    assertResult(true) { results.contains(WorkspaceShort(workspace.namespace, workspace.name, workspace.createdDate, workspace.createdBy)) }
  }
}
