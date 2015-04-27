package org.broadinstitute.dsde.rawls.dataaccess

import java.io.File
import java.util.{Date, UUID}

import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceDAOSpec extends FlatSpec with Matchers {
  val s1 = Entity("s1", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> AttributeList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))))
  val workspace = Workspace(
    UUID.randomUUID().toString,
    DateTime.now().withMillis(0),
    "test",
    Map(
      "samples" -> Map("s1" -> s1),
      "individuals" -> Map("i" -> Entity("i", Map("samples" -> AttributeList(Seq(AttributeReference("samples", "s2"), AttributeReference("samples", "s1"))))))
    )
  )

  val storageDir = new File(System.getProperty("java.io.tmpdir"))
  val dao = new WorkspaceDAO(storageDir)

  "WorkspaceDAO" should "save a workspace" in {
    assertResult(false) { new File(storageDir, workspace.name).exists() }
    dao.save(workspace)
    assertResult(true) { new File(storageDir, workspace.name).exists() }
  }

  it should "load a workspace" in {
    assertResult(workspace) { dao.load(workspace.name) }
    assertResult(Seq(None, Option(s1))) {
      for(("samples", AttributeList(x)) <- workspace.entities("individuals")("i").attributes;
        AttributeReference(a,b) <- x
      ) yield (AttributeReference(a,b).resolve(workspace))
    }
  }
}
