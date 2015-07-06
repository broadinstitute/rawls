package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.{RawlsException, VertexProperty}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field
import scala.collection.JavaConversions._

class GraphDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  lazy val dao: GraphDAO = new GraphDAO() {}

  val test = Test("a", Option("hi"), "b", 4, true, 3.3)

  val expected = Map(
  "_s" -> "a",
  "_op" -> "hi",
  "_i" -> 4,
  "_b" -> true,
  "_d" -> 3.3,
  "_clazz" -> test.getClass.getSimpleName
  )

  "GraphDAO" should "update a vertex" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      txn.withGraph { graph =>
        val vertex = dao.setVertexProperties(test, dao.addVertex(graph, null))

        assertResult(expected) {
          vertex.getPropertyKeys.map(key => key -> vertex.getProperty(key)).toMap
        }
      }
    }
  }

  it should "create case class from vertex" in withEmptyTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      txn.withGraph { graph =>
        val vertex = graph.addVertex(null)
        vertex.setProperty("_s", "a")
        vertex.setProperty("_op", "hi")
        vertex.setProperty("_i", 4)
        vertex.setProperty("_b", true)
        vertex.setProperty("_d", 3.3)
        vertex.setProperty("_clazz", test.getClass.getSimpleName)

        assertResult(test) {
          dao.fromVertex[Test](vertex, Map("x" -> "b"))
        }

        vertex.removeProperty("_op")
        assertResult(test.copy(op = None)) {
          dao.fromVertex[Test](vertex, Map("x" -> "b"))
        }

        intercept[RawlsException] {
          dao.fromVertex[Test](vertex, Map())
        }

        vertex.removeProperty("_s")
        intercept[RawlsException] {
          dao.fromVertex[Test](vertex, Map("x" -> "b"))
        }

      }
    }
  }
}

case class Test(@(VertexProperty@field) val s: String,
                @(VertexProperty@field) val op: Option[String],
                val x: String,
                @(VertexProperty@field) val i: Int,
                @(VertexProperty@field) val b: Boolean,
                @(VertexProperty@field) val d: Double)
