package org.broadinstitute.dsde.rawls.dataaccess

import org.scalatest.{Matchers, FlatSpec}
import scala.collection.JavaConversions._

/**
 * Created by dvoet on 5/19/15.
 */
class DataSourceSpec extends FlatSpec with Matchers {
  "DataSource" should "commit txn" in {
    val ds = DataSource("memory:DataSource", "admin", "admin")
    ds inTransaction { txn =>
      txn withGraph { graph =>
        val v = graph.addVertex(null)
        v.setProperty("foo", "bar")
      }
    }

    assertResult(List("bar")) {
      ds inTransaction { txn =>
        txn withGraph { graph =>
          val l = graph.getVertices("foo", "bar").toList
          l.map(_.getProperty("foo").asInstanceOf[String])
        }
      }
    }
  }

  it should "rollback txn on exception" in {
    val ds = DataSource("memory:DataSource", "admin", "admin")
    intercept[RuntimeException] {
      ds inTransaction { txn =>
        txn withGraph { graph =>
          val v = graph.addVertex(null)
          v.setProperty("foo2", "bar")
        }
        throw new RuntimeException
      }
    }

    assertResult(List()) {
      ds inTransaction { txn =>
        txn withGraph { graph =>
          val l = graph.getVertices("foo2", "bar").toList
          l.map(_.getProperty("foo2").asInstanceOf[String])
        }
      }
    }
  }

  it should "rollback txn on call to rollback" in {
    val ds = DataSource("memory:DataSource", "admin", "admin")
    ds inTransaction { txn =>
      txn withGraph { graph =>
        val v = graph.addVertex(null)
        v.setProperty("foo2", "bar")
        txn.setRollbackOnly()

        val v2 = graph.addVertex(null)
        v2.setProperty("foo3", "bar")
      }
    }

    assertResult(List()) {
      ds inTransaction { txn =>
        txn withGraph { graph =>
          val l = graph.getVertices("foo2", "bar").toList
          l.map(_.getProperty("foo2").asInstanceOf[String])
        }
      }
    }
    assertResult(List()) {
      ds inTransaction { txn =>
        txn withGraph { graph =>
          val l = graph.getVertices("foo3", "bar").toList
          l.map(_.getProperty("foo3").asInstanceOf[String])
        }
      }
    }
  }

}
