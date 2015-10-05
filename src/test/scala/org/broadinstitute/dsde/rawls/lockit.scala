package org.broadinstitute.dsde.rawls

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientElement, OrientGraph, OrientGraphFactory}
import org.scalatest.FlatSpecLike

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class lockit extends FlatSpecLike {

  "blarg" should "pass" in {
    val dbName = "blah"
    val factory = new OrientGraphFactory("memory:" + dbName, "admin", "admin")
    factory.setThreadMode(THREAD_MODE.MANUAL)
    factory.setAutoStartTx(false)
    factory.setUseClassForEdgeLabel(false)
    factory.setUseLightweightEdges(true)

    //Initial setup of DB.
    val graph = factory.getTx
    val root = graph.asInstanceOf[OrientGraph].addVertex(s"class:Test", Map.empty[String, Object].asJava)
    val v2 = graph.asInstanceOf[OrientGraph].addVertex(s"class:OtherSide", Map.empty[String, Object].asJava)
    graph.begin()
    root.addEdge("foo", v2)
    graph.commit()

    new Thread(new Runnable {
      def run() {
        //Thread switches out the vertex on the other side of the "foo" edge.
        val graph = factory.getTx
        graph.begin()
        println("t1 open")
        Thread.sleep(400)
        val root = graph.asInstanceOf[OrientGraph].getVerticesOfClass("Test").head
        root.asInstanceOf[OrientElement].lock(true)
        root.getEdges(Direction.OUT, "foo").map(v => graph.removeVertex(v.getVertex(Direction.IN)))
        println("t1 removed vtx")

        val v2 = graph.asInstanceOf[OrientGraph].addVertex(s"class:OtherSide", Map.empty[String, Object].asJava)
        root.addEdge("foo", v2)
        root.asInstanceOf[OrientElement].unlock()
        println("t1 readded vtx")

        graph.commit()
      }
    }).start()

    Thread.sleep(500)
    val graph2 = factory.getTx
    graph2.begin()

    //If I move this line after the 550 sleep (i.e. after t1 commits), it's fine.
    val root2 = graph2.asInstanceOf[OrientGraph].getVerticesOfClass("Test").head
    root2.asInstanceOf[OrientElement].lock(false)
    println("t2 got root")
    Thread.sleep(550)

    println("t2 looking for verts")
    //This assertion fails.
    assert(root2.getEdges(Direction.OUT, "foo").size > 0)
    root2.asInstanceOf[OrientElement].unlock()
  }
}
