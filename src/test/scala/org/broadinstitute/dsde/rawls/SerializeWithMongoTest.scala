package org.broadinstitute.dsde.rawls

import com.mongodb.util.JSON
import org.broadinstitute.dsde.rawls.TestClass._
import org.scalatest.{FunSuite, BeforeAndAfter}
import spray.json._

import com.mongodb.casbah.Imports._

import scala.util.Random

/**
 * Created by abaumann on 4/21/15.
 */
class SerializeWithMongoTest extends FunSuite with BeforeAndAfter {
  var mongoClient:MongoClient = _
  var mongoDb:MongoDB = _
  var mongoColl:MongoCollection = _

  before {
    mongoClient = MongoClient("localhost", 27017)
    mongoDb = mongoClient("test" + new Random().nextInt())
    mongoColl = mongoDb("serializeTest")
  }

  after {
    mongoDb.dropDatabase()
  }

  ignore("Serialize, write to Mongo, Deserialize returns the same object") {
    val tc = TestClass(33)

    // to JSON AST representation
    val tcAsJsonAst = tc.toJson
    // JSON AST to JSON string
    val tcAsJsonString = tcAsJsonAst.compactPrint

    // create Mongo appropriate DBObject
    val tcAsJsonDbObject = JSON.parse(tcAsJsonString).asInstanceOf[DBObject]

    // insert this object
    mongoColl.insert(tcAsJsonDbObject)

    // we only have one object in the collection, so find first item
    // in the collection.  An Option is returned, it exists so we get
    // and then toString is in JSON form.
    // TODO: we had difficulty finding how to get back the original
    // TODO: JSON and this looked like the best option
    val tcBackString = mongoColl.findOne().get.toString

    // deserialize JSON back to TestClass
    val tcBack = tcBackString.parseJson.convertTo[TestClass]

    // assert the TestClass inserted into MongoDB and read back in
    // is the same as the original object
    assert(tcBack == tc)

    mongoColl.drop()
  }
}
