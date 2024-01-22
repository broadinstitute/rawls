package org.broadinstitute.dsde.rawls.entities

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  Entity,
  EntityColumnFilter,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  FilterOperators,
  SortDirections,
  WorkspaceFieldSpecs
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.scalatest.concurrent.ScalaFutures

import java.nio.charset.Charset

class EntityStreamingUtilsSpec extends AnyFlatSpec with Matchers with ScalaFutures with TestDriverComponent {

  // exemplar data used in multiple tests
  val entityQueryParameters: EntityQuery =
    EntityQuery(
      1,
      2,
      "foo",
      SortDirections.Descending,
      Option("filterTerm"),
      FilterOperators.Or,
      WorkspaceFieldSpecs(),
      Option(EntityColumnFilter(AttributeName.withDefaultNS("attr"), "colFilterValue"))
    )
  val entityQueryResultMetadata: EntityQueryResultMetadata = EntityQueryResultMetadata(23, 56, 89)

  // helper method to create the Source from createResponseSource, then parse that Source back into an EntityQueryResponse
  private def serdesViaSource(entities: Seq[Entity],
                              entityQuery: EntityQuery,
                              entityQueryResultMetadata: EntityQueryResultMetadata
  ): EntityQueryResponse = {
    val actual: Source[ByteString, _] =
      EntityStreamingUtils.createResponseSource(Source.fromIterator(() => entities.iterator),
                                                entityQuery,
                                                entityQueryResultMetadata
      )

    // convert the actual Source into a single String
    val sink = Sink.fold[String, ByteString]("") { case (acc, str) =>
      acc + str.decodeString(Charset.defaultCharset())
    }
    implicit val actorSystem: ActorSystem = ActorSystem()
    val actualString: String = actual.runWith(sink).futureValue

    // now, parse that string back into an object
    actualString.parseJson.convertTo[EntityQueryResponse]
  }

  private def assertSourceStreamFor(entities: Seq[Entity]): Unit = {
    val entityQueryResponse: EntityQueryResponse =
      serdesViaSource(entities, entityQueryParameters, entityQueryResultMetadata)

    entityQueryResponse.results shouldBe entities
    entityQueryResponse.parameters shouldBe entityQueryParameters
    entityQueryResponse.resultMetadata shouldBe entityQueryResultMetadata
  }

  behavior of "EntityStreamingUtils.createResponseSource"

  it should "serialize when zero entities" in {
    val entities: Seq[Entity] = Seq.empty[Entity]
    assertSourceStreamFor(entities)
  }

  it should "serialize when one entity" in {
    val constantTestData = new ConstantTestData()
    val entities: Seq[Entity] = Seq(constantTestData.aliquot1)
    assertSourceStreamFor(entities)
  }

  it should "serialize when multiple entities" in {
    val constantTestData = new ConstantTestData()
    val entities: Seq[Entity] = Seq(constantTestData.aliquot1, constantTestData.aliquot2, constantTestData.sample1)
    assertSourceStreamFor(entities)
  }

}
