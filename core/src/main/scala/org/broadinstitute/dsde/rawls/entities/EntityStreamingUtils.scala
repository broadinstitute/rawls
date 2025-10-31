package org.broadinstitute.dsde.rawls.entities

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.{Concat, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.EntityAndAttributesResult
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.{Entity, EntityQuery, EntityQueryResponse, EntityQueryResultMetadata}
import spray.json._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

import java.util.concurrent.ConcurrentHashMap

object EntityStreamingUtils {

  def createResponseSource(entitySource: Source[Entity, _],
                           entityQuery: EntityQuery,
                           entityQueryResultMetadata: EntityQueryResultMetadata
  ): Source[ByteString, _] = {
    // create an EntityQueryResponse with no entities; this will be the shell for the response
    val entityQueryResponse = EntityQueryResponse(entityQuery, entityQueryResultMetadata, Seq.empty)
    // serialize to a String. This will contain "[]" where the entities should be
    val responseString = entityQueryResponse.toJson.prettyPrint
    // split the string on "[]"
    val foo = responseString.split("\\[]")
    // create Sources for the ByteStrings before "[]" and after "[]"
    val startSource = Source.single(ByteString(foo(0)))
    val endSource = Source.single(ByteString(foo(1)))

    // map the Source of entities to ByteStrings, wrapped in an array
    val entitiesByteStringSource: Source[ByteString, _] =
      entitySource
        .map { entity =>
          ByteString(entity.toJson.prettyPrint)
        }
        .intersperse(ByteString("["), ByteString(","), ByteString("]"))

    Source.combine(startSource, entitiesByteStringSource, endSource)(Concat(_))
  }

}
