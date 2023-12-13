package org.broadinstitute.dsde.rawls.entities

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.EntityAndAttributesResult
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.Entity

object EntityStreamingUtils {

  /**
    * Given a Source containing entity attributes, scroll through that source and combine
    * attributes into entities. Emit a Source of entities.
    * <p>
    * IMPORTANT: this !!requires!! that the incoming Source of attributes is ordered by
    * entity ID. If the incoming source is not properly ordered, this method will emit
    * incomplete/duplicate entities.
    * <p>
    * Only used internally by EntityService.listEntities, but public to support unit testing
    *
    * @param dbSource the Source of attributes, typically from a database stream
    * @return a Source of entities constructed from the attributes
    */
  def gatherEntities(dataSource: SlickDataSource,
                     dbSource: Source[EntityAndAttributesResult, NotUsed]
  ): Source[Entity, NotUsed] = {
    // interim classes used while iterating through the stream, allows us to accumulate attributes
    // until ready to emit an entity
    trait AttributeStreamElement
    case class AttrAccum(accum: Seq[EntityAndAttributesResult], entity: Option[Entity]) extends AttributeStreamElement
    case object EmptyElement extends AttributeStreamElement

    /*
     * Given the previous and current stream elements, which are produced by the EntityCollector,
     * calculate the AttrAccum to be output
     */
    def gatherOrOutput(previous: AttributeStreamElement, current: AttributeStreamElement): AttrAccum = {
      // utility function called when an entity is finished or when the stream is finished
      def entityFinished(prevAttrs: Seq[EntityAndAttributesResult], nextAttrs: Seq[EntityAndAttributesResult]) = {
        val unmarshalled = dataSource.dataAccess.entityQuery.unmarshalEntities(prevAttrs)
        // safety check - did the attributes we gathered all marshal into a single entity?
        if (unmarshalled.size != 1)
          throw new DataEntityException(s"gatherOrOutput expected only one entity, found ${unmarshalled.size}")
        AttrAccum(nextAttrs, Some(unmarshalled.head))
      }

      // inspect the variations of previous and current
      (previous, current) match {
        // if both previous and current are empty, it means no entities found
        case (EmptyElement, EmptyElement) =>
          AttrAccum(Seq(), None)

        // if previous is empty but current is not, it's the first element
        case (EmptyElement, curr: AttrAccum) =>
          curr

        // midstream, we notice that the current entity is the same as the previous entity.
        // keep gathering attributes for this entity, and don't emit an entity yet.
        case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id == curr.accum.head.entityRecord.id =>
          val newAccum = prev.accum ++ curr.accum
          AttrAccum(newAccum, None)

        // midstream, we notice that the current entity's id is greater than the previous entity's id.
        // take all the attributes we have gathered for the previous entity,
        // marshal them into an Entity object, emit that Entity, and start a new accumulator
        // for the new/current entity
        case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id < curr.accum.head.entityRecord.id =>
          entityFinished(prev.accum, curr.accum)

        // midstream, we notice that the current entity's id is LESS than the previous entity's id.
        // this breaks the assumption that entities are ordered by their ids ascending, and indicates a coding
        // error. Throw an exception.
        case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id > curr.accum.head.entityRecord.id =>
          throw new RawlsException(
            "Unexpected internal error; the previous results may be incomplete. Cause: entity source input is in unexpected order."
          )

        // if current is empty but previous is not, it means the stream has finished.
        // Marshal and output the final Entity.
        case (prev: AttrAccum, EmptyElement) =>
          entityFinished(prev.accum, Seq())

        // relief valve, this should not happen, but if it does we should log it
        case _ =>
          throw new Exception(
            s"gatherOrOutput encountered unexpected input, cannot continue. Prev: $previous :: Curr: $current"
          )
      }
    }

    /* custom stream stage that allows us to compare the current stream element
       to the previous stream element. In turn, this allows us to accumulate attributes
       until we notice that the current element is from a different entity than the previous attribute;
       when that happens, we marshal and emit an entity.
     */
    class EntityCollector extends GraphStage[FlowShape[AttrAccum, AttrAccum]] {
      val in = Inlet[AttrAccum]("EntityCollector.in")
      val out = Outlet[AttrAccum]("EntityCollector.out")
      override val shape = FlowShape(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
        private var prev: AttributeStreamElement = EmptyElement // note: var!

        // if our downstream pulls on us, propagate that pull to our upstream
        setHandler(out,
                   new OutHandler {
                     override def onPull(): Unit = pull(in)
                   }
        )

        setHandler(
          in,
          new InHandler {
            // when a new element arrives ...
            override def onPush(): Unit = {
              // send it to gatherOrOutput which has most of the logic
              val next = gatherOrOutput(prev, grab(in))
              // save the current element to "prev" to prepare for the next iteration
              prev = next
              // emit whatever gatherOrOutput returned
              emit(out, next)
            }

            // when the upstream finishes ...
            override def onUpstreamFinish(): Unit = {
              // ensure we marshal and emit the last entity
              emit(out, gatherOrOutput(prev, EmptyElement))
              completeStage()
            }
          }
        )
      }
    }

    val pipeline = dbSource
      .map(entityAndAttributesResult =>
        AttrAccum(Seq(entityAndAttributesResult), None)
      ) // transform EntityAndAttributesResult to AttrAccum
      .via(new EntityCollector()) // execute the business logic to accumulate attributes and emit entities
      .collect { // "flatten" the stream to only emit entities
        case AttrAccum(_, Some(entity)) => entity
      }
      .log("gatherEntities")
      .addAttributes(
        Attributes.logLevels(onElement = Attributes.LogLevels.Debug,
                             onFinish = Attributes.LogLevels.Info,
                             onFailure = Attributes.LogLevels.Error
        )
      )

    Source.fromGraph(pipeline) // return a Source, which akka-http natively knows how to stream to the caller
  }

}
