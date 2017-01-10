package org.broadinstitute.dsde.rawls.util

object CollectionUtils {

  //A saner group by than Scala's.
  def groupByTuples[A, B]( tupleSeq: Seq[(A,B)] ): Map[A, Seq[B]] = {
    tupleSeq groupBy { case (a, b) => a } map { case (k, v) => k -> v.map(_._2) }
  }

  def groupByTuplesFlatten[A, B]( tupleSeq: Seq[(A, Seq[B])] ): Map[A, Seq[B]] = {
    tupleSeq groupBy { case (a,b) => a } map { case (k, v) => k -> v.flatMap(_._2) }
  }
}
