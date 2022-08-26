package org.broadinstitute.dsde.rawls.util

import cats.Monoid
import cats.instances.list._
import cats.instances.map._
import cats.syntax.foldable._

object CollectionUtils {

  // A saner group by than Scala's.
  def groupByTuples[A, B](tupleSeq: Seq[(A, B)]): Map[A, Seq[B]] =
    tupleSeq groupBy { case (a, b) => a } map { case (k, v) => k -> v.map(_._2) }

  def groupByTuplesSet[A, B](tupleSet: Set[(A, B)]): Map[A, Set[B]] =
    tupleSet groupBy { case (a, b) => a } map { case (k, v) => k -> v.map(_._2) }

  def groupByTuplesFlatten[A, B](tupleSeq: Seq[(A, Seq[B])]): Map[A, Seq[B]] =
    tupleSeq groupBy { case (a, b) => a } map { case (k, v) => k -> v.flatMap(_._2) }

  /**
    * Converts a `Seq[(A, B)]` into a `Map[A, B]`, combining the values with a `Monoid[B]` in case of key conflicts.
    *
    * For example:
    * {{{
    * scala> groupPairs(Seq(("a", 1), ("b", 2), ("a", 3)))
    * res0: Map[String,Int] = Map(b -> 2, a -> 4)
    * }}}
    * */
  def groupPairs[A, B: Monoid](pairs: Seq[(A, B)]): Map[A, B] =
    pairs.toList.foldMap { case (a, b) => Map(a -> b) }

  // Same as above but with triples
  def groupTriples[A, B, C: Monoid](trips: Seq[(A, B, C)]): Map[A, Map[B, C]] =
    trips.toList.foldMap { case (a, b, c) => Map(a -> Map(b -> c)) }
}
