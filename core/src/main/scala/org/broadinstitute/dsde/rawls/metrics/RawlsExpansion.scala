package org.broadinstitute.dsde.rawls.metrics

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path.{Empty, Segment, Slash}
import akka.http.scaladsl.server.PathMatcher
import akka.http.scaladsl.server.PathMatcher.Matched
import org.broadinstitute.dsde.rawls.metrics.Expansion.UriExpansion
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, WorkspaceName}

import scala.annotation.tailrec

/**
  * Created by rtitle on 7/25/17.
  */
object RawlsExpansion {

  // Typeclass instances:

  /**
    * Implicit expansion for WorkspaceName.
    * Statsd doesn't allow slashes in metric names, so we override makeName to override
    * the default toString based implementation.
    */
  implicit object WorkspaceNameExpansion extends Expansion[WorkspaceName] {
    override def makeName(n: WorkspaceName): String = n.toString.replace('/', '.')
  }

  /**
    * Implicit expansion for RawlsEnumeration using the default makeName.
    * This takes an upper type bound {{{A <: RawlsEnumeration}}} so it can work with any
    * subtype of RawlsEnumeration.
    */
  implicit def RawlsEnumerationExpansion[A <: RawlsEnumeration[_]] = new Expansion[A] {}

  /**
    * Creates an expansion for Uri which redacts pieces of the Uri matched by the provided PathMatcher.
    * @param pathMatchers seq of PathMatcher which matches 1 or more String elements. If the PathMatcher matches,
    *                    any matched Strings will be redacted from the Uri. Otherwise, the Uri is unchanged.
    * @return Uri Expansion instance
    */
  def redactedUriExpansion[L <: Product](pathMatchers: Seq[PathMatcher[L]]): Expansion[Uri] =
    new Expansion[Uri] {
      override def makeName(uri: Uri): String = {
        val maybePathMatcher = pathMatchers.find { m =>
          m.apply(uri.path) match {
            case Matched(_, _) => true
            case _             => false
          }
        }

        val updatedPath = maybePathMatcher match {
          case Some(pathMatcher) =>
            pathMatcher.apply(uri.path) match {
              case Matched(_, extractions) =>
                // Fold through the URI path elements, building up a new Path that has each extraction replaced
                // with the string "redacted".
                // Note the extractions are returned in the same order as they occur in the path, so we only need
                // to traverse the path once.
                // Also note extractions that match a bunch of parts of the path at once are a list so we need to flatten it out
                val flatExtractions = extractions.productIterator.toList.flatMap {
                  case t: TraversableOnce[_] => t
                  case other                 => List(other)
                }
                uri.path
                  .foldLeft[(Uri.Path, List[Any])]((Uri.Path.Empty, flatExtractions)) {
                    case ((resultPath, remainingExtractions), currentSegment) =>
                      remainingExtractions match {
                        case (h: String) :: tail if h == currentSegment =>
                          (resultPath / "redacted", tail)
                        case _ =>
                          (resultPath / currentSegment, remainingExtractions)
                      }
                  }
                  ._1

              case _ =>
                // shouldn't hit this case since we already know it is a match but it makes the compiler happy
                uri.path
            }

          case None =>
            // For non-matches, return the path unchanged.
            uri.path
        }

        // Finally invoke UriExpansion with our updated path
        UriExpansion.makeName(uri.copy(path = updatedPath))
      }
    }

  /**
    * Adds foldLeft to spray's Uri.Path which operates on its segments.
    * Path is a recursive data structure (like List), but there are no map, fold, etc operations defined on it.
    */
  implicit private class PathOps(path: Uri.Path) {
    @tailrec
    final def foldLeft[B](z: B)(op: (B, String) => B): B =
      path match {
        case Segment(head, tail) =>
          val current = op(z, head)
          tail.foldLeft(current)(op)
        case Slash(tail) =>
          // Ignore slashes; the fold only operates on segments
          tail.foldLeft(z)(op)
        case Empty =>
          z
      }
  }
}
