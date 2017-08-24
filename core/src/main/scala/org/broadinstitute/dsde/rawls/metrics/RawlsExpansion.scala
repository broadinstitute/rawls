package org.broadinstitute.dsde.rawls.metrics

import org.broadinstitute.dsde.rawls.metrics.Expansion.UriExpansion
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, WorkspaceName}
import shapeless._
import spray.http.Uri
import spray.http.Uri.Path._
import spray.routing.PathMatcher.Matched
import spray.routing.PathMatcher

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
    * @param pathMatcher a PathMatcher which matches 1 or more String elements. If the PathMatcher matches,
    *                    any matched Strings will be redacted from the Uri. Otherwise, the Uri is unchanged.
    * @return Uri Expansion instance
    */
  def redactedUriExpansion[L <: HList](pathMatcher: PathMatcher[L]): Expansion[Uri] = {

    new Expansion[Uri] {
      override def makeName(uri: Uri): String = {
        // Apply the provided PathMatcher. This will give us a HList of extractions, if it matches the Path.
        val updatedPath = pathMatcher.apply(uri.path) match {
          case Matched(_, extractions) =>
            // Fold through the URI path elements, building up a new Path that has each extraction replaced
            // with the string "redacted".
            // Note the extractions are returned in the same order as they occur in the path, so we only need
            // to traverse the path once.
            uri.path.foldLeft[(Uri.Path, HList)]((Uri.Path.Empty, extractions)) { case ((resultPath, remainingExtractions), currentSegment) =>
              remainingExtractions match {
                case (h: String) :: tail if h == currentSegment =>
                  (resultPath / "redacted", tail)
                case _  =>
                  (resultPath / currentSegment, remainingExtractions)
              }
            }._1
          case _ =>
            // For non-matches, return the path unchanged.
            uri.path
        }
        // Finally invoke UriExpansion with our updated path
        UriExpansion.makeName(uri.copy(path = updatedPath))
      }
    }
  }

  /**
    * Adds foldLeft to spray's Uri.Path which operates on its segments.
    * Path is a recursive data structure (like List), but there are no map, fold, etc operations defined on it.
    */
  private implicit class PathOps(path: Uri.Path) {
    @tailrec
    final def foldLeft[B](z: B)(op: (B, String) => B): B = {
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
}
