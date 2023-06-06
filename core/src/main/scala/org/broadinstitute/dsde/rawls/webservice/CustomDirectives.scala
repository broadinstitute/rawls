package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives.{extractRequest, mapResponseHeaders}

trait CustomDirectives {
  def addLocationHeader(path: String): Directive0 = extractRequest.flatMap { request =>
    mapResponseHeaders(headers => headers :+ Location(request.uri.copy(path = Uri.Path(path))))
  }
}

object CustomDirectives extends CustomDirectives
