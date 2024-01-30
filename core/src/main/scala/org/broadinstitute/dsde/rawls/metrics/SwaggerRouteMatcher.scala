package org.broadinstitute.dsde.rawls.metrics

import io.swagger.v3.parser.OpenAPIV3Parser

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

case class RouteInfo(path: String, regex: Regex, parameterNames: List[String])
case class MatchedRoute(path: String, parameters: Map[String, String])

object SwaggerRouteMatcher {

  private def loadSwaggerPaths(): List[String] = {
    val swaggerResource = scala.io.Source.fromResource("swagger/api-docs.yaml")
    val parseResults = new OpenAPIV3Parser().readContents(swaggerResource.mkString)
    parseResults.getOpenAPI.getPaths.keySet().asScala.toList
  }

  private val parseRoutes: List[RouteInfo] = {
    loadSwaggerPaths().map { path =>
      val parameterNames = "\\{([^}]+)}".r.findAllMatchIn(path).map(_.group(1)).toList
      // create a regex that matches the path, replacing the parameter names with a named regex that matches anything
      val regex = parameterNames.foldLeft(path) { (acc, param) =>
        acc.replace(s"{$param}", s"(?<$param>[^/]+)")
      }.r
      RouteInfo(path, regex, parameterNames)
    // sort the routes by the number of parameters, so that the most specific routes are first
    }.sortBy(_.parameterNames.length)
  }

  def matchRoute(path: String): Option[MatchedRoute] = {
    // find the first route that matches the path
    parseRoutes.to(LazyList).map(route => (route, route.regex.pattern.matcher(path))).collectFirst {
      case (route, matcher) if matcher.matches() =>
        val parameters = route.parameterNames.map { name =>
          name -> matcher.group(name)
        }.toMap
        MatchedRoute(route.path, parameters)
    }
  }
}
