package org.broadinstitute.dsde.rawls.ws

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import org.broadinstitute.dsde.rawls.ws.PerRequest._
import spray.http.StatusCodes._
import spray.httpx.marshalling.ToResponseMarshaller
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}
import spray.routing.RequestContext
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import spray.http.StatusCode

/**
 * This actor controls the lifecycle of a request. It is responsible for forwarding the initial message
 * to a target handling actor. This actor waits for the target actor to signal completion (via a message),
 * timeout, or handle an exception. It is this actors responsibility to respond to the request and
 * shutdown itself and child actors.
 *
 * Request completion can be signaled in 2 ways:
 * 1) with just a response object
 * 2) with a RequestComplete message which can specify http status code as well as the response
 */
trait PerRequest extends Actor {
  import context._

  def r: RequestContext
  def target: ActorRef
  def message: AnyRef
  def timeout: Duration

  setReceiveTimeout(timeout)
  target ! message

  def receive = {
    case RequestCompleteOK_(response, marshaller) => complete(OK, response)(marshaller)
    case RequestComplete_(response, statusCode, marshaller) => complete(statusCode, response)(marshaller)
    case RequestCompleteNoContent(statusCode) => complete(statusCode)
    case ReceiveTimeout => complete(GatewayTimeout)
    case x =>
      system.log.error("Unsupported response message sent to PreRequest actor: " + Option(x).getOrElse("null").toString)
      complete(InternalServerError)
  }

  /**
   * Complete the request sending the given response and status code
   * @param status http status code
   * @param response to send to the caller
   * @param marshaller to use for marshalling the response
   * @tparam T the type of the response
   * @return
   */
  private def complete[T](status: StatusCode, response: T)(implicit marshaller: ToResponseMarshaller[T]) = {
    r.complete(response)(marshaller)
    stop(self)
  }

  /**
   * Complete the request with no content, just a status code
   * @param status http status code
   */
  private def complete(status: StatusCode) = {
    r.complete(status)
    stop(self)
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        system.log.error(e, "error processing request: " + r.request.uri)
        r.complete(InternalServerError, e.getMessage)
        Stop
      }
    }
}


object PerRequest {
  sealed trait PerRequestMessage
  /**
   * Report complete, specifying a specific status code
   */
  case class RequestComplete[T](statusCode: StatusCode, response: T)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /**
   * Report complete, use the OK status code
   */
  case class RequestCompleteOK[T](response: T)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /**
   * Report complete with no content, specifying a specific status code
   */
  case class RequestCompleteNoContent(statusCode: StatusCode) extends PerRequestMessage

  // the RequestCompleteOK_ and RequestComplete_ objects allow us to pattern match on the associated case classes
  // and pull out the marshaller as well. But we can't name them the same, thus the trailing _
  private object RequestCompleteOK_ {
    def unapply[T](requestCompleteOK: RequestCompleteOK[T]) = Some((requestCompleteOK.response, requestCompleteOK.marshaller))
  }

  private object RequestComplete_ {
    def unapply[T](requestComplete: RequestComplete[T]) = Some((requestComplete.response, requestComplete.statusCode, requestComplete.marshaller))
  }

  case class WithProps(r: RequestContext, props: Props, message: AnyRef, timeout: Duration) extends PerRequest {
    lazy val target = context.actorOf(props)
  }
}

/**
 * Provides factory methods for creating per request actors
 */
trait PerRequestCreator {
  implicit def actorRefFactory: ActorRefFactory

  def perRequest(r: RequestContext, props: Props, message: AnyRef, timeout: Duration = 1 minutes) =
    actorRefFactory.actorOf(Props(new WithProps(r, props, message, timeout)))
}