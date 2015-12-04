package org.broadinstitute.dsde.rawls.webservice

import akka.actor.Status.Failure
import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithStatusCode, RawlsException}
import org.broadinstitute.dsde.rawls.model.ErrorReport
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.webservice.PerRequest._
import spray.http.StatusCodes._
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}
import spray.httpx.marshalling.{BasicToResponseMarshallers, ToResponseMarshaller}
import spray.json.DefaultJsonProtocol
import spray.routing.RequestContext
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import spray.http._
import scala.language.postfixOps


case class RawlsMessage(message: String)

object RawlsMessageJsonSupport extends DefaultJsonProtocol with BasicToResponseMarshallers {

  implicit val RawlsMessageFormat = jsonFormat1(RawlsMessage)

}

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
  import spray.json.DefaultJsonProtocol._
  import spray.httpx.SprayJsonSupport._
  import RawlsMessageJsonSupport._

  def r: RequestContext
  def target: ActorRef
  def message: AnyRef
  def timeout: Duration

  setReceiveTimeout(timeout)
  target ! message

  def receive = {
    case RequestComplete_(response, marshaller: ToResponseMarshaller[Any]) => complete(response)(marshaller)
    case RequestCompleteWithHeaders_(response, headers, marshaller: ToResponseMarshaller[Any]) => complete(response, headers:_*)(marshaller)
    case RequestCompleteWithLocation_(response, location, marshaller: ToResponseMarshaller[Any]) => complete(response, HttpHeaders.Location(r.request.uri.copy(path = Uri.Path(location))))(marshaller)
    case ReceiveTimeout => complete(GatewayTimeout)
    case Failure(t) =>
      // failed Futures will end up in this case
      handleException(t)
      stop(self)
    case x =>
      system.log.error("Unsupported response message sent to PreRequest actor: " + Option(x).getOrElse("null").toString)
      complete(InternalServerError)
  }

  /**
   * Complete the request sending the given response and status code
   * @param response to send to the caller
   * @param marshaller to use for marshalling the response
   * @tparam T the type of the response
   * @return
   */
  private def complete[T](response: T, headers: HttpHeader*)(implicit marshaller: ToResponseMarshaller[T]): Unit = {
    //if the body of the response is a string, we need to wrap it in a RawlsMessage that can be marshaled to and from json
    response match {
      case (statusCode: StatusCode, message: String) =>
        val newResponse = (statusCode, RawlsMessage(message))
        //we need to explicitly set the implicit marshaller here, otherwise it uses the implicit marshaller above
        r.withHttpResponseHeadersMapped(h => h ++ headers).complete(newResponse)(RawlsMessageJsonSupport.fromStatusCodeAndT(s => s, RawlsMessageFormat))
        stop(self)
      case errorReport: ErrorReport =>
        val newResponse = (errorReport.statusCode.getOrElse(StatusCodes.InternalServerError), errorReport)
        r.withHttpResponseHeadersMapped(h => h ++ headers).complete(newResponse)(RawlsMessageJsonSupport.fromStatusCodeAndT(s => s, ErrorReportFormat))
      case _ =>
        r.withHttpResponseHeadersMapped(h => h ++ headers).complete(response)
        stop(self)
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        handleException(e)
        Stop
      }
    }

  def handleException(e: Throwable): Unit = {
    system.log.error(e, "error processing request: " + r.request.uri)
    import spray.httpx.SprayJsonSupport._
    val code = e match {
      case e: RawlsExceptionWithStatusCode => e.getCode
      case _ => InternalServerError
    }
    r.complete(code, ErrorReport(e,code))
  }
}

object PerRequest {
  sealed trait PerRequestMessage
  /**
   * Report complete, follows same pattern as spray.routing.RequestContext.complete; examples of how to call
   * that method should apply here too. E.g. even though this method has only one parameter, it can be called
   * with 2 where the first is a StatusCode: RequestComplete(StatusCode.Created, response)
   */
  case class RequestComplete[T](response: T)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /**
   * Report complete with response headers. To response with a special status code the first parameter can be a
   * tuple where the first element is StatusCode: RequestCompleteWithHeaders((StatusCode.Created, results), header).
   * Note that this is here so that RequestComplete above can behave like spray.routing.RequestContext.complete.
   */
  case class RequestCompleteWithHeaders[T](response: T, headers: HttpHeader*)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /**
   * Report complete with a path to use as a Location header
   */
  case class RequestCompleteWithLocation[T](response: T, location: String)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /** allows for pattern matching with extraction of marshaller */
  private object RequestComplete_ {
    def unapply[T](requestComplete: RequestComplete[T]) = Some((requestComplete.response, requestComplete.marshaller))
  }

  /** allows for pattern matching with extraction of marshaller */
  private object RequestCompleteWithHeaders_ {
    def unapply[T](requestComplete: RequestCompleteWithHeaders[T]) = Some((requestComplete.response, requestComplete.headers, requestComplete.marshaller))
  }

  /** allows for pattern matching with extraction of marshaller */
  private object RequestCompleteWithLocation_ {
    def unapply[T](requestComplete: RequestCompleteWithLocation[T]) = Some((requestComplete.response, requestComplete.location, requestComplete.marshaller))
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

  def perRequest(r: RequestContext, props: Props, message: AnyRef, timeout: Duration = 3 minutes) =
    actorRefFactory.actorOf(Props(new WithProps(r, props, message, timeout)))
}

