package org.broadinstitute.dsde.rawls.serviceFactory

import com.typesafe.scalalogging.LazyLogging

import java.lang.reflect.{Method, Proxy}
import scala.reflect.{classTag, ClassTag}

object DisabledServiceFactory extends LazyLogging {

  /**
   * Create a new instance of a service that throws UnsupportedOperationException for all methods.
   * Implemented using a dynamic proxy.
   * @tparam T the type of the service, must be a trait
   * @return a new instance of the service that throws UnsupportedOperationException for all methods
   */
  def newDisabledService[T: ClassTag]: T =
    Proxy
      .newProxyInstance(
        classTag[T].runtimeClass.getClassLoader,
        Array(classTag[T].runtimeClass),
        (_, method, _) =>
          throw new UnsupportedOperationException(s"${method.toString} is not supported in Azure control plane.")
      )
      .asInstanceOf[T]

  /**
   * Create a new instance of a service that throws UnsupportedOperationException for all non-Unit methods.
   * For Unit methods, an error is not explicitly thrown but instead logged and does not block dependent processes.
   * Implemented using a dynamic proxy.
   * @tparam T the type of the service, must be a trait
   * @return a new instance of the service that throws UnsupportedOperationException for all non-Unit methods
   */
  def newSilentDisabledService[T: ClassTag]: T =
    Proxy
      .newProxyInstance(
        classTag[T].runtimeClass.getClassLoader,
        Array(classTag[T].runtimeClass),
        (_, method, _) =>
          if (method.getReturnType().equals(().getClass())) {
            logger.error(
              s"${method.toString} is not supported in Azure control plane. Service has been identified to be non-blocking while inoperable and will silently fail."
            )
            None
          } else {
            throw new UnsupportedOperationException(s"${method.toString} is not supported in Azure control plane.")
          }
      )
      .asInstanceOf[T]
}
