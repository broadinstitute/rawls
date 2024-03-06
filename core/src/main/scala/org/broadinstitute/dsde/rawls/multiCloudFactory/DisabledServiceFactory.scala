package org.broadinstitute.dsde.rawls.multiCloudFactory

import scala.reflect.{ClassTag, classTag}

object DisabledServiceFactory {
  def newDisabledService[T : ClassTag]: T = {
    java.lang.reflect.Proxy.newProxyInstance(
      classTag[T].runtimeClass.getClassLoader,
      Array(classTag[T].runtimeClass),
      (_, method, _) => throw new UnsupportedOperationException(s"${method.toString} is not supported in Azure control plane.")
    ).asInstanceOf[T]
  }
}
