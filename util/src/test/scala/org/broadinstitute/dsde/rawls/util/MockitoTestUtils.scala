package org.broadinstitute.dsde.rawls.util

import org.mockito.ArgumentCaptor
import org.scalatest.mock.MockitoSugar

import scala.reflect.{ClassTag, classTag}

/**
  * Created by rtitle on 7/14/17.
  */
trait MockitoTestUtils extends MockitoSugar {

  // Scala sugar for Mockitor ArgumentCaptor
  def captor[T: ClassTag]: ArgumentCaptor[T] =
    ArgumentCaptor.forClass(classTag[T].runtimeClass).asInstanceOf[ArgumentCaptor[T]]

}
