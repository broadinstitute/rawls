package org.broadinstitute.dsde.rawls.metrics

/**
  * An instrumented Google service.
  */
object GoogleInstrumentedService extends Enumeration {
  type GoogleInstrumentedService = Value
  val Billing, Buckets, Genomics, Groups, PubSub = Value

  /**
    * Expansion for GoogleInstrumentedService which uses the default toString implementation.
    */
  implicit object GoogleInstrumentedServiceExpansion extends Expansion[GoogleInstrumentedService]
}
