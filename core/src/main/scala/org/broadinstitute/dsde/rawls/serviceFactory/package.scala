package org.broadinstitute.dsde.rawls

/**
 * This package contains service factories that encapsulate config lookups to instantiate. When config is not available,
 * the service factory will return a disabled service. This happens when Rawls is running in a non-GCP environment.
 */
package object serviceFactory {}
