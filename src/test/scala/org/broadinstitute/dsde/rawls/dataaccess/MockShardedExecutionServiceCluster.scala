package org.broadinstitute.dsde.rawls.dataaccess

/**
  * Created by davidan on 6/16/16.
  */
class MockShardedExecutionServiceCluster(members: Map[Int, ExecutionServiceDAO])
  extends ShardedHttpExecutionServiceCluster(members: Map[Int, ExecutionServiceDAO]) {

  def defaultInstance = members.values.head

}
