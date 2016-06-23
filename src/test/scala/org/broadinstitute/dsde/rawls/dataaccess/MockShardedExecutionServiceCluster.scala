package org.broadinstitute.dsde.rawls.dataaccess

/**
  * Created by davidan on 6/16/16.
  */
class MockShardedExecutionServiceCluster(members: Map[ExecutionServiceId, ExecutionServiceDAO], dataSource: SlickDataSource)
  extends ShardedHttpExecutionServiceCluster(members: Map[ExecutionServiceId, ExecutionServiceDAO], dataSource: SlickDataSource) {

  def defaultInstance = members.values.head

}

object MockShardedExecutionServiceCluster {
  def fromDAO(dao: ExecutionServiceDAO, dataSource: SlickDataSource) = new MockShardedExecutionServiceCluster( Map(ExecutionServiceId("0")->dao), dataSource)
}
