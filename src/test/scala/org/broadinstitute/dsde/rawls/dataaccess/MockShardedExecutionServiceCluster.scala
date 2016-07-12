package org.broadinstitute.dsde.rawls.dataaccess

/**
  * Created by davidan on 6/16/16.
  */
class MockShardedExecutionServiceCluster(members: Map[ExecutionServiceId, ExecutionServiceDAO], default: ExecutionServiceId, dataSource: SlickDataSource)
  extends ShardedHttpExecutionServiceCluster(members: Map[ExecutionServiceId, ExecutionServiceDAO], default: ExecutionServiceId, dataSource: SlickDataSource) {

}

object MockShardedExecutionServiceCluster {
  def fromDAO(dao: ExecutionServiceDAO, dataSource: SlickDataSource) = new MockShardedExecutionServiceCluster( Map(ExecutionServiceId("default")->dao), ExecutionServiceId("default"), dataSource)


}
