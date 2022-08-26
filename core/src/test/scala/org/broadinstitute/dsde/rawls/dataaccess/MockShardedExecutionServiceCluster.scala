package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem

/**
  * Created by davidan on 6/16/16.
  */
class MockShardedExecutionServiceCluster(val readMembers: Set[ClusterMember],
                                         submitMembers: Set[ClusterMember],
                                         dataSource: SlickDataSource
)(implicit system: ActorSystem)
    extends ShardedHttpExecutionServiceCluster(readMembers, submitMembers, dataSource) {

  // for unit tests
  def getDefaultSubmitMember: ExecutionServiceDAO = submitMembers.head.dao

}

object MockShardedExecutionServiceCluster {
  def fromDAO(dao: ExecutionServiceDAO, dataSource: SlickDataSource)(implicit system: ActorSystem) =
    new MockShardedExecutionServiceCluster(Set(ClusterMember(ExecutionServiceId("unittestdefault"), dao)),
                                           Set(ClusterMember(ExecutionServiceId("unittestdefault"), dao)),
                                           dataSource
    )
}
