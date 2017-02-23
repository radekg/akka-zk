package uk.co.appministry.akka.zk

import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.{WatchedEvent, Watcher}

case class EventMetadata(val event: WatchedEvent) {

  val dataChangeTriggeringEvents = List(
    EventType.NodeDataChanged,
    EventType.NodeDeleted,
    EventType.NodeCreated )

  val childChangeTriggeringEvents = List(
    EventType.NodeChildrenChanged,
    EventType.NodeCreated,
    EventType.NodeDeleted )

  lazy val stateChanged = Option(event.getPath) == None
  lazy val znodeChanged = Option(event.getPath) != None
  lazy val dataChanged = dataChangeTriggeringEvents.contains(event.getType)
  lazy val childrenChanged = childChangeTriggeringEvents.contains(event.getType)
}

/**
  * ZooKeeper watcher mixin.
  *
  * Contains the logic for handling ZooKeeper [[org.apache.zookeeper.WatchedEvent]]s.
  */
trait ZkClientWatcher extends Watcher { this: ZkClientActor =>

  private var currentState = KeeperState.Disconnected

  override def process(event: WatchedEvent): Unit = {

    val meta = EventMetadata(event)

    if (currentState != event.getState && event.getState == KeeperState.SyncConnected) {
      self ! ZkInternalProtocol.ZkConnectionSuccessful()
    }

    currentState = event.getState

    if (meta.stateChanged) {
      self ! ZkInternalProtocol.ZkProcessStateChange(event)
    }

    if (meta.dataChanged) {
      self ! ZkInternalProtocol.ZkProcessDataChange(event)
    }

    if (meta.childrenChanged) {
      self ! ZkInternalProtocol.ZkProcessChildChange(event)
    }
  }

}
