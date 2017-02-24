package uk.co.appministry.akka.zk

import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.{WatchedEvent, Watcher}

/**
  * A rich wrapper for the [[org.apache.zookeeper.WatchedEvent]]
  * @param underlaying original event
  */
case class WatchedEventMeta(val underlaying: WatchedEvent) {

  val dataChangeTriggeringEvents = List(
    EventType.NodeDataChanged,
    EventType.NodeDeleted,
    EventType.NodeCreated )

  val childChangeTriggeringEvents = List(
    EventType.NodeChildrenChanged,
    EventType.NodeCreated,
    EventType.NodeDeleted )

  lazy val stateChanged = Option(underlaying.getPath) == None
  lazy val znodeChanged = Option(underlaying.getPath) != None
  lazy val dataChanged = dataChangeTriggeringEvents.contains(underlaying.getType)
  lazy val childrenChanged = childChangeTriggeringEvents.contains(underlaying.getType)
}

/**
  * ZooKeeper watcher mixin.
  *
  * Contains the logic for handling ZooKeeper [[org.apache.zookeeper.WatchedEvent]]s.
  */
trait ZkClientWatcher extends Watcher { this: ZkClientActor =>

  private var currentState = KeeperState.Disconnected

  /**
    * Process an incoming [[org.apache.zookeeper.WatchedEvent]].
    * @param event event to process
    */
  override def process(event: WatchedEvent): Unit = {

    val meta = WatchedEventMeta(event)

    if (currentState != event.getState && event.getState == KeeperState.SyncConnected) {
      self ! ZkInternalProtocol.ZkConnectionSuccessful()
    }

    currentState = event.getState

    if (meta.stateChanged) {
      self ! ZkInternalProtocol.ZkProcessStateChange(meta)
    }

    if (meta.dataChanged) {
      self ! ZkInternalProtocol.ZkProcessDataChange(meta)
    }

    if (meta.childrenChanged) {
      self ! ZkInternalProtocol.ZkProcessChildChange(meta)
    }
  }

}
