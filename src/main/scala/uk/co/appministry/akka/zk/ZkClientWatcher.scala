package uk.co.appministry.akka.zk

import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.{WatchedEvent, Watcher}

/**
  * A rich wrapper for the [[org.apache.zookeeper.WatchedEvent]]
  * @param underlying original event
  */
case class WatchedEventMeta(val underlying: WatchedEvent) {

  /**
    * List of data change event types.
    */
  val dataChangeTriggeringEvents = List(
    EventType.NodeDataChanged,
    EventType.NodeDeleted,
    EventType.NodeCreated )

  /**
    * List of child change event types.
    */
  val childChangeTriggeringEvents = List(
    EventType.NodeChildrenChanged,
    EventType.NodeCreated,
    EventType.NodeDeleted )

  /**
    * State changed?.
    */
  lazy val stateChanged = Option(underlying.getPath) == None

  /**
    * znode changed?.
    */
  lazy val znodeChanged = Option(underlying.getPath) != None

  /**
    * Data changed?.
    */
  lazy val dataChanged = dataChangeTriggeringEvents.contains(underlying.getType)

  /**
    * Children changed?.
    */
  lazy val childrenChanged = childChangeTriggeringEvents.contains(underlying.getType)
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

    if (currentState != event.getState && event.getState == KeeperState.Disconnected) {
      self ! ZkInternalProtocol.ZkConnectionLost()
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
