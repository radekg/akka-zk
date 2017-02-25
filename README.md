# akka ZK

Reactive akka ZooKeeper client.

## Build status

[![Build Status](https://travis-ci.org/AppMinistry/akka-zk.svg?branch=master)](https://travis-ci.org/AppMinistry/akka-zk)

## Usage

### Dependencies

    libraryDependencies ++= Seq(
      "uk.co.appministry" %% "akka-zk" % "0.1.0-snapshot"
    )

## Examples

### Creating the client

The `akka-zk` ZooKeeper client uses `akka streams` for delivering watch notifications. It is possible to create the client as a regular actor:

```scala
val actorSystem = ActorSystem("examples")
val zkClient = system.actorOf(Props(new ZkClientActor))
```
    
### Client configuration

The client does not have any special configuration needs. All configuration is passed with `ZkRequestProtocol.Connect` message.

### Connecting

```scala
val system = ActorSystem("examples")
 
val runner = system.actorOf(Props(new Actor {
  
  val zkClient = context.system.actorOf(Props(new ZkClientActor))
  context.watch(zkClient)
  
  override def supervisorStrategy = OneForOneStrategy() {
    case _: ZkClientConnectionFailedException =>
      // ZooKeeper client was unable to connect to the server for `connectionAttempts` times.
      // The client is stopped and a new actor has to be created.
      Escalate
  }
  
  def receive = {
    case "connect" =>
      zkClient ! ZkRequestProtocol.Connect(connectionString = "10.100.0.21:2181",
                                           connectionAttempts = 5,
                                           sessionTimeout = 30 seconds)
    case ZkResponseProtocol.Connected(request, reactiveStreamsPublisher) =>
      // zkClient is now ready for work
  }
  
}))
runner ! "connect"
```

### Subscribing to and unsubscribing from data / children changes

ZooKeeper client emits three types of events related to ZooKeeper state changes:

- `ZkClientStreamingResponse.StateChange(event: WatchedEventMeta)`: this is a client connection state change event
- `ZkClientStreamingResponse.ChildChange(event: WatchedEventMeta)`: this is a znode children change event
- `ZkClientStreamingResponse.DataChange(event: WatchedEventMeta)`: this is a znode data change event

The `StateChange` events are automatically delivered, there is no subscription required. However, the `ChildChange` and `DataChange` events 
are per `path` thus requiring an explicit subscription. To initialize a subscription:

```scala
val system = ActorSystem("examples")
 
val runner = system.actorOf(Props(new Actor {
  
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(
      initialSize = 10,
      maxSize = 64))
  
  val zkClient = context.system.actorOf(Props(new ZkClientActor))
  
  def receive = {
    case "connect" =>
      zkClient ! ZkRequestProtocol.Connect(connectionString = "10.100.0.21:2181",
                                           connectionAttempts = 5,
                                           sessionTimeout = 30 seconds)
    case ZkResponseProtocol.Connected(request, publisher) =>
      
      // a very simple example:
      Source.fromPublisher[ZkClientStreamProtocol.StreamResponse](publisher).map { message =>
        message match {
          case m: ZkClientStreamProtocol.ChildChange => // children change event
          case m: ZkClientStreamProtocol.DataChange  => // data change event
          case m: ZkClientStreamProtocol.StateChange => // state change event
        }
      }.runWith(Sink.ignore)
      
      self ! "subscribe"
      
    case "subscribe" =>
      zkClient ! ZkRequestProtocol.SubscribeChildChanges("/some/zookeeper/path")
      zkClient ! ZkRequestProtocol.SubscribeDataChanges("/some/other/zookeeper/path")
    case ZkResponseProtocol.SubscriptionSuccess(request) =>
      request match {
        case _: ZkRequestProtocol.SubscribeChildChanges =>
          // from now on, the child changes for the requested path will be streaming via the Source
        case _: ZkRequestProtocol.SubscribeDataChanges =>
          // from now on, the data changes for the requested path will be streaming via the Source
      }
    case "unsubscribe" =>
      zkClient ! ZkRequestProtocol.UnsubscribeChildChanges("/some/zookeeper/path")
      zkClient ! ZkRequestProtocol.UnsubscribeDataChanges("/some/other/zookeeper/path")
    case ZkResponseProtocol.UnsubscriptionSuccess(request) =>
      request match {
        case _: ZkRequestProtocol.UnsubscribeChildChanges =>
          // child change for the requested path will stop streaming via the Flow
        case _: ZkRequestProtocol.UnsubscribeDataChanges =>
          // data change for the requested path will stop streaming via the Flow
      }
  }
}))

runner ! "connect"
```

### Handling underlying ZooKeeper errors

Any operation that failed will be wrapped in and returned as `ZkResponseProtocol.OperationError(originalRequest, cause)`. An example:

```scala
  def receive = {
    case "create-node" =>
      zkClient ! ZkRequestProtocol.CreatePersistent("/some/zookeeper/path/for/which/the/parent/does/not/exist")
    case ZkResponseProtocol.OperationError(request, cause) =>
      request match {
        case r: ZkRequestProtocol.CreatePersistent =>
          log.error(s"Failed to create znode: ${r.path}. Reason: $cause.")
        case _ =>
      }
  }
```

## License

Author: Rad Gruchalski (radek@gruchalski.com)

This work will be available under Apache License, Version 2.0.

Copyright 2017 Rad Gruchalski (radek@gruchalski.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.