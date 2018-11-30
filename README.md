
# service.tools

A Clojure library designed to map functions over kafka topics.

## Usage

The `kafka-adapter` function is the main function, it takes one function and an options map. It returns a channel, any activity (writing or closing) on it stops the adapter.

The adapter dispatches to workers, manages dynamic rebalancing.

The following options are available:

 * `:config` is a kafka config map (strings to strings) shared by consumer and producer.
 * `:consumer-config` and `:producer-config` are kafka config maps specific to consumers and producers.
 * `:input-topics` a collection of topic names to consume.
 * `:topic-aliases` a map from aliases to topic names; this is used to route "messages" emitted by `f` to actual topics. There are two reserved aliases: `:out` and `:state`. Both are mandatory for stateful mode.
 * `:edn`, `:edn-in` and `:edn-out` all default to true and control if the adpater should automatically read and print to edn.
 * `:traces` a channel to which maps of the form `{:from deps :to [topic partition offset]}` are sent.
 * `:deps-fn` a function from an outgoing value to a collection of dependencies ids.

Messages are read from the topics listed under `:input-topics`. 
When a message arrives, its value (parsed as edn by default) is passed to `f` which returns a collection of pairs `[alias v]` where alias is a topic alias used to lookup the actual topic in the map specified under `:topic-aliases`. This functionality can be used for routing as messages can be dispatched to serveral outputs.

### Tracing and dependencies

When the `:traces` option is set to a channel, this channel receives values like `{:from deps :to [topic partition offset]}` where deps is a collection of source messages ids (for Kafka messages they are identified by a triple `[topic partition offset]`).

If `:traces` is set but not `:deps-fn` then the incoming message is considered to be the only dependency of the outgoing message.

## License

Copyright © 2016 HCA

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
