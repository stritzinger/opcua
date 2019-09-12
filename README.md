# Huh, what is this?

This branch contains the WIP on OPC UA PubSub. The related files all have the
*_pubsub_* substring in the name.

See [confluence](https://stritzinger.atlassian.net/wiki/spaces/DEV/pages/287244289/OPC+UA#PubSub)
for the research state on PubSub. For the official specs see part 14.

## Initial Targets and current State

Implementing PubSub is quite a large endeavour and so as a start we want to get
something suitable for a demo: just send some little data to a subscriber. This
is now _working_ in the following sense:

* Via `opcua_pubsub:start_link()` a publisher is started which sends UADP 
packets to a UDP multicast socket. The packets only contain a system
timestamp.

* The publisher also 'subscribes' to the multicast socket and will receive its
own data to print it out (you will see something in the erlang shell)

* Using the [open62541 subscriber](https://github.com/open62541/open62541/blob/master/examples/pubsub/tutorial_pubsub_subscribe.c)
you can connect from another host (in the same subnet) to the multicast socket
and receive the packets. The subscriber doesn't print out the content, it will
only print out that a message was received.

Code quality is at the moment neglected, it is not PR ready and certainly a
big chunk of the code will be thrown away for the real implementation.

## Working with the open62541 example subscriber

First: use Linux for that, don't even attempt MacOS. It might work, but rather
not :).

Checkout the [docu](https://open62541.org/doc/current/building.html) to build
the open62541 together with the examples. Use `ccmake` to enable building the
examples and also pubsub support, otherwise the subscriber won't be built.

There is no further configuration necessary, publisher and subscriber have
hardcoded configs being ready to go.
