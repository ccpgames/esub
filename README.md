# esub

esub is a simple short lived subscription/reply microservice over http.

esub does not care about the content it passes, it sees everything as bytes. Do not rely on the content-encoding received from esub.

You can attempt to secure your sub by passing a `token` qsparam. The subsequent rep call must match the token (the default of empty string also requires a match. For example, if no auth is requested but some is passed, the call will fail).

A sub can be taken over by another caller. If this happens the original sub is disconnected and the new sub takes over. You should use UUIDs for your sub keys to avoid this situation.

Rep calls do not wait. They either succeed and deliver their payload to the waiting sub, or they fail with a 4xx level error.

Sub calls never time out. If you want, you can set up a proxy in front of esub to enforce timeouts. Generally speaking though, don't do this. The clients should be using timeouts and disconnecting when they no longer care. As soon as a sub disconnects rep calls to that key will 404 (after phase2 if configured those 404 payloads will be stored for later replay capabilities).


# use case

Here's the scenario:

You have a codebase, one part of that would like work done by something in a different proc, container, datacentre, whatever. You use a persistent queue to send work out to a batch of workers, one of whom picks up the task and performs work. You'd like a response from that worker. Enter esub.

The general esub flow is:

  - contact esub, find an available node address
  - create a reply UUID, add that to your work message payload as an esub URI (with the node address as well)
  - send the message to your queue system
  - make an HTTP GET call to the esub node's `/sub/:sub` endpoint with your reply id and optional token (main thread blocked)
  - worker thread picks up message and performs work, sends response to dictated node and sub key (reply UUID)
  - main thread receives response, continues on...


# esub phases

Currently esub has implemented phase 1 of 4.


## esub phase 1

The only supported HTTP method is `GET`, except for `/rep/:rep` which is `POST`.

route     | description
----------|----------------
/info     | static server info JSON
/keys     | list all known local sub keys
/ping     | health check endpoint
/sub/:sub | start a new sub
/rep/:rep | reply to a sub


## esub phase 2

Extends phase 1 by adding a DB to dump `/rep/:rep` calls where the sub is not known/connected, or all messages (configurable).

route      | description
-----------|---------------------
/rsub/:sub | replay a sub from db


## esub phase 3

Phase 3 starts to bring cluster operations to esub (again configurable). These routes will require a shared cluster token.

route     | description
----------|----------------
/join     | add another esub server to our memberlist
/members  | memberlist JSON
/announce | announce new/fulfilled keys

The `/keys` route in phase 3 will be changed to be a mapping of `{node: [keys]}`.

Phase 3 might not ever happen. There is debatable need for this and the performance impact will be noticeable.


## esub phase 4

Phase 4 is adding `HEAD` methods to most routes (with location headers through cluster knowledge or local depending on config). This should be entirely avoidable though, as you should already know the node to respond to (since you have the key as well).

With phase 4 it would be possible to rep a sub on an incorrect node in the same cluster. Likewise to phase 3 though, phase 4 may also never happen. It's here for posterity.


# esub clients

There is a python client library available [here](https://github.com/ccpgames/esub-client). Using a dedicated client for esub is debatable. The intention is for the API to be straight-forward enough to not require one. Basically just use whatever HTTP client library you're comfortable with, use the python client as a guide if you need it.


# docker

A Dockerfile is included in this repo. There is also a `run.sh` script which will build and (re)start an esub container for you, exposed on 8090 with debug logging enabled.


# environment variables

esub uses the following environment variables:

env var               | default            | purpose
----------------------|--------------------|-------------
DATADOG_SERVICE_HOST  | localhost          | datadog host to send metrics to
ESUB_DEBUG            |                    | debug logging (set to anything to enable)
ESUB_ENVIRONMENT_NAME |                    | environment tag value used in metrics
ESUB_METRIC_PREFIX    | esub               | prefix for all metrics
ESUB_NODE_IP          | first non-loopback | node address
ESUB_TAG_KEYS         |                    | include sub keys in metric tags (set to anything to enable)
ESUB_VERBOSE_DEBUG    |                    | verbose debug logging (set to anything to enable)


# bugs

Please use the github issues system for bug reporting and be sure to include repro steps.
