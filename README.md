# vertx-redisques

[![Build Status](https://travis-ci.org/swisspush/vertx-redisques.svg?branch=master)](https://travis-ci.org/swisspush/vertx-redisques)
[![codecov](https://codecov.io/gh/swisspush/vertx-redisques/branch/master/graph/badge.svg)](https://codecov.io/gh/swisspush/vertx-redisques)
[![Maven Central](https://img.shields.io/maven-central/v/org.swisspush/redisques.svg)](http://search.maven.org/#artifactdetails|org.swisspush|redisques|2.2.0|)

A highly scalable redis-persistent queuing system for vert.x.

## Getting Started
### Install
* Clone this repository or unzip [archive](https://github.com/swisspush/vertx-redisques/archive/master.zip)
* Install and start Redis
  * Debian/Ubuntu: `apt-get install redis-server`
  * Fedora/RedHat/CentOS: `yum install redis`
  * OS X: `brew install redis`
  * [Windows](https://github.com/MSOpenTech/redis/releases/download/win-2.8.2400/Redis-x64-2.8.2400.zip)
  * [Other](http://redis.io/download)

### Build
You need Java 8 and gradle.
```
cd vertx-redisques
gradle build
```

## Dynamic Queues

They are stored as redis lists, thus created only when needed and removed when empty. 
There is nothing left even if you just used thousands of queues with thousands of messages.

## Smart Consuming

It is guaranteed that a queue is consumed by the same RedisQuesVerticle instance (a consumer). 
If no consumer is registered for a queue, one is assigned (this uses redis setnx to ensure only one is assigned).
When idle for a given time, a consumer is removed. This prevents subscription leaks and makes recovering automatic
when a consumer dies.

## Safe Distribution

There is no single point of control/failure. Just create many instances of RedisQues, they will work together.
If an instance dies, its queues will be assigned to other instances.

## Configuration

The following configuration values are available:

| Property | Default value | Description |
|:--------- | :----------- | :----------- |
| address | redisques | The eventbus address the redisques module is listening to |
| redis-prefix | redisques: | Prefix for redis keys holding queues and consumers |
| processor-address | redisques-processor | Address of message processors |
| refresh-period | 10 | The frequency [s] of consumers refreshing their subscriptions to consume |
| redisHost | localhost | The host where redis is running on |
| redisPort | 6379 | The port where redis is running on |
| redisEncoding | UTF-8 | The encoding to use in redis |
| checkInterval | 60 | The interval [s] to check timestamps of not-active / empty queues by executing **check** queue operation. _checkInterval_ value must be greater 0, otherwise the default is used. |
| httpRequestHandlerEnabled | false | Enable / disable the HTTP API |
| httpRequestHandlerPrefix | /queuing | The url prefix for all HTTP API endpoints |
| httpRequestHandlerPort | 7070 | The port of the HTTP API |
| httpRequestHandlerUserHeader | x-rp-usr | The name of the header property where the user information is provided. Used for the HTTP API  |

### Configuration util

The configurations have to be passed as JsonObject to the module. For a simplyfied configuration the _RedisquesConfigurationBuilder_ can be used.

Example:

```java
RedisquesConfiguration config = RedisquesConfiguration.with()
		.redisHost("anotherhost")
		.redisPort(1234)
		.build();

JsonObject json = config.asJsonObject();
```

Properties not overriden will not be changed. Thus remaining default.

To use default values only, the _RedisquesConfiguration_ constructor without parameters can be used:

```java
JsonObject json  = new RedisquesConfiguration().asJsonObject();
```

## RedisQues APIs

Redisques API for Vert.x - Eventbus

**Evenbus Settings:**

> address = redisque

### RedisquesAPI util
For a simplyfied working with the Redisques module, see the RedisquesAPI class:

> org.swisspush.redisques.util.RedisquesAPI

This class provides utility methods for a simple configuration of the queue operations. See _Queue operations_ chapter below for details.

### Queue operations
The following operations are available in the Redisques module.

#### enqueue

Request Data

```
{
    "operation": "enqueue",
    "payload": {
        "queuename": <str QUEUENAME>
    },
    "message": {
        "method": "POST",
        "uri": <st REQUEST URI>,
        "payload": null
    }
}
```

Response Data

```
{
    "status": "ok" / "error",
    "message": "enqueued" / <str RESULT>
}
```

#### lockedEnqueue
Request Data

```
{
    "operation": "lockedEnqueue",
    "payload": {
        "queuename": <str QUEUENAME>,
        "requestedBy": <str user who created the lock>
    },
    "message": {
        "method": "POST",
        "uri": <st REQUEST URI>,
        "payload": null
    }
}
```
Response Data
```
{
    "status": "ok" / "error",
    "message": "enqueued" / <str RESULT>
}
```

#### getQueues

Request Data

```
{
    "operation": "getQueues"
}
```

Response Data

```
{
    "status": "ok" / "error",
    "value": <objArr RESULT>
}
```

#### getQueuesCount

Request Data

```
{
    "operation": "getQueuesCount"
}
```

Response Data

```
{
    "status": "ok" / "error",
    "value": <long RESULT>
}
```

#### getQueueItemsCount

Request Data
```
{
    "operation": "getQueueItemsCount",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <long RESULT>
}
```

#### check

Request Data
```
{
    "operation": "check"
}
```

Response Data
```
{}
```

#### reset

Request Data
```
{
    "operation": "reset"
}
```

Response Data
```
{}
```

#### stop

Request Data
```
{
    "operation": "stop"
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getQueueItems

Request Data
```
{
    "operation": "getQueueItems",
    "payload": {
        "queuename": <str QUEUENAME>,
        "limit": <str LIMIT>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <objArr RESULT>,
    "info": <nbrArray with result array (value property) size and total queue item count (can be greater than limit)>
}
```

#### addQueueItem

Request Data
```
{
    "operation": "addQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "buffer": <str BUFFERDATA>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getQueueItem

Request Data
```
{
    "operation": "getQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "index": <int INDEX>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### replaceQueueItem

Request Data
```
{
    "operation": "replaceQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "buffer": <str BUFFERDATA>,
        "index": <int INDEX>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### deleteQueueItem

Request Data
```
{
    "operation": "deleteQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "index": <int INDEX>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### deleteAllQueueItems

Request Data
```
{
    "operation": "deleteAllQueueItems",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getAllLocks

Request Data
```
{
    "operation": "getAllLocks"
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### putLock

Request Data
```
{
    "operation": "putLock",
    "payload": {
        "queuename": <str QUEUENAME>,
        "requestedBy": <str user who created the lock>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getLock

Request Data
```
{
    "operation": "getLock",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### deleteLock

Request Data
```
{
    "operation": "deleteLock",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

## RedisQues HTTP API
RedisQues provides a HTTP API to modify queues, queue items and get information about queue counts and queue item counts.

### Configuration
To enable the HTTP API, the _httpRequestHandlerEnabled_ configuration property has to be set to _TRUE_.

For additional configuration options relating the HTTP API, see the [Configuration](#configuration) section.

### API Endpoints
The following request examples will use a configured prefix of _/queuing_

### List endpoints
To list the available endpoints use
> GET /queuing

The result will be a json object with the available endpoints like the example below

```json
{
  "queuing": [
    "locks/",
    "queues/",
    "monitor/"
  ]
}
```

### Get monitor information
The monitor information contains the active queues and their queue items count. To get the monitor information use
> GET /queuing/monitor

Available url parameters are:
* _limit_: The maximum amount of queues to list
* _emptyQueues_: Also show empty queues

The result will be a json object with the monitor information like the example below

```json
{
  "queues": [
    {
      "name": "queue_1",
      "size": 3
    },
    {
      "name": "queue_2",
      "size": 2
    }
  ]
}
```

### Enqueue
To enqueue a new queue use
> PUT /queuing/enqueue/myNewQueue

having the payload in the request body. When the request body is not a valid json object, a statusCode 400 with the error message _'Bad Request'_ will be returned.

Available url parameters are:
* _locked_: Lock the queue before enqueuing to prevent processing

When the _locked_ url parameter is set, the configured _httpRequestHandlerUserHeader_ property will be used to define the user which requested the lock. If no header is provided, "Unknown" will be used instead.

### List or count queues
To list the active queues use
> GET /queuing/queues

The result will be a json object with a list of active queues like the example below

```json
{
  "queues": [
    "queue_1",
    "queue_2",
    "queue_3"
  ]
}
```
**Attention:** The result will also contain empty queues when requested before the internal cleanup has passed. Use the monitor endpoint when non-empty queues should be listed only.

To get the count of active queues only, use
> GET /queuing/queues?count

The result will be a json object with the count of active queues like the example below

```json
{
  "count": 3
}
```
**Attention:** The count will also contain empty queues when requested before the internal cleanup has passed. Use the monitor endpoint when non-empty queues should be counted only.

### List or count queue items
To list the queue items of a single queue use
> GET /queuing/queues/myQueue

Add the _limit_ url parameter to define the maximum amount of queue items to retrieve

The result will be a json object with a list of queue items like the example below

```json
{
  "myQueue": [
    "queueItem1",
    "queueItem2",
    "queueItem3",
    "queueItem4"
  ]
}
```

To get the count of queue items only, use
> GET /queuing/queues/myQueue?count

The result will be a json object with the count of queue items like the example below

```json
{
  "count": 4
}
```

### Delete all queue items
To delete all queue items of a single queue use
> DELETE /queuing/queues/myQueue

### Get single queue item
To get a single queue item use
> GET /queuing/queues/myQueue/0

The result will be a json object representing the queue item at the given index (0 in the example above). When no queue item at the given index exists, a statusCode 404 with the error message _'Not Found'_ will be returned.

### Replace single queue item
To replace a single queue item use
> PUT /queuing/queues/myQueue/0

having the payload in the request body. The queue must be locked to perform this operation, otherwise a statusCode 409 with the error message _'Queue must be locked to perform this operation'_ will be returned. When no queue item at the given index exists, a statusCode 404 with the error message _'Not Found'_ will be returned.

### Delete single queue item
To delete a single queue item use
> DELETE /queuing/queues/myQueue/0

The queue must be locked to perform this operation, otherwise a statusCode 409 with the error message _'Queue must be locked to perform this operation'_ will be returned. When no queue item at the given index exists, a statusCode 404 with the error message _'Not Found'_ will be returned.

### Add queue item
To add a queue item (to the end of the queue) use
> POST /queuing/queues/myQueue/

having the payload in the request body. When the request body is not a valid json object, a statusCode 400 with the error message _'Bad Request'_ will be returned.

### Get all locks
To list all existing locks use
> GET /queuing/locks/

The result will be a json object with a list of all locks like the example below

```json
{
  "locks": [
    "queue1",
    "queue2"
  ]
}
```

### Add lock
To add a lock use
> PUT /queuing/locks/myQueue

having an empty json object {} in the body. The configured _httpRequestHandlerUserHeader_ property will be used to define the user which requested the lock. If no header is provided, "Unknown" will be used instead.

### Get single lock
To get a single lock use
> GET /queuing/locks/queue1

The result will be a json object with the lock information like the example below

```json
{
  "requestedBy": "someuser",
  "timestamp": 1478100330698
}
```

### Delete single lock
To delete a single lock use
> DELETE /queuing/locks/queue1

## Dependencies

Redisques versions greater than 01.00.17 depend on Vert.x v3.2.0 and therefore require Java 8.

## Use gradle with alternative repositories

As standard the default maven repositories are set.
You can overwrite these repositories by setting these properties (`-Pproperty=value`):
* `repository` this is the repository where resources are fetched
* `uploadRepository` the repository used in `uploadArchives`
* `repoUsername` the username for uploading archives
* `repoPassword` the password for uploading archives

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/lbovet/vertx-redisques/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
