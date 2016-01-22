vertx-redisques
===============

A highly scalable redis-persistent queuing system for vert.x.

Dynamic Queues
--------------

They are stored as redis lists, thus created only when needed and removed when empty. 
There is nothing left even if you just used thousands of queues with thousands of messages.

Smart Consuming
---------------

It is guaranteed that a queue is consumed by the same RedisQuesVerticle instance (a consumer). 
If no consumer is registered for a queue, one is assigned (this uses redis setnx to ensure only one is assigned). 
When idle for a given time, a consumer is removed. This prevents subscription leaks and makes recovering automatic
when a consumer dies.

Safe Distribution
-----------------

There is no single point of control/failure. Just create many instances of RedisQues, they will work together. 
If an instance dies, its queues will be assigned to other instances.

RedisQue APIs
-----------------

Redisque API for Vert.x - Eventbus

**Evenbus Settings:**

	address = redisque

### enqueue ###

Request Data

	{
		"operation": "enqueue",
		"queue": <str QUEUENAME>,
		"message": {
			"method": "POST",
			"uri": <st REQUEST URI>,
			"payload": null
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"message": "enqueued" / <str RESULT>
	}

### check ###

Request Data

	{
		"operation": "check"
	}

Response Data

	{}

### reset ###

Request Data

	{
		"operation": "reset"
	}

Response Data

	{}

### lock ###

Request Data

	{
		"operation": "lock",
		"queue": <str QUEUENAME>
	}

Response Data

	{
		"status": "ok" / "error"
	}

### stop ###

Request Data

	{
		"operation": "stop"
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getListRange ###

Request Data

	{
		"operation": "getListRange",
		"payload": {
			"queuename": <str QUEUENAME>,
			"limit": <str LIMIT>
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <objArr RESULT>
	}

### addItem ###

Request Data

	{
		"operation": "addItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"buffer": <str BUFFERDATA>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getItem ###

Request Data

	{
		"operation": "getItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"index": <int INDEX>
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <obj RESULT>
	}

### replaceItem ###

Request Data

	{
		"operation": "replaceItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"buffer": <str BUFFERDATA>,
			"index": <int INDEX>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### deleteItem ###

Request Data

	{
		"operation": "deleteItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"index": <int INDEX>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### deleteAllQueueItems ###

Request Data

	{
		"operation": "deleteAllQueueItems",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getAllLocks ###

Request Data

	{
		"operation": "getAllLocks",
		"payload": {}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <obj RESULT>
	}

### putLock ###

Request Data

	{
		"operation": "putLock",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getLock ###

Request Data

	{
		"operation": "getLock",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <obj RESULT>
	}

### deleteLock ###

Request Data

	{
		"operation": "deleteLock",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

Dependencies
------------

Module [mod-redis](https://github.com/vert-x/mod-redis) is required and used via the event bus. 
Version 1.0 of Redisques has been tested with Vert.x 2.1M3 and io.vertx~mod-redis~1.1.3

Use gradle with alternative repositories
----------------------------------------
As standard the default maven repositories are set.
You can overwrite these repositories by setting these properties (`-Pproperty=value`):
* `repository` this is the repository where resources are fetched
* `uploadRepository` the repository used in `uploadArchives`
* `repoUsername` the username for uploading archives
* `repoPassword` the password for uploading archives

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/lbovet/vertx-redisques/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
