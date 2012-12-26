vertx-redisques
===============

A highly scalable redis-persistent queuing system for vertx.

Dynamic Queues
--------------

They are stored as redis lists, thus created only when needed and removed when empty. 
There is nothing left even if you just used thousands of queues with thousands of messages.

Smart Consuming
---------------

It is guaranteed that a queue is consumed by the same RedisQueVerticle instance (a consumer). 
If no consumer is registered for a queue, one is assigned (this uses redis setnx to ensure only one is assigned). 
When idle for a given time, a consumer is removed. This prevents subscription leaks and makes recovering automatic
when a consumer dies.

Dependencies
------------

RedisQues depends on redis-client module "de.marx-labs.redis-client-v0.3" which currently needs vertx 1.2.
