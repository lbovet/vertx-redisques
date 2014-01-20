vertx-redisques
===============

A highly scalable redis-persistent queuing system for Vert.x.

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

Dependencies
------------

Module [mod-redis](https://github.com/vert-x/mod-redis) is required and used via the event bus. 
Version 1.0 of Redisques has been tested with Vert.x 2.1M3 and io.vertx~mod-redis~1.1.3

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/lbovet/vertx-redisques/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

