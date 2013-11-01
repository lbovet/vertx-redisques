vertx-redisques
===============

A highly scalable redis-persistent queuing system for vertx.

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

RedisQues 0.2 use the redis-client busmod "de.marx-labs.redis-client" which currently (as of version v0.3) needs vertx 1.2 
and does not work with vertx 1.3.

RedisQues 0.3 is built for vertx 1.3 and thus requires a "de.marx-labs.redis-client" compatible with vertx 1.3. 
Until it is officially provided, here is a fork that works with vertx 1.3: https://github.com/lbovet/vert.x-busmod-redis, 
you will need to build it by yourself, though.


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/lbovet/vertx-redisques/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

