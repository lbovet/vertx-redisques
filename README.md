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

There is no single point of control/failure. Just create many instances of RedisQuesVerticle, they will work together. 
If an instance dies, its queues will assigned to other instances.

Dependencies
------------

RedisQues depends on redis-client module "de.marx-labs.redis-client-v0.3" which currently needs vertx 1.2.

Run the Example
---------------

Just run the TestVerticle, it listens to 8888 and accepts HTTP PUT requests as messages to queue. 
The URL path is used as queue name.

The message processor is simply a time wait.

The PUT body is the number of milliseconds that the message processor will wait, 
allowing simulation of a long processing.
