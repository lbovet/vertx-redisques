package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.swisspush.redisques.util.RedisquesConfiguration;
import org.swisspush.redisques.util.StatusCode;

import java.nio.charset.Charset;
import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Handler class for HTTP requests providing access to Redisques over HTTP.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesHttpRequestHandler implements Handler<HttpServerRequest> {

    private static final String UTF_8 = "UTF-8";
    private static Logger log = LoggerFactory.getLogger(RedisquesHttpRequestHandler.class);

    private Router router;
    private EventBus eventBus;

    private static final String APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE = "content-type";

    private final String redisquesAddress;
    private final String userHeader;

    public RedisquesHttpRequestHandler(Vertx vertx, RedisquesConfiguration modConfig) {
        this.router = Router.router(vertx);
        this.eventBus = vertx.eventBus();
        this.redisquesAddress = modConfig.getAddress();
        this.userHeader = modConfig.getHttpRequestHandlerUserHeader();

        final String prefix = modConfig.getHttpRequestHandlerPrefix();

        /*
         * List queuing features
         */
        router.get(prefix + "/").handler(ctx -> {
            JsonObject result = new JsonObject();
            JsonArray items = new JsonArray();
            items.add("locks/");
            items.add("monitoring");
            items.add("queues/");
            result.put(lastPart(ctx.request().path(), "/"), items);
            ctx.response().putHeader(CONTENT_TYPE, APPLICATION_JSON);
            ctx.response().end(result.encode());
        });

        /*
         * List or count queues
         */
        router.get(prefix + "/queues/").handler(this::listOrCountQueues);

        /*
         * List or count queue items
         */
        router.getWithRegex(prefix + "/queues/[^/]+").handler(this::listOrCountQueueItems);

        /*
         * Delete all queue items
         */
        router.deleteWithRegex(prefix + "/queues/[^/]+").handler(this::deleteAllQueueItems);

        /*
         * Get single queue item
         */
        router.getWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::getSingleQueueItem);

        /*
         * Replace single queue item
         */
        //TODO implmement

        /*
         * Delete single queue item
         */
        router.deleteWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::deleteQueueItem);

        /*
         * Add queue item
         */
        router.postWithRegex(prefix + "/queues/([^/]+)/").handler(this::addQueueItem);

        /*
         * Get all locks
         */
        router.getWithRegex(prefix + "/locks/").handler(this::getAllLocks);

        /*
         * Add lock
         */
        router.putWithRegex(prefix + "/locks/[^/]+").handler(this::addLock);

        /*
         * Get single lock
         */
        router.getWithRegex(prefix + "/locks/[^/]+").handler(this::getSingleLock);

        /*
         * Delete single lock
         */
        router.deleteWithRegex(prefix + "/locks/[^/]+").handler(this::deleteSingleLock);

        router.routeWithRegex(".*").handler(this::respondMethodNotAllowed);
    }

    @Override
    public void handle(HttpServerRequest request) {
        router.accept(request);
    }

    private void respondMethodNotAllowed(RoutingContext ctx) {
        respondWith(StatusCode.METHOD_NOT_ALLOWED, ctx.request());
    }

    private void getAllLocks(RoutingContext ctx) {
        eventBus.send(redisquesAddress, buildGetAllLocksOperation(), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (OK.equals(reply.result().body().getString(STATUS))) {
                    jsonResponse(ctx.response(), reply.result().body().getJsonObject(VALUE));
                } else {
                    respondWith(StatusCode.NOT_FOUND, ctx.request());
                }
            }
        });
    }

    private void addLock(RoutingContext ctx) {
        String queue = lastPart(ctx.request().path(), "/");
        eventBus.send(redisquesAddress, buildPutLockOperation(queue, extractUser(ctx.request())), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                checkReply(reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
            }
        });
    }

    private void getSingleLock(RoutingContext ctx) {
        String queue = lastPart(ctx.request().path(), "/");
        eventBus.send(redisquesAddress, buildGetLockOperation(queue), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (OK.equals(reply.result().body().getString(STATUS))) {
                    ctx.response().putHeader(CONTENT_TYPE, APPLICATION_JSON);
                    ctx.response().end(reply.result().body().getString(VALUE));
                } else {
                    ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                    ctx.response().setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                    ctx.response().end(NO_SUCH_LOCK);
                }
            }
        });
    }

    private void deleteSingleLock(RoutingContext ctx) {
        String queue = lastPart(ctx.request().path(), "/");
        eventBus.send(redisquesAddress, buildDeleteLockOperation(queue), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                checkReply(reply.result(), ctx.request(), StatusCode.INTERNAL_SERVER_ERROR);
            }
        });
    }

    private void getQueuesCount(RoutingContext ctx) {
        eventBus.send(redisquesAddress, buildGetQueuesCountOperation(), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                    JsonObject result = new JsonObject();
                    result.put("count", reply.result().body().getLong(VALUE));
                    jsonResponse(ctx.response(), result);
                } else {
                    respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error gathering count of active queues", ctx.request());
                }
            }
        });
    }

    private void getQueueItemsCount(RoutingContext ctx) {
        final String queue = lastPart(ctx.request().path(), "/");
        eventBus.send(redisquesAddress, buildGetQueueItemsCountOperation(queue), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                    JsonObject result = new JsonObject();
                    result.put("count", reply.result().body().getLong(VALUE));
                    jsonResponse(ctx.response(), result);
                } else {
                    respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error gathering count of active queue items", ctx.request());
                }
            }
        });
    }

    private void listOrCountQueues(RoutingContext ctx) {
        if (ctx.request().params().contains("count")) {
            getQueuesCount(ctx);
        } else {
            listQueues(ctx);
        }
    }

    private void listQueues(RoutingContext ctx) {
        eventBus.send(redisquesAddress, buildGetQueuesOperation(), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                    jsonResponse(ctx.response(), reply.result().body().getJsonObject(VALUE));
                } else {
                    respondWith(StatusCode.INTERNAL_SERVER_ERROR, "Error gathering names of active queues", ctx.request());
                }
            }
        });
    }

    private void listOrCountQueueItems(RoutingContext ctx) {
        if (ctx.request().params().contains("count")) {
            getQueueItemsCount(ctx);
        } else {
            listQueueItems(ctx);
        }
    }

    private void listQueueItems(RoutingContext ctx) {
        final String queue = lastPart(ctx.request().path(), "/");
        String limitParam = null;
        if (ctx.request() != null && ctx.request().params().contains("limit")) {
            limitParam = ctx.request().params().get("limit");
        }
        eventBus.send(redisquesAddress, buildGetQueueItemsOperation(queue, limitParam), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                JsonObject replyBody = reply.result().body();
                if (OK.equals(replyBody.getString(STATUS))) {
                    List<Object> list = reply.result().body().getJsonArray(VALUE).getList();
                    JsonArray items = new JsonArray();
                    for (Object item : list.toArray()) {
                        items.add((String) item);
                    }
                    JsonObject result = new JsonObject().put(queue, items);
                    jsonResponse(ctx.response(), result);
                } else {
                    ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                    ctx.response().end(reply.result().body().getString("message"));
                    log.warn("Error in routerMatcher.getWithRegEx. Command = '" + (replyBody.getString("command") == null ? "<null>" : replyBody.getString("command")) + "'.");
                }
            }
        });
    }

    private void addQueueItem(RoutingContext ctx) {
        final String queue = part(ctx.request().path(), "/", 1);
        ctx.request().bodyHandler(buffer -> {
            try {
                String strBuffer = encode(buffer.toString());
                eventBus.send(redisquesAddress, buildAddQueueItemOperation(queue, strBuffer), new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                        checkReply(reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                    }
                });
            } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
            }
        });
    }

    private void getSingleQueueItem(RoutingContext ctx) {
        final String queue = lastPart(ctx.request().path().substring(0, ctx.request().path().length() - 2), "/");
        final int index = Integer.parseInt(lastPart(ctx.request().path(), "/"));
        eventBus.send(redisquesAddress, buildGetQueueItemOperation(queue, index), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                JsonObject replyBody = reply.result().body();
                if (OK.equals(replyBody.getString(STATUS))) {
                    ctx.response().putHeader(CONTENT_TYPE, APPLICATION_JSON);
                    ctx.response().end(decode(reply.result().body().getString(VALUE)));
                } else {
                    ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                    ctx.response().setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                    ctx.response().end("Not Found");
                }
            }
        });
    }

    private void deleteQueueItem(RoutingContext ctx) {
        final String queue = part(ctx.request().path(), "/", 2);
        final int index = Integer.parseInt(lastPart(ctx.request().path(), "/"));
        checkLocked(queue, ctx.request(), aVoid -> eventBus.send(redisquesAddress, buildDeleteQueueItemOperation(queue, index), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                checkReply(reply.result(), ctx.request(), StatusCode.NOT_FOUND);
            }
        }));
    }

    private void deleteAllQueueItems(RoutingContext ctx) {
        final String queue = lastPart(ctx.request().path(), "/");
        eventBus.send(redisquesAddress, buildDeleteAllQueueItemsOperation(queue), reply -> {
            ctx.response().end();
        });
    }

    private void respondWith(StatusCode statusCode, String responseMessage, HttpServerRequest request) {
        log.info("Responding with status code " + statusCode + " and message: " + responseMessage);
        request.response().setStatusCode(statusCode.getStatusCode());
        request.response().setStatusMessage(statusCode.getStatusMessage());
        request.response().end(responseMessage);
    }

    private void respondWith(StatusCode statusCode, HttpServerRequest request) {
        respondWith(statusCode, statusCode.getStatusMessage(), request);
    }

    private String lastPart(String source, String separator) {
        String[] tokens = source.split(separator);
        return tokens[tokens.length - 1];
    }

    private String part(String source, String separator, int pos) {
        String[] tokens = source.split(separator);
        return tokens[tokens.length - pos];
    }

    private void jsonResponse(HttpServerResponse response, JsonObject object) {
        response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
        response.end(object.encode());
    }

    private String extractUser(HttpServerRequest request) {
        String user = request.headers().get(userHeader);
        if (user == null) {
            user = "Unknown";
        }
        return user;
    }

    private void checkLocked(String queue, final HttpServerRequest request, final Handler<Void> handler) {
        request.pause();
        eventBus.send(redisquesAddress, buildGetLockOperation(queue), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (NO_SUCH_LOCK.equals(reply.result().body().getString(STATUS))) {
                    request.resume();
                    request.response().setStatusCode(StatusCode.CONFLICT.getStatusCode());
                    request.response().setStatusMessage("Queue must be locked to perform this operation");
                    request.response().end("Queue must be locked to perform this operation");
                } else {
                    handler.handle(null);
                    request.resume();
                }
            }
        });
    }

    private void checkReply(Message<JsonObject> reply, HttpServerRequest request, StatusCode statusCode) {
        if (OK.equals(reply.body().getString(STATUS))) {
            request.response().end();
        } else {
            request.response().setStatusCode(statusCode.getStatusCode());
            request.response().setStatusMessage(statusCode.getStatusMessage());
            request.response().end(statusCode.getStatusMessage());
        }
    }

    /**
     * Encode the payload from a payloadString or payloadObjet.
     *
     * @param decoded decoded
     * @return String
     */
    public String encode(String decoded) throws Exception {
        JsonObject object = new JsonObject(decoded);

        String payloadString;
        JsonObject payloadObject = object.getJsonObject("payloadObject");
        if (payloadObject != null) {
            payloadString = payloadObject.encode();
        } else {
            payloadString = object.getString("payloadString");
        }

        if (payloadString != null) {
            object.put(PAYLOAD, payloadString.getBytes(Charset.forName(UTF_8)));
            object.remove("payloadString");
            object.remove("payloadObject");
        }

        // update the content-length
        int length = 0;
        if (object.containsKey(PAYLOAD)) {
            length = object.getBinary(PAYLOAD).length;
        }
        JsonArray newHeaders = new JsonArray();
        for (Object headerObj : object.getJsonArray("headers")) {
            JsonArray header = (JsonArray) headerObj;
            String key = header.getString(0);
            if (key.equalsIgnoreCase("content-length")) {
                JsonArray contentLengthHeader = new JsonArray();
                contentLengthHeader.add("Content-Length");
                contentLengthHeader.add(Integer.toString(length));
                newHeaders.add(contentLengthHeader);
            } else {
                newHeaders.add(header);
            }
        }
        object.put("headers", newHeaders);

        return object.toString();
    }

    /**
     * Decode the payload if the content-type is text or json.
     *
     * @param encoded encoded
     * @return String
     */
    public String decode(String encoded) {
        JsonObject object = new JsonObject(encoded);
        JsonArray headers = object.getJsonArray("headers");
        for (Object headerObj : headers) {
            JsonArray header = (JsonArray) headerObj;
            String key = header.getString(0);
            String value = header.getString(1);
            if (key.equalsIgnoreCase(CONTENT_TYPE) && (value.contains("text/") || value.contains(APPLICATION_JSON))) {
                try {
                    object.put("payloadObject", new JsonObject(new String(object.getBinary(PAYLOAD), Charset.forName(UTF_8))));
                } catch (DecodeException e) {
                    object.put("payloadString", new String(object.getBinary(PAYLOAD), Charset.forName(UTF_8)));
                }
                object.remove(PAYLOAD);
                break;
            }
        }
        return object.toString();
    }
}
