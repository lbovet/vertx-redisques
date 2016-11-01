package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.swisspush.redisques.util.StatusCode;

import java.util.List;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Handler class for HTTP requests providing access to Redisques over HTTP.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesHttpRequestHandler implements Handler<HttpServerRequest> {

    private static Logger log = LoggerFactory.getLogger(RedisquesHttpRequestHandler.class);

    private Router router;
    private EventBus eventBus;

    public static final String APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE = "content-type";

    private final String redisquesAddress;

    public RedisquesHttpRequestHandler(Vertx vertx, String prefix, String redisquesAddress) {
        this.router = Router.router(vertx);
        this.eventBus = vertx.eventBus();
        this.redisquesAddress = redisquesAddress;

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
         * List queues
         */
        router.get(prefix + "/queues/").handler(this::listQueues);

        /*
         * List queue items
         */
        router.getWithRegex(prefix + "/queues/[^/]+").handler(this::listQueueItems);

        /*
         * Delete all queue items
         */
        router.deleteWithRegex(prefix + "/queues/[^/]+").handler(this::deleteAllQueueItems);

        /*
         * Get single queue item
         */

        router.routeWithRegex(".*").handler(this::respondMethodNotAllowed);
    }

    @Override
    public void handle(HttpServerRequest request) {
        router.accept(request);
    }

    private void respondMethodNotAllowed(RoutingContext ctx){
        respondWith(StatusCode.METHOD_NOT_ALLOWED, ctx.request());
    }

    private void listQueues(RoutingContext ctx){
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

    private void listQueueItems(RoutingContext ctx){
        final String queue = lastPart(ctx.request().path(), "/");
        String limitParam = null;
        if(ctx.request() != null && ctx.request().params().contains("limit")) {
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

    private void deleteAllQueueItems(RoutingContext ctx){
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

    private void jsonResponse(HttpServerResponse response, JsonObject object) {
        response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
        response.end(object.encode());
    }
}
