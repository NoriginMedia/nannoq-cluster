package com.nannoq.tools.cluster;

import com.nannoq.tools.cluster.services.ServiceManager;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

import java.util.function.Consumer;

/**
 * Created by anders on 30/12/2016.
 */
public class CircuitBreakerUtils {
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerUtils.class.getSimpleName());

    public static <T> void performRequestWithCircuitBreaker(CircuitBreaker circuitBreaker,
                                                            Handler<AsyncResult<T>> resultHandler,
                                                            Handler<Future<T>> handler,
                                                            Consumer<Throwable> backup) {
        Future<T> result = Future.future();
        result.setHandler(operationResult -> {
            logger.debug("Received " + circuitBreaker.name() + " Result: " + operationResult.succeeded());

            if (operationResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(operationResult.result()));
            } else {
                logger.debug("Failed: " + operationResult.cause());

                if (operationResult.cause() instanceof ServiceException) {
                    ServiceManager.handleResultFailed(operationResult.cause());

                    resultHandler.handle(Future.failedFuture(operationResult.cause()));
                } else {
                    if (operationResult.cause() != null && operationResult.cause().getMessage().equals("operation timeout")) {
                        logger.error(circuitBreaker.name() + " Timeout, failures: " +
                                circuitBreaker.failureCount() + ", state: " + circuitBreaker.state().name());
                    }

                    backup.accept(operationResult.cause());
                }
            }
        });

        circuitBreaker.executeAndReport(result, handler);
    }

    public static void handleCircuitBreakerEvent(CircuitBreaker circuitBreaker, Message<JsonObject> serviceEvent) {
        /*logger.trace("Event for: "  + circuitBreaker.name());

        MultiMap headers = serviceEvent.headers();
        JsonObject body = serviceEvent.body();

        logger.trace("CircuitBreaker Event:\n" + Json.encodePrettily(serviceEvent) +
                "\nHeaders:\n" + Json.encodePrettily(headers) +
                "\nBody:\n" + Json.encodePrettily(body));*/
    }
}
