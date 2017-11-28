/*
 * MIT License
 *
 * Copyright (c) 2017 Anders Mikkelsen
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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
 * This class defines various helpers for circuitbreakers.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
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

    /**
     * For use with debugging circuitbreaker operation.
     *
     * @param circuitBreaker CircuitBreaker
     * @param serviceEvent Message of JsonObject
     */
    public static void handleCircuitBreakerEvent(CircuitBreaker circuitBreaker, Message<JsonObject> serviceEvent) {
        /*logger.trace("Event for: "  + circuitBreaker.name());

        MultiMap headers = serviceEvent.headers();
        JsonObject body = serviceEvent.body();

        logger.trace("CircuitBreaker Event:\n" + Json.encodePrettily(serviceEvent) +
                "\nHeaders:\n" + Json.encodePrettily(headers) +
                "\nBody:\n" + Json.encodePrettily(body));*/
    }
}
