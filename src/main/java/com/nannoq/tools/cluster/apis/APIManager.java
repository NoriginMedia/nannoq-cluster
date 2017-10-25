package com.nannoq.tools.cluster.apis;

import com.nannoq.tools.cluster.CircuitBreakerUtils;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.types.HttpEndpoint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by anders on 28/12/2016.
 */
public class APIManager {
    private static final Logger logger = LoggerFactory.getLogger(APIManager.class.getSimpleName());

    private static final String GENERIC_HTTP_REQUEST_CIRCUITBREAKER = "com.apis.generic.circuitbreaker";
    private static final String API_CIRCUIT_BREAKER_BASE = "com.apis.circuitbreaker.";

    private final Vertx vertx;
    private final APIHostProducer apiHostProducer;
    private Map<String, CircuitBreaker> circuitBreakerMap;
    private Map<String, MessageConsumer<JsonObject>> circuitBreakerMessageConsumerMap;

    private String publicHost, privateHost;

    public APIManager(Vertx vertx, JsonObject appConfig) {
        this(vertx, appConfig, null);
    }

    public APIManager(Vertx vertx, JsonObject appConfig, APIHostProducer apiHostProducer) {
        this.vertx = vertx;
        this.apiHostProducer = apiHostProducer;
        circuitBreakerMap = new ConcurrentHashMap<>();
        circuitBreakerMessageConsumerMap = new ConcurrentHashMap<>();

        publicHost = appConfig.getString("publicHost");
        privateHost = appConfig.getString("privateHost");

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                circuitBreakerMessageConsumerMap.values().forEach(consumer -> {
                    consumer.unregister();

                    logger.info("Unregistered API circuitbreaker Consumer: " + consumer.address());
                })));
    }

    private CircuitBreaker prepareCircuitBreaker(String path) {
        final CircuitBreaker existingCircuitBreaker = circuitBreakerMap.get(path);
        if (existingCircuitBreaker != null) return existingCircuitBreaker;

        String circuitBreakerName = API_CIRCUIT_BREAKER_BASE + path;
        CircuitBreaker circuitBreaker = CircuitBreaker.create(circuitBreakerName, vertx,
                new CircuitBreakerOptions()
                        .setMaxFailures(3)
                        .setTimeout(30000)
                        .setFallbackOnFailure(true)
                        .setResetTimeout(10000)
                        .setNotificationAddress(circuitBreakerName)
                        .setNotificationPeriod(60000L * 60 * 6))
                .openHandler(v -> logger.info(circuitBreakerName + " OPEN"))
                .halfOpenHandler(v -> logger.info(circuitBreakerName + " HALF-OPEN"))
                .closeHandler(v -> logger.info(circuitBreakerName + " CLOSED"));
        circuitBreaker.close();

        MessageConsumer<JsonObject> apiConsumer = vertx.eventBus().consumer(circuitBreakerName);
        apiConsumer.handler(message -> CircuitBreakerUtils.handleCircuitBreakerEvent(circuitBreaker, message));

        circuitBreakerMap.put(path, circuitBreaker);

        return circuitBreaker;
    }

    public <T> void performRequestWithCircuitBreaker(String path, Handler<AsyncResult<T>> resultHandler,
                                                     Handler<Future<T>> handler,
                                                     Consumer<Throwable> fallback) {
        CircuitBreakerUtils.performRequestWithCircuitBreaker(
                prepareCircuitBreaker(path), resultHandler, handler, fallback);
    }

    public static <T> void performRequestWithCircuitBreaker(Handler<AsyncResult<T>> resultHandler,
                                                            Handler<Future<T>> handler,
                                                            Consumer<Throwable> fallback) {
        CircuitBreakerUtils.performRequestWithCircuitBreaker(
                CircuitBreaker.create(GENERIC_HTTP_REQUEST_CIRCUITBREAKER, Vertx.currentContext().owner(),
                        new CircuitBreakerOptions()
                                .setMaxFailures(5)
                                .setFallbackOnFailure(true)
                                .setTimeout(5000L)
                                .setNotificationAddress(GENERIC_HTTP_REQUEST_CIRCUITBREAKER)
                                .setNotificationPeriod(60000L)),
                resultHandler, handler, fallback);
    }

    public Record createInternalApiRecord(String name, String path) {
        return createInternalApiRecord(name, path, true);
    }

    public Record createInternalApiRecord(String name, String path, boolean ssl) {
        return HttpEndpoint.createRecord(name, ssl,
                apiHostProducer == null ? privateHost : apiHostProducer.getInternalHost(name), ssl ? 443 : 80, path, null);
    }

    public Record createExternalApiRecord(String name, String path) {
        return createExternalApiRecord(name, path, true);
    }

    public Record createExternalApiRecord(String name, String path, boolean ssl) {
        return HttpEndpoint.createRecord(name, ssl,
                apiHostProducer == null ? publicHost : apiHostProducer.getExternalHost(name), ssl ? 443 : 80, path, null);
    }
}