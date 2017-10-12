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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by anders on 28/12/2016.
 */
public class APIManager {
    private static final Logger logger = LoggerFactory.getLogger(APIManager.class.getSimpleName());

    private static final String GENERIC_HTTP_REQUEST_CIRCUITBREAKER = "com.apis.generic.circuitbreaker";

    private static final String AWS_PUBLIC_HOST = "www.appifycloud.com";
    private static final String AWS_HOST = "internal-ECS-Data-API-Internal-1284241550.eu-west-1.elb.amazonaws.com";
    private static final String TRANSCODER_HOST = "transcoder.nannoq.com";

    private static final String API_CIRCUIT_BREAKER_BASE = "com.apis.circuitbreaker.";

    private static final String GCM_BASE = "/gcm";
    public static final String GCM_NOTIFICATION_ENDPOINT = GCM_BASE + "/api/notification/:feedId";
    public static final String GCM_USER_NOTIFICATION_ENDPOINT = GCM_BASE + "/api/notification/:feedId/users/:userId";

    private static final String AGGREGATOR_BASE = "/aggregator";
    public static final String INTERNAL_AGGREGATOR_ENDPOINT = AGGREGATOR_BASE + "/api/collectors/feeds/:hash/providers/:provider/providerFeeds/:id";

    public static final String DATA_API_BASE = "/api/v2";
    public static final String EXCLUSIVE_TRANSACTIONS_ENDPOINT = DATA_API_BASE + "/transactions/exclusive/:id";

    public static final String GCM_DEVICE_GROUP_BASE = "android.googleapis.com";
    public static final String GCM_DEVICE_GROUP_HTTP_ENDPOINT = "/gcm/notification";
    public static final String GCM_DEVICE_GROUP_HTTP_ENDPOINT_COMPLETE = "https://" + GCM_DEVICE_GROUP_BASE + "/gcm/notification";

    public static final String TRANSCODER_BASE = "/transcoder";
    public static final String TRANSCODER_UPLOAD_ENDPOINT = "/feeds/:feedId/transactions/:transactionId/upload";

    //Data API
    public static final String DATA_API_FAVOURITES_GET = "/api/v2/updates/feeds/:hash/feedItems/favorites";
    public static final String DATA_API_FAVOURITES_PUT = "/api/v2/updates/feeds/:hash/feedItems/favorites";

    private static final boolean dev;

    public static String getTranscoderHost() {
        return dev ? "localhost:5443" : TRANSCODER_HOST;
    }

    public enum API { DATA, DATA_2, DATA_LOCAL, AUTH, AUTH_LOCAL, AGGREGATOR, AGGREGATOR_LOCAL, APPCREATOR, APPCREATOR_LOCAL, GCM, GCM_LOCAL, TRANSCODER, TRANSCODER_LOCAL }

    private final Vertx vertx;
    private final JsonObject appConfig;
    private Map<API, CircuitBreaker> circuitBreakerMap;
    private Map<API, MessageConsumer<JsonObject>> circuitBreakerMessageConsumerMap;

    static {
        final Boolean devBool = Vertx.currentContext().config().getBoolean("dev");
        dev = devBool != null && devBool;
    }

    public APIManager(Vertx vertx, JsonObject appConfig) {
        this.vertx = vertx;
        this.appConfig = appConfig;
        circuitBreakerMap = new ConcurrentHashMap<>();
        circuitBreakerMessageConsumerMap = new ConcurrentHashMap<>();

        prepareCircuitBreakers();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                circuitBreakerMessageConsumerMap.values().forEach(consumer -> {
                    consumer.unregister();

                    logger.info("Unregistered API circuitbreaker Consumer: " + consumer.address());
                })));
    }

    private void prepareCircuitBreakers() {
        Arrays.stream(API.values()).forEach(api -> {
            String cicuitBreakerName = API_CIRCUIT_BREAKER_BASE + api.name();
            CircuitBreaker circuitBreaker = CircuitBreaker.create(cicuitBreakerName, vertx,
                    new CircuitBreakerOptions()
                            .setMaxFailures(3)
                            .setTimeout(30000)
                            .setFallbackOnFailure(true)
                            .setResetTimeout(10000)
                            .setNotificationAddress(cicuitBreakerName)
                            .setNotificationPeriod(60000L * 60 * 6))
                    .openHandler(v -> logger.info(cicuitBreakerName + " OPEN"))
                    .halfOpenHandler(v -> logger.info(cicuitBreakerName + " HALF-OPEN"))
                    .closeHandler(v -> logger.info(cicuitBreakerName + " CLOSED"));
            circuitBreaker.close();

            MessageConsumer<JsonObject> apiConsumer = vertx.eventBus().consumer(cicuitBreakerName);
            apiConsumer.handler(message -> CircuitBreakerUtils.handleCircuitBreakerEvent(circuitBreaker, message));

            circuitBreakerMap.put(api, circuitBreaker);
        });
    }

    public <T> void performRequestWithCircuitBreaker(API api, Handler<AsyncResult<T>> resultHandler,
                                                     Handler<Future<T>> handler, Consumer<Throwable> fallback) {
        CircuitBreakerUtils.performRequestWithCircuitBreaker(
                circuitBreakerMap.get(api), resultHandler, handler, fallback);
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

    public static Record createInternalApiRecord(API api) {
        if (!api.name().contains("LOCAL")) throw new UnsupportedOperationException("API Name does not contain LOCAL");
        
        String host = "", root = "";

        switch (api) {
            case DATA_LOCAL:
                host = dev ? "data-api-nginx" : AWS_HOST;
                root = "/api/v2";

                break;
            case AUTH_LOCAL:
                host = dev ? "auth-nginx" : AWS_HOST;
                root = "/auth";

                break;
            case AGGREGATOR_LOCAL:
                host = dev ? "aggregator-nginx" : AWS_HOST;
                root = "/aggregator";

                break;
            case APPCREATOR_LOCAL:
                host = dev ? "appcreator-nginx" : AWS_HOST;
                root = "/appcreator";

                break;
            case GCM_LOCAL:
                host = dev ? "gcm-nginx" : AWS_HOST;
                root = "/gcm";

                break;
            case TRANSCODER_LOCAL:
                if (!dev) throw new UnsupportedOperationException("Transcoder is not running on AWS!");
                host = "transcoder-nginx";
                root = "/transcoder";

                break;
        }

        return HttpEndpoint.createRecord(api.name(), false, host, 80, root, null);
    }

    public static Record createExternalApiRecord(API api) {
        String root = "";
        String host = "";

        switch (api) {
            case DATA:
                host = dev ? "data-api-nginx" : AWS_PUBLIC_HOST;
                root = "/api/v2";

                break;
            case AUTH:
                host = dev ? "auth-nginx" : AWS_PUBLIC_HOST;
                root = "/auth";

                break;
            case AGGREGATOR:
                host = dev ? "aggregator-nginx" : AWS_PUBLIC_HOST;
                root = "/aggregator";

                break;
            case APPCREATOR:
                host = dev ? "appcreator-nginx" : AWS_PUBLIC_HOST;
                root = "/appcreator";

                break;
            case GCM:
                host = dev ? "gcm-nginx" : AWS_PUBLIC_HOST;
                root = "/gcm";

                break;
            case TRANSCODER:
                host = dev ? "transcoder-nginx" : TRANSCODER_HOST;
                root = "/transcoder";

                break;
        }

        return HttpEndpoint.createRecord(api.name(), true, host, dev ? 80 : 443, root, null);
    }
}
