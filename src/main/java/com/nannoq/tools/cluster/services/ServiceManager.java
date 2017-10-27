package com.nannoq.tools.cluster.services;

import com.nannoq.tools.cluster.apis.APIManager;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.servicediscovery.types.HttpEndpoint;
import io.vertx.serviceproxy.ProxyHelper;
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.serviceproxy.ServiceException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by anders on 25/12/2016.
 */
public class ServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(ServiceManager.class.getSimpleName());

    private static final String NANNOQ_SERVICE_ANNOUNCE_ADDRESS = "com.nannoq.services.manager.announce";
    private static final String NANNOQ_SERVICE_SERVICE_NAME = "nannoq-service-manager-service-discovery";

    private static final int NOT_FOUND = 404;
    private static final int INTERNAL_ERROR = 500;

    private ServiceDiscovery serviceDiscovery;
    private static ConcurrentHashSet<MessageConsumer<JsonObject>> registeredServices = new ConcurrentHashSet<>();
    private static ConcurrentHashSet<Record> registeredRecords = new ConcurrentHashSet<>();
    private static ConcurrentHashMap<String, Object> fetchedServices = new ConcurrentHashMap<>();

    private static Vertx vertx;
    private static ServiceManager instance = null;
    private static MessageConsumer<JsonObject> serviceAnnounceConsumer;

    private ServiceManager() {
        this(Vertx.currentContext().owner());
    }

    private ServiceManager(Vertx vertx) {
        ServiceManager.vertx = vertx;
        openDiscovery();
    }

    public static ServiceManager getInstance() {
        if (instance == null) {
            instance = new ServiceManager();
        }

        return instance;
    }

    public static ServiceManager getInstance(Vertx vertx) {
        if (instance == null) {
            instance = new ServiceManager(vertx);
        }

        return instance;
    }

    public void kill() {
        System.out.println("Destroying ServiceManager");

        Future<Boolean> waitForKilled = Future.future();

        if (serviceDiscovery != null) {
            System.out.println("Unpublishing all records...");

            List<Future> unPublishFutures = new ArrayList<>();

            registeredRecords.forEach(record -> {
                Future<Boolean> unpublish = Future.future();

                serviceDiscovery.unpublish(record.getRegistration(), unpublishResult -> {
                    if (unpublishResult.failed()) {
                        System.out.println("Failed Unpublish: " + record.getName());

                        unpublishResult.cause().printStackTrace();

                        unpublish.fail(unpublishResult.cause());
                    } else {
                        System.out.println("Unpublished: " + record.getName());

                        unpublish.complete();
                    }
                });

                unPublishFutures.add(unpublish);
            });

            CompositeFuture.all(unPublishFutures).setHandler(res -> {
                try {
                    System.out.println("UnPublish complete, Unregistering all services...");

                    registeredServices.forEach(service -> {
                        ProxyHelper.unregisterService(service);

                        System.out.println("Unregistering " + service.address());
                    });

                    System.out.println("Releasing all consumed service objects...");

                    fetchedServices.values().forEach(service ->
                            ServiceDiscovery.releaseServiceObject(serviceDiscovery, service));

                    closeDiscovery();

                    System.out.println("Discovery Closed!");
                } finally {
                    waitForKilled.complete();
                }
            });
        } else {
            System.out.println("Discovery is null...");
        }

        while (!waitForKilled.isComplete()) {}

        System.out.println("ServiceManager destroyed...");
    }

    @Fluent
    public ServiceManager publishApi(@Nonnull Record httpRecord) {
        return publishService(httpRecord, this::handlePublishResult);
    }

    @Fluent
    public ServiceManager publishApi(@Nonnull Record httpRecord,
                                     @Nonnull Handler<AsyncResult<Record>> resultHandler) {
        return publishService(httpRecord, resultHandler);
    }

    @Fluent
    public <T> ServiceManager publishService(@Nonnull Class<T> type, @Nonnull T service) {
        return publishService(createRecord(type, service), this::handlePublishResult);
    }

    @Fluent
    public <T> ServiceManager publishService(@Nonnull Class<T> type, @Nonnull T service,
                                             @Nonnull Handler<AsyncResult<Record>> resultHandler) {
        return publishService(createRecord(type, service), resultHandler);
    }

    @Fluent
    public ServiceManager consumeApi(@Nonnull String path,
                                     @Nonnull Handler<AsyncResult<HttpClient>> resultHandler) {
        return getApi(path, resultHandler);
    }

    @Fluent
    public <T> ServiceManager consumeService(@Nonnull Class<T> type, @Nonnull Handler<AsyncResult<T>> resultHandler) {
        return getService(type, resultHandler);
    }

    private void openDiscovery() {
        logger.debug("Opening Discovery...");

        if (serviceDiscovery == null) {
            serviceDiscovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions()
                    .setAnnounceAddress(NANNOQ_SERVICE_ANNOUNCE_ADDRESS)
                    .setUsageAddress(NANNOQ_SERVICE_ANNOUNCE_ADDRESS)
                    .setName(NANNOQ_SERVICE_SERVICE_NAME));

            logger.debug("Setting Discovery message consumer...");

            serviceAnnounceConsumer = vertx.eventBus()
                    .consumer(NANNOQ_SERVICE_ANNOUNCE_ADDRESS, this::handleServiceEvent);
        }

        logger.debug("Discovery ready...");
    }

    private void handleServiceEvent(Message<JsonObject> serviceEvent) {
        MultiMap headers = serviceEvent.headers();
        JsonObject body = serviceEvent.body();

        logger.trace("Service Event:\n" + Json.encodePrettily(serviceEvent) +
                "\nHeaders:\n" + Json.encodePrettily(headers) +
                "\nBody:\n" + Json.encodePrettily(body));

        String name = body.getString("name");
        String status = body.getString("status");

        if (status != null && status.equals("DOWN")) {
            logger.debug("Removing downed service: " + name);

            fetchedServices.remove(name);
        }
    }

    private void closeDiscovery() {
        if (serviceDiscovery != null) serviceDiscovery.close();
        serviceDiscovery = null;

        logger.debug("Unregistering Service Event Listener...");

        if (serviceAnnounceConsumer != null) serviceAnnounceConsumer.unregister();
        serviceAnnounceConsumer = null;
    }

    private ServiceManager getApi(String path, Handler<AsyncResult<HttpClient>> resultHandler) {
        logger.debug("Getting API: " + path);

        Object existingService = fetchedServices.get(path);

        if (existingService != null) {
            logger.debug("Returning fetched Api...");

            resultHandler.handle(Future.succeededFuture((HttpClient) existingService));
        } else {
            HttpEndpoint.getClient(serviceDiscovery, new JsonObject().put("name", path), ar -> {
                if (ar.failed()) {
                    logger.error("Unable to fetch API...");

                    resultHandler.handle(ServiceException.fail(404, "API not found..."));
                } else {
                    HttpClient client = ar.result();
                    fetchedServices.put(path, client);

                    resultHandler.handle(Future.succeededFuture(client));
                }
            });
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    private <T> ServiceManager getService(Class<T> type, Handler<AsyncResult<T>> resultHandler) {
        logger.debug("Getting service: " + type.getSimpleName());

        Object existingService = fetchedServices.get(type.getSimpleName());

        if (existingService != null) {
            logger.debug("Returning fetched service...");

            resultHandler.handle(Future.succeededFuture((T) existingService));
        } else {
            EventBusService.getProxy(serviceDiscovery, type, ar -> {
                if (ar.failed()) {
                    logger.error("ERROR: Unable to get service for " + type.getSimpleName());

                    resultHandler.handle(ServiceException.fail(NOT_FOUND,
                            "Unable to get service for " + type.getSimpleName() + " : " + ar.cause()));
                } else {
                    T service = ar.result();
                    fetchedServices.put(type.getSimpleName(), service);

                    logger.debug("Successful fetch of: " + service.getClass().getSimpleName());

                    resultHandler.handle(Future.succeededFuture(service));
                }
            });
        }

        return getInstance();
    }

    private <T> Record createRecord(Class<T> type, T service) {
        String serviceName = type.getSimpleName();

        registeredServices.add(new ServiceBinder(vertx)
                .setAddress(serviceName)
                .register(type, service));

        return EventBusService.createRecord(type.getSimpleName(), serviceName, type);
    }

    private ServiceManager publishService(@Nonnull Record record, @Nonnull Handler<AsyncResult<Record>> resultHandler) {
        serviceDiscovery.publish(record, ar -> {
            if (ar.failed()) {
                logger.error("ERROR: Failed publish of " +
                        record.getName() + " to " +
                        record.getLocation().encodePrettily() + " with " +
                        record.getType() + " : " +
                        record.getStatus());

                resultHandler.handle(ServiceException.fail(INTERNAL_ERROR, ar.cause().getMessage()));
            } else {
                Record publishedRecord = ar.result();
                registeredRecords.add(publishedRecord);

                logger.debug("Successful publish of: " +
                        publishedRecord.getName() + " to " +
                        publishedRecord.getLocation().encodePrettily() + " with " +
                        publishedRecord.getType() + " : " +
                        publishedRecord.getStatus());

                resultHandler.handle(Future.succeededFuture(publishedRecord));
            }
        });

        return getInstance();
    }

    private void handlePublishResult(AsyncResult<Record> publishResult) {
        if (publishResult.failed()) {
            if (publishResult.cause() instanceof ServiceException) {
                ServiceException serviceException = (ServiceException) publishResult.cause();

                logger.error("Unable to publish service: " +
                        serviceException.failureCode() + " : " +
                        serviceException.getMessage());
            } else {
                logger.error("Unable to publish service: " + publishResult.cause());
            }
        } else {
            Record record = publishResult.result();

            logger.debug("Published Service Record: " +
                    record.getName() + " : " +
                    record.getLocation() + " : " +
                    record.getType() + " : " +
                    record.getStatus());
        }
    }

    public static void handleResultFailed(Throwable t) {
        if (t instanceof ServiceException) {
            ServiceException serviceException = (ServiceException) t;

            logger.error(serviceException.failureCode() + " : " +
                    serviceException.getMessage(), t);
        } else {
            logger.error(t.getMessage(), t);
        }
    }
}
