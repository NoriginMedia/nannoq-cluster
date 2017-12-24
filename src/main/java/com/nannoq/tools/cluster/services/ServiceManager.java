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

package com.nannoq.tools.cluster.services;

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
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.serviceproxy.ServiceException;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * This class defines a wrapper for publishing and consuming service declaration interfaces, and HTTP records.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@SuppressWarnings("UnusedReturnValue")
public class ServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(ServiceManager.class.getSimpleName());

    private static final String NANNOQ_SERVICE_ANNOUNCE_ADDRESS = "com.nannoq.services.manager.announce";
    private static final String NANNOQ_SERVICE_SERVICE_NAME = "nannoq-service-manager-service-discovery";

    private static final int NOT_FOUND = 404;
    private static final int INTERNAL_ERROR = 500;

    private ServiceDiscovery serviceDiscovery;
    private ConcurrentHashMap<String, MessageConsumer<JsonObject>> registeredServices = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Record> registeredRecords = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentHashSet<Object>> fetchedServices = new ConcurrentHashMap<>();

    private Vertx vertx;
    private static ServiceManager instance = null;
    private MessageConsumer<JsonObject> serviceAnnounceConsumer;

    private ServiceManager() {
        this(Vertx.currentContext().owner());
    }

    private ServiceManager(Vertx vertx) {
        this.vertx = vertx;
        openDiscovery();
        startServiceManagerKillVerticle();
    }

    private void startServiceManagerKillVerticle() {
        vertx.deployVerticle(new KillVerticle());
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

    private class KillVerticle extends AbstractVerticle {
        @Override
        public void stop(Future<Void> stopFuture) throws Exception {
            logger.info("Destroying ServiceManager");

            if (serviceDiscovery != null) {
                logger.info("Unpublishing all records...");

                List<Future> unPublishFutures = new ArrayList<>();

                registeredRecords.forEach((k, v) -> {
                    Future<Boolean> unpublish = Future.future();

                    serviceDiscovery.unpublish(v.getRegistration(), unpublishResult -> {
                        if (unpublishResult.failed()) {
                            logger.info("Failed Unpublish: " + v.getName(), unpublishResult.cause());

                            unpublish.fail(unpublishResult.cause());
                        } else {
                            logger.info("Unpublished: " + v.getName());

                            unpublish.complete();
                        }
                    });

                    unPublishFutures.add(unpublish);
                });

                CompositeFuture.all(unPublishFutures).setHandler(res -> {
                    try {
                        registeredRecords.clear();

                        logger.info("UnPublish complete, Unregistering all services...");

                        registeredServices.forEach((k, v) -> {
                            new ServiceBinder(vertx).setAddress(v.address()).unregister(v);

                            logger.info("Unregistering " + v.address());
                        });

                        registeredServices.clear();

                        logger.info("Releasing all consumed service objects...");

                        fetchedServices.values().forEach(service ->
                                ServiceDiscovery.releaseServiceObject(serviceDiscovery, service));

                        fetchedServices.clear();

                        closeDiscovery(unRegisterRes -> {
                            serviceAnnounceConsumer = null;

                            logger.info("Discovery Closed!");

                            instance = null;
                            stopFuture.tryComplete();

                            logger.info("ServiceManager destroyed...");
                        });
                    } finally {
                        instance = null;
                        stopFuture.tryComplete();

                        logger.info("ServiceManager destroyed...");
                    }
                });
            } else {
                logger.info("Discovery is null...");

                instance = null;
                stopFuture.tryComplete();
            }
        }
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

    private void closeDiscovery(Handler<AsyncResult<Void>> resultHandler) {
        if (serviceDiscovery != null) serviceDiscovery.close();
        serviceDiscovery = null;

        logger.debug("Unregistering Service Event Listener...");

        if (serviceAnnounceConsumer != null) serviceAnnounceConsumer.unregister(resultHandler);
    }

    @Fluent
    public ServiceManager publishApi(@Nonnull Record httpRecord) {
        return publishService(httpRecord, r -> registeredRecords.put(r.getRegistration(), r),this::handlePublishResult);
    }

    @Fluent
    public ServiceManager publishApi(@Nonnull Record httpRecord,
                                     @Nonnull Handler<AsyncResult<Record>> resultHandler) {
        return publishService(httpRecord, r -> registeredRecords.put(r.getRegistration(), r), resultHandler);
    }

    @Fluent
    public ServiceManager unPublishApi(@Nonnull Record service, @Nonnull Handler<AsyncResult<Void>> resultHandler) {
        registeredRecords.remove(service.getRegistration());
        serviceDiscovery.unpublish(service.getRegistration(), resultHandler);
        final ConcurrentHashSet<Object> objects = fetchedServices.get(service.getName());
        if (objects != null && objects.size() > 0) {
            final Iterator<Object> iterator = objects.iterator();
            iterator.next();
            iterator.remove();
        }

        return this;
    }

    @Fluent
    public <T> ServiceManager publishService(@Nonnull Class<T> type, @Nonnull T service) {
        String serviceName = type.getSimpleName();

        return publishService(createRecord(serviceName, type), r -> registeredServices.put(r.getRegistration(), new ServiceBinder(vertx)
                .setAddress(serviceName)
                .register(type, service)), this::handlePublishResult);
    }

    @Fluent
    public <T> ServiceManager publishService(@Nonnull Class<T> type, @Nonnull String customName, @Nonnull T service) {
        return publishService(createRecord(customName, type), r -> registeredServices.put(r.getRegistration(), new ServiceBinder(vertx)
                .setAddress(customName)
                .register(type, service)), this::handlePublishResult);
    }

    @Fluent
    public <T> ServiceManager publishService(@Nonnull Class<T> type, @Nonnull T service,
                                             @Nonnull Handler<AsyncResult<Record>> resultHandler) {
        return publishService(createRecord(type), r -> registeredServices.put(r.getRegistration(), new ServiceBinder(vertx)
                .setAddress(type.getSimpleName())
                .register(type, service)),resultHandler);
    }

    @Fluent
    public <T> ServiceManager publishService(@Nonnull Class<T> type, @Nonnull String customName, @Nonnull T service,
                                             @Nonnull Handler<AsyncResult<Record>> resultHandler) {
        return publishService(createRecord(customName, type), r -> registeredServices.put(r.getRegistration(), new ServiceBinder(vertx)
                .setAddress(customName)
                .register(type, service)), resultHandler);
    }

    @Fluent
    public <T> ServiceManager unPublishService(@Nonnull Class<T> type, @Nonnull Record service) {
        String serviceName = type.getSimpleName();

        return unPublishService(serviceName, service);
    }

    @Fluent
    public ServiceManager unPublishService(@Nonnull String serviceName, @Nonnull Record service) {
        return unPublishService(serviceName, service, res -> logger.info("Unpublish res: " + res.succeeded()));
    }

    @Fluent
    public <T> ServiceManager unPublishService(@Nonnull Class<T> type, @Nonnull Record service,
                                               @Nonnull Handler<AsyncResult<Void>> resultHandler) {
        String serviceName = type.getSimpleName();

        return unPublishService(serviceName, service, resultHandler);
    }

    @Fluent
    public ServiceManager unPublishService(@Nonnull String serviceName, @Nonnull Record service,
                                           @Nonnull Handler<AsyncResult<Void>> resultHandler) {
        new ServiceBinder(vertx)
                .setAddress(serviceName)
                .unregister(registeredServices.get(service.getRegistration()));

        serviceDiscovery.unpublish(service.getRegistration(), resultHandler);

        registeredServices.remove(service.getRegistration());

        final ConcurrentHashSet<Object> objects = fetchedServices.get(service.getName());
        if (objects != null && objects.size() > 0) {
            final Iterator<Object> iterator = objects.iterator();
            iterator.next();
            iterator.remove();
        }

        return this;
    }

    @Fluent
    public ServiceManager consumeApi(@Nonnull String name,
                                     @Nonnull Handler<AsyncResult<HttpClient>> resultHandler) {
        return getApi(name, resultHandler);
    }

    @Fluent
    public <T> ServiceManager consumeService(@Nonnull Class<T> type, @Nonnull Handler<AsyncResult<T>> resultHandler) {
        return consumeService(type, type.getSimpleName(), resultHandler);
    }

    @Fluent
    public <T> ServiceManager consumeService(@Nonnull Class<T> type, @Nonnull String customName,
                                             @Nonnull Handler<AsyncResult<T>> resultHandler) {
        return getService(type, customName, resultHandler);
    }

    private ServiceManager getApi(String name, Handler<AsyncResult<HttpClient>> resultHandler) {
        logger.debug("Getting API: " + name);

        ConcurrentHashSet<Object> existingServices = fetchedServices.get(name);

        if (existingServices != null && existingServices.size() > 0) {
            logger.debug("Returning fetched Api...");

            final ArrayList<Object> objects = new ArrayList<>(existingServices);
            Collections.shuffle(objects);
            resultHandler.handle(Future.succeededFuture((HttpClient) objects.get(0)));
        } else {
            HttpEndpoint.getClient(serviceDiscovery, new JsonObject().put("name", name), ar -> {
                if (ar.failed()) {
                    logger.error("Unable to fetch API...");

                    resultHandler.handle(ServiceException.fail(404, "API not found..."));
                } else {
                    HttpClient client = ar.result();
                    ConcurrentHashSet<Object> objects = fetchedServices.get(name);

                    if (objects == null) {
                        fetchedServices.put(name, new ConcurrentHashSet<>());
                        objects = fetchedServices.get(name);
                    }

                    if (!objects.contains(client)) {
                        objects.add(client);
                    }

                    fetchedServices.put(name, objects);

                    resultHandler.handle(Future.succeededFuture(client));
                }
            });
        }

        return this;
    }

    private <T> ServiceManager getService(Class<T> type, Handler<AsyncResult<T>> resultHandler) {
        return getService(type, type.getSimpleName(), resultHandler);
    }

    @SuppressWarnings("unchecked")
    private <T> ServiceManager getService(Class<T> type, String serviceName, Handler<AsyncResult<T>> resultHandler) {
        logger.debug("Getting service: " + serviceName);

        ConcurrentHashSet<Object> existingServices = fetchedServices.get(serviceName);

        if (existingServices != null && existingServices.size() > 0) {
            logger.debug("Returning fetched Api...");

            final ArrayList<Object> objects = new ArrayList<>(existingServices);
            Collections.shuffle(objects);
            resultHandler.handle(Future.succeededFuture((T) objects.get(0)));
        } else {
            EventBusService.getProxy(serviceDiscovery, type, ar -> {
                if (ar.failed()) {
                    logger.error("ERROR: Unable to get service for " + serviceName);

                    resultHandler.handle(ServiceException.fail(NOT_FOUND,
                            "Unable to get service for " + serviceName + " : " + ar.cause()));
                } else {
                    T service = ar.result();
                    ConcurrentHashSet<Object> objects = fetchedServices.get(serviceName);

                    if (objects == null) {
                        fetchedServices.put(serviceName, new ConcurrentHashSet<>());
                        objects = fetchedServices.get(serviceName);
                    }

                    if (!objects.contains(service)) {
                        objects.add(service);
                    }

                    fetchedServices.put(serviceName, objects);

                    logger.debug("Successful fetch of: " + service.getClass().getSimpleName());

                    resultHandler.handle(Future.succeededFuture(service));
                }
            });
        }

        return getInstance();
    }

    private <T> Record createRecord(Class<T> type) {
        return createRecord(type.getSimpleName(), type);
    }

    private <T> Record createRecord(String serviceName, Class<T> type) {
        return EventBusService.createRecord(serviceName, serviceName, type);
    }

    private ServiceManager publishService(@Nonnull Record record, @Nonnull Consumer<Record> recordLogic,
                                          @Nonnull Handler<AsyncResult<Record>> resultHandler) {
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
                registeredRecords.put(publishedRecord.getRegistration(), publishedRecord);
                recordLogic.accept(publishedRecord);

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
