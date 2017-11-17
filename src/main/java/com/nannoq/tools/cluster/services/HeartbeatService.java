package com.nannoq.tools.cluster.services;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * This class defines a service declaration for the HeartbeatService. It responds to pings, to enable clients to ensure
 * connection to eventbus services.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@ProxyGen
@VertxGen
public interface HeartbeatService {
    @Fluent
    HeartbeatService ping(Handler<AsyncResult<Boolean>> resultHandler);
}
