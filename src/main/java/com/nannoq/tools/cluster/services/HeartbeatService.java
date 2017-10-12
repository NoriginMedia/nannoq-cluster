package com.nannoq.tools.cluster.services;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

@ProxyGen
@VertxGen
public interface HeartbeatService {
    @Fluent
    HeartbeatService ping(Handler<AsyncResult<Boolean>> resultHandler);
}
