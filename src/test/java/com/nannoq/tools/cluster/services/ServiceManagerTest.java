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
 */

package com.nannoq.tools.cluster.services;

import com.nannoq.tools.cluster.apis.APIHostProducer;
import com.nannoq.tools.cluster.apis.APIManager;
import com.nannoq.tools.cluster.service.HeartBeatServiceImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.stream.IntStream;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@RunWith(VertxUnitRunner.class)
public class ServiceManagerTest {
    private static final Logger logger = LogManager.getLogger(ServiceManagerTest.class.getSimpleName());

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Rule
    public TestName name = new TestName();

    @Test
    public void publishApi(TestContext testContext) throws Exception {
        ServiceManager.getInstance().publishApi(getApiManager().createExternalApiRecord("SOME_API", "/api"));
        ServiceManager.getInstance().consumeApi("SOME_API", testContext.asyncAssertSuccess());
    }

    @Test
    public void unPublishApi(TestContext testContext) throws Exception {
        ServiceManager.getInstance().publishApi(getApiManager().createExternalApiRecord("SOME_API", "/api"));
        ServiceManager.getInstance().consumeApi("SOME_API", testContext.asyncAssertSuccess());
        ServiceManager.getInstance().unPublishApi("SOME_API", testContext.asyncAssertSuccess());
        ServiceManager.getInstance().consumeApi("SOME_API", testContext.asyncAssertFailure());
    }

    @Test
    public void consumeApi(TestContext testContext) throws Exception {
        ServiceManager.getInstance().publishApi(getApiManager().createExternalApiRecord("SOME_API", "/api"));

        IntStream.range(0, 100).parallel().forEach(i -> {
            Async async = testContext.async();

            ServiceManager.getInstance().consumeApi("SOME_API", apiRes -> {
                if (apiRes.failed()) {
                    testContext.fail(apiRes.cause());
                } else {
                    async.complete();
                }
            });
        });
    }

    @Test
    public void publishService(TestContext testContext) throws Exception {
        ServiceManager.getInstance().publishService(HeartbeatService.class, new HeartBeatServiceImpl());
        ServiceManager.getInstance().consumeService(HeartbeatService.class, testContext.asyncAssertSuccess());
    }

    @Test
    public void unPublishService(TestContext testContext) throws Exception {
        ServiceManager.getInstance().publishService(HeartbeatService.class, new HeartBeatServiceImpl());
        ServiceManager.getInstance().consumeService(HeartbeatService.class, testContext.asyncAssertSuccess());
        ServiceManager.getInstance().unPublishService(HeartbeatService.class, testContext.asyncAssertSuccess());
        ServiceManager.getInstance().consumeService(HeartbeatService.class, testContext.asyncAssertFailure());
    }

    @Test
    public void consumeService(TestContext testContext) throws Exception {
        ServiceManager.getInstance().publishService(HeartbeatService.class, new HeartBeatServiceImpl());

        IntStream.range(0, 100).parallel().forEach(i -> {
            Async async = testContext.async();

            ServiceManager.getInstance().consumeService(HeartbeatService.class, res -> {
                if (res.failed()) {
                    testContext.fail(res.cause());
                } else {
                    res.result().ping(pingRes -> {
                        if (pingRes.failed()) {
                            testContext.fail(pingRes.cause());
                        } else {
                            async.complete();
                        }
                    });
                }
            });
        });
    }

    public APIManager getApiManager() {
        return new APIManager(rule.vertx(), new JsonObject()
                .put("publicHost", "localhost")
                .put("privateHost", "localhost"),
                new APIHostProducer() {
                    @Override
                    public String getInternalHost(String name) {
                        return "localhost";
                    }

                    @Override
                    public String getExternalHost(String name) {
                        return "localhost";
                    }
                });
    }
}