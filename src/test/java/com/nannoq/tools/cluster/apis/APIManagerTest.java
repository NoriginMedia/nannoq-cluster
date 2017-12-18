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

package com.nannoq.tools.cluster.apis;

import com.nannoq.tools.cluster.services.ServiceManager;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.servicediscovery.Record;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
@RunWith(VertxUnitRunner.class)
public class APIManagerTest {
    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Test
    public void performRequestWithCircuitBreaker(TestContext testContext) {
        ServiceManager.getInstance().publishApi(getApiManager("www.google.com").createExternalApiRecord("TEST", "/"));
        Async async = testContext.async();

        ServiceManager.getInstance().consumeApi("TEST", res -> {
            testContext.assertTrue(res.succeeded());

            res.result().get("/").handler(resRes -> {
                testContext.assertTrue(resRes.statusCode() == 302);
                async.complete();
            }).end();
        });
    }

    @Test
    public void createInternalApiRecord(TestContext testContext) {
        final Record record = getApiManager().createInternalApiRecord("TEST_API", "/api");

        testContext.assertEquals("TEST_API", record.getName());
    }

    @Test
    public void createExternalApiRecord(TestContext testContext) {
        final Record record = getApiManager().createExternalApiRecord("TEST_API", "/api");

        testContext.assertEquals("TEST_API", record.getName());
    }

    public APIManager getApiManager() {
        return getApiManager("localhost");
    }

    public APIManager getApiManager(String host) {
        return new APIManager(rule.vertx(), new JsonObject()
                .put("publicHost", host)
                .put("privateHost", host),
                new APIHostProducer() {
                    @Override
                    public String getInternalHost(String name) {
                        return host;
                    }

                    @Override
                    public String getExternalHost(String name) {
                        return host;
                    }
                });
    }
}