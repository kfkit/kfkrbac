/*
 * Copyright 2026 Ivan Abramov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.kfkit.kfkrbac.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.junit.jupiter.api.Test;

import jakarta.ws.rs.core.Configurable;

/** The extension as the worker finds, configures and registers it. */
class RbacConnectRestExtensionTest {

    /** AC1, in part: the worker finds the extension in the jar by its class name, and as a service. */
    @Test
    void theWorkerFindsItAsAService() {
        List<Class<?>> found = new ArrayList<>();
        ServiceLoader.load(ConnectRestExtension.class).stream().forEach(provider -> found.add(provider.type()));
        assertTrue(found.contains(RbacConnectRestExtension.class), found.toString());
    }

    @Test
    void itRegistersItsFilterAfterTheWorkersAuthentication() {
        List<Object> registered = new ArrayList<>();
        List<Integer> priorities = new ArrayList<>();
        Configurable<?> configurable = FakeRequest.proxy(Configurable.class, (name, args) -> {
            if (name.equals("register") && args.length == 2 && args[1] instanceof Integer priority) {
                registered.add(args[0]);
                priorities.add(priority);
                return null;
            }
            throw new UnsupportedOperationException(name);
        });
        ConnectClusterState cluster = FakeRequest.connectors();
        ConnectRestExtensionContext context = FakeRequest.proxy(ConnectRestExtensionContext.class, (name, args) -> {
            switch (name) {
                case "configurable":
                    return configurable;
                case "clusterState":
                    return cluster;
                default:
                    throw new UnsupportedOperationException(name);
            }
        });
        try (RbacConnectRestExtension extension = new RbacConnectRestExtension()) {
            // The admin client connects lazily: nothing listens there, and nothing has to.
            extension.configure(Map.of("bootstrap.servers", "localhost:1", "kfkrbac.refresh.interval.ms", "60000",
                    "rest.extension.classes", RbacConnectRestExtension.class.getName()));
            extension.register(context);
            assertEquals(1, registered.size());
            assertInstanceOf(ConnectorFilter.class, registered.get(0));
            assertTrue(priorities.get(0) > 5000, "after the worker's basic authentication filter, at 5000");
        }
    }

    @Test
    void aSettingThatCannotBeReadStopsTheWorkerAtStartUp() {
        try (RbacConnectRestExtension extension = new RbacConnectRestExtension()) {
            assertThrows(ConfigException.class,
                    () -> extension.configure(Map.of("kfkrbac.super.users", "admin")));
        }
    }
}
