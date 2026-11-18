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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import dev.kfkit.kfkrbac.RestAccess;

/**
 * Role bindings in Kafka Connect: a request to the worker's REST interface
 * is allowed when a role bound to the caller covers the connector, and
 * refused otherwise. One line in the worker's settings:
 *
 * <pre>rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension</pre>
 *
 * <p>It reads the bindings from the Kafka cluster the worker uses, with the
 * worker's own connection settings; what is decided for which request is
 * in {@code docs/connect.md}.
 */
public final class RbacConnectRestExtension implements ConnectRestExtension {

    /**
     * Where the filter runs among the worker's own: after its basic
     * authentication filter, which runs at the default priority, 5000, in
     * some releases whatever it declares.
     */
    static final int PRIORITY = 6000;

    private Map<String, ?> settings = Map.of();
    private RestAccess access;

    /** Created by the worker. */
    public RbacConnectRestExtension() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        settings = Collections.unmodifiableMap(new HashMap<>(configs));
        // Fail at start-up, not at the first request, when a setting of ours cannot be read.
        RestAccess.validate(settings);
    }

    @Override
    public void register(ConnectRestExtensionContext context) {
        access = RestAccess.start(settings, "connect");
        context.configurable().register(new ConnectorFilter(access, context.clusterState()), PRIORITY);
    }

    @Override
    public void close() {
        if (access != null) {
            access.close();
        }
    }

    @Override
    public String version() {
        String version = RbacConnectRestExtension.class.getPackage().getImplementationVersion();
        return version == null ? "unknown" : version;
    }
}
