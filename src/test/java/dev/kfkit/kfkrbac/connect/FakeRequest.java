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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.health.ConnectClusterState;

import dev.kfkit.kfkrbac.RestAccess;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;

/**
 * A request to the worker's REST interface as the filter sees it, and its
 * response: what the worker's JAX-RS hands a filter, and nothing more.
 */
final class FakeRequest {

    final String method;
    final String path;
    private final Map<String, Object> properties = new HashMap<>();
    private String user;
    private InputStream entity = InputStream.nullInputStream();
    Response aborted;

    private FakeRequest(String method, String path) {
        this.method = method;
        this.path = path;
    }

    /** An anonymous request: {@code GET /connectors/billing-sink/status}. */
    static FakeRequest of(String method, String path) {
        return new FakeRequest(method, path);
    }

    /** Authenticated with HTTP basic authentication, as the worker's filter leaves it. */
    FakeRequest as(String user) {
        this.user = user;
        return this;
    }

    /** Over mutual TLS, with the client's certificate chain where the servlet container puts it. */
    FakeRequest withCertificates(Object chain) {
        properties.put(RestAccess.CERTIFICATE_ATTRIBUTE, chain);
        return this;
    }

    FakeRequest withBody(String body) {
        entity = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    /** What the worker reads of the body after the filter. */
    String body() {
        try {
            return new String(entity.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The status the filter aborted with, or 0 when it let the request through. */
    int status() {
        return aborted == null ? 0 : aborted.getStatus();
    }

    /** The body of the response the filter aborted with. */
    String message() {
        return aborted == null ? "" : String.valueOf(aborted.getEntity());
    }

    ContainerRequestContext context() {
        UriInfo uri = proxy(UriInfo.class, (name, args) -> {
            if (name.equals("getPathSegments")) {
                List<PathSegment> segments = new ArrayList<>();
                for (String segment : path.substring(1).split("/", -1)) {
                    segments.add(proxy(PathSegment.class, (n, a) -> {
                        if (n.equals("getPath")) {
                            return segment;
                        }
                        throw new UnsupportedOperationException(n);
                    }));
                }
                return segments;
            }
            throw new UnsupportedOperationException(name);
        });
        SecurityContext security = proxy(SecurityContext.class, (name, args) -> {
            if (name.equals("getUserPrincipal")) {
                return user == null ? null : (Principal) () -> user;
            }
            throw new UnsupportedOperationException(name);
        });
        return proxy(ContainerRequestContext.class, (name, args) -> {
            switch (name) {
                case "getMethod":
                    return method;
                case "getUriInfo":
                    return uri;
                case "getSecurityContext":
                    return security;
                case "getProperty":
                    return properties.get((String) args[0]);
                case "setProperty":
                    properties.put((String) args[0], args[1]);
                    return null;
                case "getEntityStream":
                    return entity;
                case "setEntityStream":
                    entity = (InputStream) args[0];
                    return null;
                case "abortWith":
                    aborted = (Response) args[0];
                    return null;
                default:
                    throw new UnsupportedOperationException(name);
            }
        });
    }

    /** A response of status 200 carrying the entity, as a resource method returns it. */
    static final class FakeResponse {

        Object entity;

        FakeResponse(Object entity) {
            this.entity = entity;
        }

        ContainerResponseContext context() {
            return proxy(ContainerResponseContext.class, (name, args) -> {
                switch (name) {
                    case "getStatus":
                        return 200;
                    case "getEntity":
                        return entity;
                    case "setEntity":
                        entity = args[0];
                        return null;
                    default:
                        throw new UnsupportedOperationException(name);
                }
            });
        }
    }

    /** The connectors that exist on the Connect cluster. */
    static ConnectClusterState connectors(String... names) {
        return proxy(ConnectClusterState.class, (name, args) -> {
            if (name.equals("connectors")) {
                return Set.of(names);
            }
            throw new UnsupportedOperationException(name);
        });
    }

    interface Handler {
        Object handle(String method, Object[] args);
    }

    @SuppressWarnings("unchecked")
    static <T> T proxy(Class<T> type, Handler handler) {
        return (T) Proxy.newProxyInstance(FakeRequest.class.getClassLoader(), new Class<?>[] {type},
                (self, method, args) -> {
                    switch (method.getName()) {
                        case "toString":
                            return type.getSimpleName();
                        case "hashCode":
                            return System.identityHashCode(self);
                        case "equals":
                            return self == args[0];
                        default:
                            return handler.handle(method.getName(), args);
                    }
                });
    }
}
