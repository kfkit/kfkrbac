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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.connect.health.ConnectClusterState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.kfkit.kfkrbac.ConnectorOperation;
import dev.kfkit.kfkrbac.RestAccess;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

/**
 * Decides every request to the worker's REST interface from the role
 * bindings, and narrows the list of connectors to what the caller may see.
 *
 * <p>The order: the two requests workers send each other pass, as they do
 * the worker's own authentication filter, and the worker verifies them
 * itself; an anonymous request is refused with 401; the health check
 * passes; while the bindings have not been read, everything else is
 * refused with 503; a super user passes; every other request is the
 * operation on the connector the table in {@code docs/connect.md} gives,
 * and is refused with 403, saying which, when no role bound to the caller
 * grants it. A request the table does not name takes {@code ClusterAdmin}
 * or {@code SystemAdmin}.
 */
final class ConnectorFilter implements ContainerRequestFilter, ContainerResponseFilter {

    /** The request property the caller is kept in between the request and the response. */
    static final String CALLER = "dev.kfkit.kfkrbac.caller";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final RestAccess access;
    private final ConnectClusterState cluster;

    ConnectorFilter(RestAccess access, ConnectClusterState cluster) {
        this.access = Objects.requireNonNull(access, "access");
        this.cluster = Objects.requireNonNull(cluster, "cluster");
    }

    /** What a request takes. */
    private enum Kind {
        /** Sent by another worker, which the worker verifies itself. */
        INTERNAL,
        /** Any authenticated caller, and before the bindings are read. */
        HEALTH,
        /** Any authenticated caller. */
        ANYONE,
        /** Any authenticated caller; the answer is narrowed to what it may see. */
        LIST,
        /** The operation on the connector. */
        CONNECTOR,
        /** {@code Create} on the connector named in the body. */
        CREATE_FROM_BODY,
        /** {@code Create} when the connector does not exist, {@code Configure} when it does. */
        PUT_CONFIG,
        /** {@code ClusterAdmin} or {@code SystemAdmin}. */
        ADMIN
    }

    private record Route(Kind kind, ConnectorOperation operation, String connector) {

        static Route of(Kind kind) {
            return new Route(kind, null, null);
        }

        static Route on(ConnectorOperation operation, String connector) {
            return new Route(Kind.CONNECTOR, operation, connector);
        }
    }

    @Override
    public void filter(ContainerRequestContext request) throws IOException {
        List<String> path = path(request);
        Route route = route(request.getMethod(), path);
        if (route.kind() == Kind.INTERNAL) {
            return;
        }
        SecurityContext security = request.getSecurityContext();
        KafkaPrincipal caller = RestAccess.caller(
                security == null || security.getUserPrincipal() == null ? null : security.getUserPrincipal().getName(),
                request.getProperty(RestAccess.CERTIFICATE_ATTRIBUTE));
        if (caller == null) {
            abort(request, Response.Status.UNAUTHORIZED,
                    "kfkrbac: the request is anonymous; authenticate with HTTP basic authentication "
                            + "or a client certificate");
            return;
        }
        request.setProperty(CALLER, caller);
        if (route.kind() == Kind.HEALTH) {
            return;
        }
        if (!access.ready()) {
            abort(request, Response.Status.SERVICE_UNAVAILABLE,
                    "kfkrbac: the role bindings have not been read from the cluster yet; try again shortly");
            return;
        }
        if (access.superUser(caller)) {
            return;
        }
        switch (route.kind()) {
            case ANYONE:
            case LIST:
                return;
            case ADMIN:
                if (!access.administers(caller)) {
                    forbid(request, caller + " may not " + request.getMethod() + " /" + String.join("/", path)
                            + ": that takes ClusterAdmin or SystemAdmin");
                }
                return;
            case CREATE_FROM_BODY:
                String named = nameInBody(request);
                if (named == null) {
                    abort(request, Response.Status.BAD_REQUEST,
                            "kfkrbac: the request does not name the connector to create");
                    return;
                }
                decide(request, caller, ConnectorOperation.CREATE, named);
                return;
            case PUT_CONFIG:
                decide(request, caller, cluster.connectors().contains(route.connector())
                        ? ConnectorOperation.CONFIGURE
                        : ConnectorOperation.CREATE, route.connector());
                return;
            default:
                decide(request, caller, route.operation(), route.connector());
        }
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) {
        if (!(request.getProperty(CALLER) instanceof KafkaPrincipal caller) || response.getStatus() != 200
                || route(request.getMethod(), path(request)).kind() != Kind.LIST || access.superUser(caller)) {
            return;
        }
        response.setEntity(visible(caller, response.getEntity()));
    }

    /**
     * The connectors of a listing the caller may {@code ReadStatus}; with
     * {@code ?expand=info}, a connector's configuration only where it may
     * {@code ReadConfig}. A listing of a shape this does not know shows nothing.
     */
    private Object visible(KafkaPrincipal caller, Object listing) {
        if (listing instanceof Collection<?> names) {
            List<Object> shown = new ArrayList<>();
            for (Object name : names) {
                if (name instanceof String connector
                        && access.grants(caller, ConnectorOperation.READ_STATUS, connector)) {
                    shown.add(connector);
                }
            }
            return shown;
        }
        Map<Object, Object> shown = new LinkedHashMap<>();
        if (listing instanceof Map<?, ?> expanded) {
            expanded.forEach((name, details) -> {
                if (!(name instanceof String connector)
                        || !access.grants(caller, ConnectorOperation.READ_STATUS, connector)) {
                    return;
                }
                if (details instanceof Map<?, ?> parts
                        && !access.grants(caller, ConnectorOperation.READ_CONFIG, connector)) {
                    Map<Object, Object> kept = new LinkedHashMap<>(parts);
                    kept.remove("info");
                    shown.put(connector, kept);
                } else if (details instanceof Map<?, ?>) {
                    shown.put(connector, details);
                }
            });
        }
        return shown;
    }

    private void decide(ContainerRequestContext request, KafkaPrincipal caller, ConnectorOperation operation,
            String connector) {
        if (!access.grants(caller, operation, connector)) {
            forbid(request, caller + " may not " + RestAccess.name(operation) + " connector " + connector
                    + ": no role bound to it grants " + RestAccess.name(operation) + " on that connector");
        }
    }

    /** What the request is, by its method and path; the table in {@code docs/connect.md}. */
    private static Route route(String method, List<String> path) {
        String verb = "HEAD".equals(method) ? "GET" : method;
        int length = path.size();
        if (length == 0) {
            return Route.of("GET".equals(verb) ? Kind.ANYONE : Kind.ADMIN);
        }
        switch (path.get(0)) {
            case "health":
                return Route.of(length == 1 && "GET".equals(verb) ? Kind.HEALTH : Kind.ADMIN);
            case "connector-plugins":
                return Route.of(length == 1 && "GET".equals(verb) ? Kind.ANYONE : Kind.ADMIN);
            case "connectors":
                return connectors(verb, path);
            default:
                return Route.of(Kind.ADMIN);
        }
    }

    private static Route connectors(String verb, List<String> path) {
        int length = path.size();
        if (length == 1) {
            return Route.of("GET".equals(verb) ? Kind.LIST : "POST".equals(verb) ? Kind.CREATE_FROM_BODY : Kind.ADMIN);
        }
        String connector = path.get(1);
        if (length == 2) {
            switch (verb) {
                case "GET":
                    return Route.on(ConnectorOperation.READ_CONFIG, connector);
                case "DELETE":
                    return Route.on(ConnectorOperation.DELETE, connector);
                default:
                    return Route.of(Kind.ADMIN);
            }
        }
        String what = verb + " " + String.join("/", path.subList(2, length));
        if (length == 5 && "tasks".equals(path.get(2))) {
            what = verb + " tasks/{id}/" + path.get(4);
        }
        switch (what) {
            case "POST tasks":
            case "PUT fence":
                return Route.of(Kind.INTERNAL);
            case "GET config":
            case "GET tasks":
                return Route.on(ConnectorOperation.READ_CONFIG, connector);
            case "PUT config":
                return new Route(Kind.PUT_CONFIG, null, connector);
            case "PATCH config":
            case "PATCH offsets":
            case "DELETE offsets":
                return Route.on(ConnectorOperation.CONFIGURE, connector);
            case "GET status":
            case "GET tasks/{id}/status":
            case "GET offsets":
                return Route.on(ConnectorOperation.READ_STATUS, connector);
            case "GET topics":
                return Route.on(ConnectorOperation.READ_ACTIVE_TOPICS, connector);
            case "PUT topics/reset":
                return Route.on(ConnectorOperation.RESET_ACTIVE_TOPICS, connector);
            case "PUT pause":
            case "PUT stop":
                return Route.on(ConnectorOperation.PAUSE, connector);
            case "PUT resume":
            case "POST restart":
            case "POST tasks/{id}/restart":
                return Route.on(ConnectorOperation.RESUME, connector);
            default:
                return Route.of(Kind.ADMIN);
        }
    }

    /** The decoded segments of the path, without the empty one a trailing slash leaves. */
    private static List<String> path(ContainerRequestContext request) {
        List<String> path = new ArrayList<>();
        for (PathSegment segment : request.getUriInfo().getPathSegments(true)) {
            path.add(segment.getPath());
        }
        while (!path.isEmpty() && path.get(path.size() - 1).isEmpty()) {
            path.remove(path.size() - 1);
        }
        return path;
    }

    /**
     * The name of the connector to create, from the JSON body, trimmed as the
     * worker trims it; the body is put back for the worker to read.
     * {@code null} when the body names none.
     */
    private static String nameInBody(ContainerRequestContext request) throws IOException {
        byte[] body = request.getEntityStream().readAllBytes();
        request.setEntityStream(new ByteArrayInputStream(body));
        try {
            JsonNode name = JSON.readTree(body).path("name");
            if (!name.isTextual() || name.asText().trim().isEmpty()) {
                return null;
            }
            return name.asText().trim();
        } catch (IOException e) {
            return null;
        }
    }

    private static void forbid(ContainerRequestContext request, String reason) {
        abort(request, Response.Status.FORBIDDEN, "kfkrbac: " + reason);
    }

    private static void abort(ContainerRequestContext request, Response.Status status, String message) {
        request.abortWith(Response.status(status)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity("{\"error_code\":" + status.getStatusCode() + ",\"message\":" + quoted(message) + "}")
                .build());
    }

    private static String quoted(String text) {
        try {
            return JSON.writeValueAsString(text);
        } catch (IOException e) {
            return "\"\"";
        }
    }
}
