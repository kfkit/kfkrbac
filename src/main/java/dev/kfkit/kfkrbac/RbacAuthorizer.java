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
package dev.kfkit.kfkrbac;

import java.util.List;

import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * The broker's {@link StandardAuthorizer} with roles.
 *
 * <p>A role names a set of permissions once; a role binding hands it to a
 * user or a group. Roles only ever add access: what an ACL allows stays
 * allowed, what an ACL denies stays denied, and a request no ACL allows is
 * allowed when a role bound to its principal covers it.
 *
 * <p>Extending rather than wrapping is deliberate. The broker feeds ACLs
 * only to authorizers that are a {@code ClusterMetadataAuthorizer}; a plain
 * wrapper would silently never receive them.
 *
 * <p>Configure with {@code authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer}.
 *
 * <p>This is the skeleton: roles do not exist yet and the verdict is the
 * parent's.
 */
public class RbacAuthorizer extends StandardAuthorizer {

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        // Roles are consulted here once they exist; until then the ACLs decide alone.
        return super.authorize(requestContext, actions);
    }
}
