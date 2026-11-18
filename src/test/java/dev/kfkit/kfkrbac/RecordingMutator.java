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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.metadata.authorizer.AclMutator;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;

/**
 * The controller as an authorizer sees it, for tests: it records what it
 * was asked to create and delete, does it, and delivers the change to the
 * authorizer the way the metadata log would.
 */
final class RecordingMutator implements AclMutator {

    /** Every record it was asked to create, in order. */
    final List<AclBinding> created = new ArrayList<>();

    /** Every filter it was asked to delete by, in order. */
    final List<AclBindingFilter> deleted = new ArrayList<>();

    /** On whose behalf each request came. */
    final List<KafkaPrincipal> callers = new ArrayList<>();

    private final Map<Uuid, StandardAcl> records = new LinkedHashMap<>();
    private final ClusterMetadataAuthorizer authorizer;

    RecordingMutator(ClusterMetadataAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    /** These are now all the ACL records of the cluster. */
    void load(StandardAcl... acls) {
        records.clear();
        for (StandardAcl acl : acls) {
            records.put(Uuid.randomUuid(), acl);
        }
        authorizer.loadSnapshot(new HashMap<>(records));
    }

    /** The records of the cluster as a client would list them. */
    List<AclBinding> records() {
        List<AclBinding> all = new ArrayList<>();
        records.values().forEach(acl -> all.add(acl.toBinding()));
        return all;
    }

    @Override
    public CompletableFuture<List<AclCreateResult>> createAcls(ControllerRequestContext context,
            List<AclBinding> aclBindings) {
        callers.add(context.principal());
        List<AclCreateResult> results = new ArrayList<>();
        for (AclBinding binding : aclBindings) {
            created.add(binding);
            Uuid id = Uuid.randomUuid();
            StandardAcl acl = StandardAcl.fromAclBinding(binding);
            records.put(id, acl);
            authorizer.addAcl(id, acl);
            results.add(AclCreateResult.SUCCESS);
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    public CompletableFuture<List<AclDeleteResult>> deleteAcls(ControllerRequestContext context,
            List<AclBindingFilter> aclBindingFilters) {
        callers.add(context.principal());
        List<AclDeleteResult> results = new ArrayList<>();
        for (AclBindingFilter filter : aclBindingFilters) {
            deleted.add(filter);
            List<AclBindingDeleteResult> gone = new ArrayList<>();
            for (Iterator<Map.Entry<Uuid, StandardAcl>> all = records.entrySet().iterator(); all.hasNext();) {
                Map.Entry<Uuid, StandardAcl> record = all.next();
                AclBinding binding = record.getValue().toBinding();
                if (filter.matches(binding)) {
                    all.remove();
                    authorizer.removeAcl(record.getKey());
                    gone.add(new AclBindingDeleteResult(binding));
                }
            }
            results.add(new AclDeleteResult(gone));
        }
        return CompletableFuture.completedFuture(results);
    }
}
