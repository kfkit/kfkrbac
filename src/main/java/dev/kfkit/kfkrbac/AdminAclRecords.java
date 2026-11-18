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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

/**
 * The ACL records of a cluster through its admin client: {@code describeAcls},
 * {@code createAcls} and {@code deleteAcls}, which is all the ACL tool needs
 * too. The cluster decides what the caller may do; its refusal comes back as
 * a {@link Failure} with the reason it gave.
 */
final class AdminAclRecords implements AclRecords {

    private final Admin admin;

    AdminAclRecords(Admin admin) {
        this.admin = Objects.requireNonNull(admin, "admin");
    }

    @Override
    public Collection<AclBinding> describe(AclBindingFilter filter) {
        return get(admin.describeAcls(filter).values());
    }

    @Override
    public void create(Collection<AclBinding> records) {
        get(admin.createAcls(records).all());
    }

    @Override
    public Collection<AclBinding> delete(Collection<AclBindingFilter> filters) {
        return get(admin.deleteAcls(filters).all());
    }

    @Override
    public void close() {
        admin.close();
    }

    private static <T> T get(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw Failure.of(e.getCause() == null ? e : e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Failure.of(e);
        }
    }
}
