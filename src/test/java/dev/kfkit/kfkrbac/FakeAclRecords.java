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
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

/**
 * The ACL records of a cluster, in memory, for tests of the command: it
 * answers the three calls with Kafka's own filters, counts them, and can
 * refuse as a cluster would.
 */
final class FakeAclRecords implements AclRecords {

    final Set<AclBinding> records = new LinkedHashSet<>();
    int described;
    int created;
    int deleted;
    boolean closed;

    /** When set, every call fails with it, as a call the cluster refuses does. */
    Failure refusal;

    FakeAclRecords(AclBinding... records) {
        this.records.addAll(List.of(records));
    }

    @Override
    public Collection<AclBinding> describe(AclBindingFilter filter) {
        described++;
        refuse();
        return records.stream().filter(filter::matches).toList();
    }

    @Override
    public void create(Collection<AclBinding> created) {
        this.created++;
        refuse();
        records.addAll(created);
    }

    @Override
    public Collection<AclBinding> delete(Collection<AclBindingFilter> filters) {
        deleted++;
        refuse();
        List<AclBinding> matched = new ArrayList<>();
        for (AclBinding record : records) {
            if (filters.stream().anyMatch(filter -> filter.matches(record))) {
                matched.add(record);
            }
        }
        matched.forEach(records::remove);
        return matched;
    }

    @Override
    public void close() {
        closed = true;
    }

    private void refuse() {
        if (refusal != null) {
            throw refusal;
        }
    }
}
