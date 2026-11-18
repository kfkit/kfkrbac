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

import java.util.Set;

import org.apache.kafka.common.acl.AclBinding;

/**
 * What the cluster answered a client's request to delete ACLs. A request
 * that goes through says which records it deleted, and that is none of them
 * when its filters matched nothing: going through is not yet deleting.
 *
 * @param answer whether the request went through or was refused, and why
 * @param deleted the records the request deleted; empty when it was refused
 */
record Deletion(Answer answer, Set<AclBinding> deleted) {

    Deletion {
        deleted = Set.copyOf(deleted);
    }

    @Override
    public String toString() {
        return answer.allowed() ? answer + ", and deleted " + deleted : answer.toString();
    }
}
