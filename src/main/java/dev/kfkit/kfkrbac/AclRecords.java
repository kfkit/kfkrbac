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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

/**
 * The ACL records of a cluster, as the command reads and writes them: the
 * three calls of the admin client it is allowed, and nothing else.
 *
 * <p>Every verb works through this, so a test can run it against a set of
 * records in memory. {@link AdminAclRecords} is the one that talks to a
 * cluster.
 */
interface AclRecords extends AutoCloseable {

    /** The records the filter matches. */
    Collection<AclBinding> describe(AclBindingFilter filter);

    /** Creates the records; one that exists already is left as it is. */
    void create(Collection<AclBinding> records);

    /** Deletes the records the filters match and returns them. */
    Collection<AclBinding> delete(Collection<AclBindingFilter> filters);

    @Override
    void close();

    /** The cluster refused a call, or it failed: the reason is the one the cluster returned. */
    final class Failure extends RuntimeException {

        private static final long serialVersionUID = 1L;

        Failure(String reason, Throwable cause) {
            super(reason, cause);
        }

        /** What the cluster said, with the kind of error in front when it says what the message does not. */
        static Failure of(Throwable cause) {
            String message = cause.getMessage();
            String kind = cause.getClass().getSimpleName();
            return new Failure(message == null || message.isBlank() ? kind : kind + ": " + message, cause);
        }
    }
}
