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

import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * The identities a principal holds bindings under: itself and, once group
 * membership is read, the groups it is a member of.
 *
 * <p>Asked on the broker's request path: in memory, no I/O, no blocking.
 */
interface Identities {

    /** A principal is only itself: no group is known. */
    Identities SELF = principal -> List.of(principal);

    /**
     * The members a binding may name for it to apply to the principal, the
     * principal itself among them.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     */
    List<KafkaPrincipal> of(KafkaPrincipal principal);
}
