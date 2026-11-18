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

import org.apache.kafka.common.errors.AuthorizationException;

/**
 * What the cluster answered a client's request: it went through, or the
 * cluster refused it and said why.
 *
 * @param request what was asked, for the failure message: {@code alice producing to billing.orders}
 * @param refusal the exception the client got, or {@code null} when the request went through
 */
record Answer(String request, AuthorizationException refusal) {

    static Answer allowed(String request) {
        return new Answer(request, null);
    }

    static Answer refused(String request, AuthorizationException refusal) {
        return new Answer(request, refusal);
    }

    boolean allowed() {
        return refusal == null;
    }

    /** Fails unless the request went through. */
    void assertAllowed(String why) {
        if (!allowed()) {
            throw new AssertionError("expected " + request + " to be allowed, " + why + "; the cluster answered: "
                    + this, refusal);
        }
    }

    /** Fails unless the request was refused. */
    void assertRefused(String why) {
        if (allowed()) {
            throw new AssertionError("expected " + request + " to be refused, " + why + "; the cluster answered: "
                    + this);
        }
    }

    /** Fails unless the request was refused with that exception. */
    void assertRefused(Class<? extends AuthorizationException> with, String why) {
        if (!with.isInstance(refusal)) {
            throw new AssertionError("expected " + request + " to be refused with " + with.getSimpleName() + ", "
                    + why + "; the cluster answered: " + this, refusal);
        }
    }

    @Override
    public String toString() {
        return allowed()
                ? request + ": allowed"
                : request + ": refused, " + refusal.getClass().getSimpleName() + ": " + refusal.getMessage();
    }
}
