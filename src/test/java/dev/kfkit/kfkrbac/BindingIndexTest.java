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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.junit.jupiter.api.Test;

/**
 * What the index does that the authorizer cannot show yet: the identities of
 * a principal, and records that arrive in ways the broker does not send
 * them. The rest is in {@link RbacAuthorizerBindingsTest}.
 */
class BindingIndexTest {

    private static final KafkaPrincipal ALICE = new KafkaPrincipal("User", "alice");
    private static final KafkaPrincipal PAYMENTS = new KafkaPrincipal("Group", "payments");
    private static final ResourcePattern BILLING = new ResourcePattern(ResourceType.TOPIC, "billing.",
            PatternType.PREFIXED);

    @Test
    void aBindingOfAnotherIdentityOfThePrincipalApplies() {
        BindingIndex index = new BindingIndex(
                principal -> principal.equals(ALICE) ? List.of(ALICE, PAYMENTS) : List.of(principal));
        index.add(Uuid.randomUuid(), binding("DeveloperRead:Group:payments", "billing."));

        assertTrue(index.grants(ALICE, read("billing.invoices")));
        assertEquals(List.of(BILLING), index.grantedPatterns(ALICE, AclOperation.READ, ResourceType.TOPIC));
        assertFalse(index.grants(new KafkaPrincipal("User", "bob"), read("billing.invoices")));
    }

    @Test
    void aPrincipalIsOnlyItselfUntilGroupsAreRead() {
        BindingIndex index = new BindingIndex();
        index.add(Uuid.randomUuid(), binding("DeveloperRead:Group:payments", "billing."));
        index.add(Uuid.randomUuid(), new StandardAcl(ResourceType.TOPIC, "Group:payments", PatternType.LITERAL,
                "Member:User:alice", "*", AclOperation.ALL, AclPermissionType.ALLOW));

        assertFalse(index.grants(ALICE, read("billing.invoices")));
        assertEquals(List.of(), index.grantedPatterns(ALICE, AclOperation.READ, ResourceType.TOPIC));
        assertEquals(0, index.malformed());
    }

    @Test
    void whatAPrincipalHoldsIsItsOwnBindingsAndThoseOfItsOtherIdentities() {
        BindingIndex index = new BindingIndex(
                principal -> principal.equals(ALICE) ? List.of(ALICE, PAYMENTS) : List.of(principal));
        Uuid owner = Uuid.randomUuid();
        index.add(owner, binding("ResourceOwner:User:alice", "billing."));
        index.add(Uuid.randomUuid(), binding("DeveloperRead:Group:payments", "payments."));
        index.add(Uuid.randomUuid(), binding("DeveloperWrite:User:bob", "billing."));
        KafkaPrincipal bob = new KafkaPrincipal("User", "bob");

        assertEquals(List.of(
                new RoleBinding(Role.RESOURCE_OWNER, ALICE, RoleBinding.Target.NAMES, "billing.",
                        PatternType.PREFIXED),
                new RoleBinding(Role.DEVELOPER_READ, PAYMENTS, RoleBinding.Target.NAMES, "payments.",
                        PatternType.PREFIXED)),
                index.heldBy(ALICE));
        assertTrue(index.holds(ALICE, Role.RESOURCE_OWNER));
        assertTrue(index.holds(ALICE, Role.DEVELOPER_READ));
        assertFalse(index.holds(ALICE, Role.DEVELOPER_WRITE));
        assertFalse(index.holds(bob, Role.RESOURCE_OWNER));
        assertEquals(1, index.heldBy(bob).size());
        assertEquals(List.of(), index.heldBy(new KafkaPrincipal("User", "carol")));

        index.remove(owner);
        assertFalse(index.holds(ALICE, Role.RESOURCE_OWNER));
        assertEquals(1, index.heldBy(ALICE).size());
    }

    @Test
    void theSameBindingUnderTwoIdsIsHeldUntilBothAreRemoved() {
        BindingIndex index = new BindingIndex();
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        index.add(first, binding("DeveloperRead:User:alice", "billing."));
        index.add(second, binding("DeveloperRead:User:alice", "billing."));

        index.remove(first);
        assertTrue(index.grants(ALICE, read("billing.invoices")));

        index.remove(second);
        assertFalse(index.grants(ALICE, read("billing.invoices")));
    }

    @Test
    void anIdAddedAgainReplacesItsRecord() {
        BindingIndex index = new BindingIndex();
        Uuid id = Uuid.randomUuid();
        index.add(id, binding("DeveloperRead:User:alice", "billing."));
        index.add(id, binding("DeveloperRead:User:alice", "payments."));

        assertFalse(index.grants(ALICE, read("billing.invoices")));
        assertTrue(index.grants(ALICE, read("payments.refunds")));

        index.remove(id);
        assertFalse(index.grants(ALICE, read("payments.refunds")));
    }

    @Test
    void removingAnIdThatIsNotThereChangesNothing() {
        BindingIndex index = new BindingIndex();
        index.add(Uuid.randomUuid(), binding("DeveloperRead:User:alice", "billing."));

        index.remove(Uuid.randomUuid());

        assertTrue(index.grants(ALICE, read("billing.invoices")));
    }

    private static Action read(String topic) {
        return new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), 1,
                true, true);
    }

    private static StandardAcl binding(String principal, String prefix) {
        return new StandardAcl(ResourceType.TOPIC, prefix, PatternType.PREFIXED, principal, "*", AclOperation.ALL,
                AclPermissionType.ALLOW);
    }
}
