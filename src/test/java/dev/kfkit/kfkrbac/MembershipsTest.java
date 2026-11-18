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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

/** The groups that membership records make, without an authorizer. */
class MembershipsTest {

    private static final KafkaPrincipal ALICE = new KafkaPrincipal("User", "alice");
    private static final KafkaPrincipal BOB = new KafkaPrincipal("User", "bob");
    private static final KafkaPrincipal PAYMENTS = new KafkaPrincipal("Group", "payments");
    private static final KafkaPrincipal BILLING = new KafkaPrincipal("Group", "billing");
    private static final KafkaPrincipal AUDIT = new KafkaPrincipal("Group", "audit");

    private final Memberships memberships = new Memberships();

    @Test
    void aPrincipalInNoGroupIsOnlyItself() {
        memberships.add(Uuid.randomUuid(), in(ALICE, PAYMENTS));

        assertEquals(List.of(BOB), memberships.of(BOB));
        assertEquals(List.of(PAYMENTS), memberships.of(PAYMENTS));
    }

    @Test
    void aMemberIsItselfAndItsGroupsUntilTheRecordsAreRemoved() {
        Uuid payments = Uuid.randomUuid();
        Uuid billing = Uuid.randomUuid();
        memberships.add(payments, in(ALICE, PAYMENTS));
        memberships.add(billing, in(ALICE, BILLING));
        memberships.add(Uuid.randomUuid(), in(BOB, BILLING));

        assertEquals(List.of(ALICE, PAYMENTS, BILLING), memberships.of(ALICE));
        assertEquals(List.of(BOB, BILLING), memberships.of(BOB));

        memberships.remove(payments);
        assertEquals(List.of(ALICE, BILLING), memberships.of(ALICE));

        memberships.remove(billing);
        assertEquals(List.of(ALICE), memberships.of(ALICE));
        assertEquals(List.of(BOB, BILLING), memberships.of(BOB));
    }

    @Test
    void theSameMembershipUnderTwoIdsLastsUntilBothAreRemoved() {
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        memberships.add(first, in(ALICE, PAYMENTS));
        memberships.add(second, in(ALICE, PAYMENTS));

        memberships.remove(first);
        assertEquals(List.of(ALICE, PAYMENTS), memberships.of(ALICE));

        memberships.remove(second);
        assertEquals(List.of(ALICE), memberships.of(ALICE));
    }

    @Test
    void anIdAddedAgainReplacesItsRecord() {
        Uuid id = Uuid.randomUuid();
        memberships.add(id, in(ALICE, PAYMENTS));
        memberships.add(id, in(ALICE, BILLING));

        assertEquals(List.of(ALICE, BILLING), memberships.of(ALICE));

        memberships.remove(id);
        assertEquals(List.of(ALICE), memberships.of(ALICE));
    }

    @Test
    void removingAnIdThatIsNotThereChangesNothing() {
        memberships.add(Uuid.randomUuid(), in(ALICE, PAYMENTS));

        memberships.remove(Uuid.randomUuid());

        assertEquals(List.of(ALICE, PAYMENTS), memberships.of(ALICE));
    }

    @Test
    void aSnapshotReplacesEveryMembership() {
        memberships.add(Uuid.randomUuid(), in(ALICE, PAYMENTS));
        Uuid bobs = Uuid.randomUuid();

        memberships.load(Map.of(bobs, in(BOB, BILLING)));

        assertEquals(List.of(ALICE), memberships.of(ALICE));
        assertEquals(List.of(BOB, BILLING), memberships.of(BOB));

        memberships.remove(bobs);
        assertEquals(List.of(BOB), memberships.of(BOB));
    }

    @Test
    void betweenTheTwoStepsOfASnapshotAMemberIsOnlyInTheGroupsItStaysIn() {
        memberships.add(Uuid.randomUuid(), in(ALICE, PAYMENTS));
        memberships.add(Uuid.randomUuid(), in(ALICE, BILLING));
        memberships.add(Uuid.randomUuid(), in(BOB, BILLING));
        Map<Uuid, BindingCodec.Membership> snapshot = new LinkedHashMap<>();
        snapshot.put(Uuid.randomUuid(), in(ALICE, BILLING));
        snapshot.put(Uuid.randomUuid(), in(ALICE, AUDIT));
        snapshot.put(Uuid.randomUuid(), in(BOB, PAYMENTS));

        memberships.narrow(snapshot.values());

        // alice has left payments and has not joined audit; bob is in neither group
        assertEquals(List.of(ALICE, BILLING), memberships.of(ALICE));
        assertEquals(List.of(BOB), memberships.of(BOB));

        memberships.load(snapshot);

        assertEquals(List.of(ALICE, BILLING, AUDIT), memberships.of(ALICE));
        assertEquals(List.of(BOB, PAYMENTS), memberships.of(BOB));
    }

    @Test
    void aSnapshotThatChangesNothingLeavesEveryMemberInItsGroupsThroughout() {
        Map<Uuid, BindingCodec.Membership> snapshot = new LinkedHashMap<>();
        snapshot.put(Uuid.randomUuid(), in(ALICE, PAYMENTS));
        snapshot.put(Uuid.randomUuid(), in(ALICE, BILLING));
        memberships.load(snapshot);

        memberships.narrow(snapshot.values());
        assertEquals(List.of(ALICE, PAYMENTS, BILLING), memberships.of(ALICE));

        memberships.load(snapshot);
        assertEquals(List.of(ALICE, PAYMENTS, BILLING), memberships.of(ALICE));
    }

    private static BindingCodec.Membership in(KafkaPrincipal member, KafkaPrincipal group) {
        return new BindingCodec.Membership(member, group);
    }
}
