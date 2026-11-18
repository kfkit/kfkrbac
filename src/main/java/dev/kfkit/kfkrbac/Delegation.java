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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Who may create and delete which ACL records: the rules of
 * {@code AGENTS.md}, "Who may bind what" and "What a delegate sees and
 * deletes".
 *
 * <ul>
 * <li>A record whose principal type is a role must be a well-formed binding
 * to be created, and one whose principal type is {@code Member} a well-formed
 * group membership, whoever asks. Nobody creates a {@code DENY} ACL whose
 * principal is a group.
 * <li>A principal that the ACLs alone allow {@code ALTER} on the cluster,
 * which a super user is, or one that holds {@code SystemAdmin}, creates any
 * well-formed binding, any membership record and any ordinary ACL, and
 * deletes any record. The first could before there were roles, and ACLs
 * mean what they meant.
 * <li>A principal that holds {@code ResourceOwner} creates and deletes
 * bindings of the roles that are bound to a name pattern, to users and to
 * groups alike, on patterns one of its own {@code ResourceOwner} bindings
 * {@linkplain #covers covers}, and nothing else: no membership, so it does
 * not decide who is in a group.
 * <li>Everyone else creates and deletes nothing.
 * <li>A request is decided as a whole: one refused record refuses them all.
 * </ul>
 *
 * <p>Only rules: the caller, the records and the caller's bindings go in, a
 * decision with a reason for each record comes out. Nothing here knows an
 * authorizer, so that the same rules can explain a refusal elsewhere.
 */
final class Delegation {

    /** How many of its patterns a reason shows an owner before it says how many more there are. */
    private static final int OWNED_SHOWN = 5;

    private static final Answer ALLOWED = new Answer(Outcome.ALLOWED, "allowed");

    /** Who creates and deletes any record, as a reason names them. */
    private static final String WHOEVER_MANAGES_EVERYTHING =
            "a principal that the ACLs allow ALTER on the cluster, as they do a super user, or a SystemAdmin";

    private Delegation() {
    }

    /**
     * Who asks.
     *
     * @param principal the principal of the request
     * @param allowedByAcls whether the ACLs alone, without any role, allow
     *        it {@code ALTER} on the cluster: it is in {@code super.users},
     *        or an ACL allows it and none denies it
     * @param held every binding that applies to it, its groups' included
     */
    record Caller(KafkaPrincipal principal, boolean allowedByAcls, List<RoleBinding> held) {

        Caller {
            Objects.requireNonNull(principal, "principal");
            held = List.copyOf(held);
        }

        /**
         * Whether it may create and delete any record: the ACLs allow it
         * {@code ALTER} on the cluster, or it holds {@code SystemAdmin}.
         */
        boolean managesEverything() {
            if (allowedByAcls) {
                return true;
            }
            for (RoleBinding binding : held) {
                if (binding.role() == Role.SYSTEM_ADMIN && binding.target() == RoleBinding.Target.CLUSTER) {
                    return true;
                }
            }
            return false;
        }

        /** Its {@code ResourceOwner} bindings: what it may hand out roles inside. */
        List<RoleBinding> owned() {
            List<RoleBinding> owned = new ArrayList<>();
            for (RoleBinding binding : held) {
                if (binding.role() == Role.RESOURCE_OWNER) {
                    owned.add(binding);
                }
            }
            return owned;
        }
    }

    /** What became of one record, or of one filter, of a request. */
    enum Outcome {
        /** The caller may, and so may the rest of the request. */
        ALLOWED,
        /**
         * Nobody creates the record: it has a role as its principal type and is not a binding, or
         * {@code Member} and is not a group membership, or it is a {@code DENY} ACL of a group.
         */
        MALFORMED,
        /** The caller may not. */
        FORBIDDEN,
        /** Nothing is wrong with this one; another one of the request was refused, and the request with it. */
        WITH_THE_REQUEST
    }

    /**
     * The answer for one record of a create, or for one filter of a delete.
     *
     * @param outcome what became of it
     * @param reason why, for the caller to act on: who, what, which pattern
     */
    record Answer(Outcome outcome, String reason) {
    }

    /**
     * The decision on a request.
     *
     * @param answers one for each record of a create, or for each filter of
     *        a delete, in the order of the request
     */
    record Decision(List<Answer> answers) {

        Decision {
            answers = List.copyOf(answers);
        }

        /** Whether the request goes through: all of it or none of it. */
        boolean allowed() {
            for (Answer answer : answers) {
                if (answer.outcome() != Outcome.ALLOWED) {
                    return false;
                }
            }
            return true;
        }

        /** The first answer that refuses the request by itself, when it is refused. */
        Optional<Answer> refusal() {
            for (Answer answer : answers) {
                if (answer.outcome() == Outcome.MALFORMED || answer.outcome() == Outcome.FORBIDDEN) {
                    return Optional.of(answer);
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Whether the caller may create these records.
     *
     * @param records the records of one request, as the caller sent them
     */
    static Decision create(Caller caller, List<AclBinding> records) {
        List<Answer> answers = new ArrayList<>(records.size());
        for (AclBinding record : records) {
            BindingCodec.Decoded decoded = BindingCodec.decode(record);
            if (decoded instanceof BindingCodec.Malformed malformed) {
                answers.add(new Answer(Outcome.MALFORMED, describe(record) + " " + malformed.kind().fault() + ": "
                        + malformed.reason()));
                continue;
            }
            String why = whyNot(caller, decoded);
            answers.add(why == null ? ALLOWED : new Answer(Outcome.FORBIDDEN,
                    caller.principal() + " may not create " + describe(record) + ": " + why));
        }
        return asAWhole(answers, "Not created");
    }

    /**
     * Whether the caller may delete the records its filters match.
     *
     * @param matched for each filter of one request, the records it matches;
     *        a filter that matches nothing deletes nothing and is no reason
     *        to refuse
     */
    static Decision delete(Caller caller, List<? extends Collection<AclBinding>> matched) {
        List<Answer> answers = new ArrayList<>(matched.size());
        boolean nothing = !caller.managesEverything() && caller.owned().isEmpty();
        for (Collection<AclBinding> records : matched) {
            if (nothing) {
                answers.add(new Answer(Outcome.FORBIDDEN, caller.principal() + " may not delete ACLs or role bindings: "
                        + whyNobody(caller)));
                continue;
            }
            String first = null;
            int refused = 0;
            for (AclBinding record : records) {
                String why = whyNot(caller, BindingCodec.decode(record));
                if (why != null && refused++ == 0) {
                    first = caller.principal() + " may not delete " + describe(record)
                            + ", which the filter matches: " + why;
                }
            }
            answers.add(first == null ? ALLOWED : new Answer(Outcome.FORBIDDEN,
                    refused == 1 ? first : first + " (and " + (refused - 1) + " more of the " + records.size()
                            + " records the filter matches)"));
        }
        return asAWhole(answers, "Nothing deleted");
    }

    /**
     * Whether a {@code ResourceOwner} binding covers another binding, so
     * that its holder may create and delete that one.
     *
     * <p>A prefixed one covers every literal and prefixed pattern whose name
     * starts with its name, except the literal {@code *}, which names every
     * resource and not one that starts with anything. A literal one covers
     * only the same literal name, {@code *} included: the owner of
     * everything hands out roles on everything, not on a part of it. One on
     * a {@code TOPIC} record covers the patterns narrowed to groups or to
     * transactional IDs too; one that is narrowed, or is for subjects or for
     * connectors, covers only bindings of its own kind. Nothing covers a
     * binding to the cluster.
     */
    static boolean covers(RoleBinding owner, RoleBinding wanted) {
        if (owner.role() != Role.RESOURCE_OWNER || wanted.role().scope() != Role.Scope.PATTERN
                || !reaches(owner.target(), wanted.target())) {
            return false;
        }
        if (owner.patternType() == PatternType.LITERAL) {
            return wanted.patternType() == PatternType.LITERAL && owner.name().equals(wanted.name());
        }
        if (owner.patternType() != PatternType.PREFIXED) {
            return false;
        }
        if (wanted.patternType() == PatternType.LITERAL) {
            return !ResourcePattern.WILDCARD_RESOURCE.equals(wanted.name()) && wanted.name().startsWith(owner.name());
        }
        return wanted.patternType() == PatternType.PREFIXED && wanted.name().startsWith(owner.name());
    }

    /** Whether what an owner's binding names includes what a binding of the other kind names. */
    private static boolean reaches(RoleBinding.Target owner, RoleBinding.Target wanted) {
        switch (owner) {
            case NAMES:
                return wanted == RoleBinding.Target.NAMES || wanted == RoleBinding.Target.GROUPS
                        || wanted == RoleBinding.Target.TRANSACTIONAL_IDS;
            case GROUPS:
            case TRANSACTIONAL_IDS:
            case SUBJECTS:
            case CONNECTORS:
                return wanted == owner;
            default:
                return false;
        }
    }

    /**
     * Why the caller may neither create nor delete the record, or
     * {@code null} when it may. Whether a malformed record can be created at
     * all is not asked here.
     */
    private static String whyNot(Caller caller, BindingCodec.Decoded record) {
        if (caller.managesEverything()) {
            return null;
        }
        List<RoleBinding> owned = caller.owned();
        if (owned.isEmpty()) {
            return whyNobody(caller);
        }
        if (record instanceof BindingCodec.Membership) {
            return "a record of group membership is managed only by " + WHOEVER_MANAGES_EVERYTHING;
        }
        if (record instanceof BindingCodec.Malformed malformed) {
            return "it " + malformed.kind().fault() + " (" + malformed.reason()
                    + "), and such a record is deleted only by " + WHOEVER_MANAGES_EVERYTHING;
        }
        if (!(record instanceof RoleBinding wanted)) {
            return "it is an ordinary ACL, and a ResourceOwner manages role bindings only: an ordinary ACL takes "
                    + WHOEVER_MANAGES_EVERYTHING;
        }
        if (wanted.target() == RoleBinding.Target.CLUSTER) {
            return wanted.role() + " is bound to the cluster, and a binding to the cluster is managed only by "
                    + WHOEVER_MANAGES_EVERYTHING;
        }
        for (RoleBinding owner : owned) {
            if (covers(owner, wanted)) {
                return null;
            }
        }
        return "none of the ResourceOwner bindings of " + caller.principal() + " covers that pattern; it owns "
                + patterns(owned);
    }

    private static String whyNobody(Caller caller) {
        return "that takes " + WHOEVER_MANAGES_EVERYTHING
                + ", or a ResourceOwner of a pattern that covers the record, and " + caller.principal()
                + " is none of them";
    }

    /** The answers as they are, or, when one refuses, with every other one refused for that. */
    private static Decision asAWhole(List<Answer> answers, String notDone) {
        Decision decision = new Decision(answers);
        Optional<Answer> refusal = decision.refusal();
        if (refusal.isEmpty()) {
            return decision;
        }
        Answer withTheRequest = new Answer(Outcome.WITH_THE_REQUEST,
                notDone + ", because the request was refused as a whole: " + refusal.get().reason());
        List<Answer> all = new ArrayList<>(answers.size());
        for (Answer answer : answers) {
            all.add(answer.outcome() == Outcome.ALLOWED ? withTheRequest : answer);
        }
        return new Decision(all);
    }

    /** A record as the caller wrote it: {@code DeveloperRead:User:alice on the TOPIC prefix 'billing.'}. */
    private static String describe(AclBinding record) {
        String what = record.entry().principal();
        BindingCodec.Decoded decoded = BindingCodec.decode(record);
        if (decoded instanceof BindingCodec.Ordinary || decoded instanceof BindingCodec.Malformed malformed
                && malformed.kind() == BindingCodec.Malformed.Kind.GROUP_DENY) {
            what = "the ACL " + what + " " + record.entry().permissionType() + " " + record.entry().operation()
                    + " from host " + record.entry().host();
        }
        return what + " on " + pattern(record.pattern().resourceType(), record.pattern().patternType(),
                record.pattern().name());
    }

    private static String patterns(List<RoleBinding> owned) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < owned.size() && i < OWNED_SHOWN; i++) {
            RoleBinding binding = owned.get(i);
            text.append(i == 0 ? "" : ", ").append(pattern(recordType(binding.target()), binding.patternType(),
                    recordName(binding)));
        }
        if (owned.size() > OWNED_SHOWN) {
            text.append(" and ").append(owned.size() - OWNED_SHOWN).append(" more");
        }
        return text.toString();
    }

    private static String pattern(ResourceType type, PatternType patternType, String name) {
        String kind = patternType == PatternType.PREFIXED ? "prefix"
                : patternType == PatternType.LITERAL ? "name"
                : "pattern (" + patternType + ")";
        return "the " + type + " " + kind + " '" + name + "'";
    }

    /** The resource type of the record a binding was read from. */
    private static ResourceType recordType(RoleBinding.Target target) {
        switch (target) {
            case GROUPS:
                return ResourceType.GROUP;
            case TRANSACTIONAL_IDS:
                return ResourceType.TRANSACTIONAL_ID;
            case CLUSTER:
                return ResourceType.CLUSTER;
            default:
                return ResourceType.TOPIC;
        }
    }

    /** The name of the record a binding was read from. */
    private static String recordName(RoleBinding binding) {
        switch (binding.target()) {
            case SUBJECTS:
                return BindingCodec.SUBJECT_PREFIX + binding.name();
            case CONNECTORS:
                return BindingCodec.CONNECTOR_PREFIX + binding.name();
            default:
                return binding.name();
        }
    }
}
