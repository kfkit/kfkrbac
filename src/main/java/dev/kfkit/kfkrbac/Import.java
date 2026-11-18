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

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * {@code import}: writes the bindings of a file that has another shape
 * than the command's own.
 *
 * <p>Two shapes are read, each described by its fields in
 * {@code docs/import.md}. A list of bindings of named roles becomes
 * bindings. A list of roles that are sets of ACLs becomes, for each role, a
 * group with the role's members and ordinary {@code ALLOW} ACLs whose
 * principal is the group.
 *
 * <p>Nothing is guessed. What the file must have and has not is an
 * {@link Json.Invalid}, and nothing is written. What it has and this cannot
 * express exactly, or could only express as more access than the file
 * gives, is skipped and reported with the reason. Whether a record is a
 * binding, a membership or an ordinary ACL is the codec's answer, as
 * everywhere.
 */
final class Import {

    /** The entry of a scope that names the Kafka cluster a binding is for. */
    private static final String KAFKA_CLUSTER = "kafka-cluster";

    /**
     * What an import writes and what it leaves out.
     *
     * @param what what the records are called in the summary: {@code binding}, {@code record}
     * @param records the records to create, each once
     * @param covered the patterns that are not written because a binding that is written covers them
     * @param skipped what could not be mapped, each with its place and the reason
     * @param leftOut what was not selected: the bindings for another cluster
     * @param notes what the reader should know about what is written
     */
    record Plan(String what, List<AclBinding> records, List<String> covered, List<String> skipped,
            List<String> leftOut, List<String> notes) {
    }

    private record Pattern(Json at, String resourceType, String name, String patternType) {
    }

    private record Bound(Json at, String principal, String role, List<Pattern> patterns, String cluster,
            List<String> otherScope) {
    }

    private record Acl(Json at, String resourceType, String resourceName, String patternType, String operation,
            String permission, String host) {
    }

    private record Group(Json at, String name, List<Json> members, List<Acl> acls) {
    }

    private record Candidate(AclBinding record, RoleBinding binding, String label) {
    }

    private Import() {
    }

    // --- role-bindings ---------------------------------------------------------

    /**
     * Reads a list of bindings of named roles.
     *
     * @param clusterId the cluster whose bindings are wanted, or {@code null}: then the file must be for one
     */
    static Plan roleBindings(Path file, String clusterId) throws Json.Invalid, Arguments.WrongUsage {
        List<Bound> bound = new ArrayList<>();
        Set<String> clusters = new TreeSet<>();
        for (Json object : Json.read(file).elements()) {
            String principal = object.text("principal");
            String role = object.text("role");
            List<Pattern> patterns = new ArrayList<>();
            Optional<Json> listed = object.optional("resourcePatterns");
            if (listed.isPresent()) {
                for (Json pattern : listed.get().elements()) {
                    patterns.add(new Pattern(pattern, pattern.text("resourceType"), pattern.text("name"),
                            pattern.text("patternType")));
                }
            }
            String cluster = null;
            List<String> otherScope = new ArrayList<>();
            Optional<Json> scope = object.optional("scope");
            if (scope.isPresent()) {
                for (Map.Entry<String, Json> field : scope.get().fields().entrySet()) {
                    if (!"clusters".equals(field.getKey())) {
                        otherScope.add(field.getKey());
                        continue;
                    }
                    for (Map.Entry<String, Json> entry : field.getValue().fields().entrySet()) {
                        String id = entry.getValue().text();
                        if (KAFKA_CLUSTER.equals(entry.getKey())) {
                            cluster = id;
                            clusters.add(id);
                        } else {
                            otherScope.add(entry.getKey());
                        }
                    }
                }
            }
            bound.add(new Bound(object, principal, role, patterns, cluster, otherScope));
        }
        if (clusterId == null && clusters.size() > 1) {
            throw new Arguments.WrongUsage("The file has bindings for " + clusters.size() + " clusters, "
                    + String.join(", ", clusters) + ": say which with --cluster-id");
        }

        List<Candidate> candidates = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<String> leftOut = new ArrayList<>();
        for (Bound binding : bound) {
            String label = label(binding.at()) + " " + binding.role() + " to " + binding.principal();
            if (clusterId != null && binding.cluster() != null && !binding.cluster().equals(clusterId)) {
                leftOut.add(label + ": it is for the cluster " + binding.cluster());
                continue;
            }
            String reason = notABinding(binding);
            if (reason != null) {
                skipped.add(label + ": " + reason);
                continue;
            }
            if (binding.patterns().isEmpty()) {
                candidate(binding, RoleBinding.Target.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL, label,
                        candidates, skipped);
            }
            for (Pattern pattern : binding.patterns()) {
                String on = label(pattern.at()) + " " + binding.role() + " to " + binding.principal() + " on "
                        + pattern.resourceType() + " " + pattern.name() + " " + pattern.patternType();
                PatternType patternType = "LITERAL".equals(pattern.patternType()) ? PatternType.LITERAL
                        : "PREFIXED".equals(pattern.patternType()) ? PatternType.PREFIXED
                        : null;
                RoleBinding.Target target = target(pattern.resourceType());
                if (target == null) {
                    skipped.add(on + ": nothing here authorizes the resource type " + pattern.resourceType());
                } else if (patternType == null) {
                    skipped.add(on + ": the pattern type is neither LITERAL nor PREFIXED");
                } else if (target == RoleBinding.Target.CLUSTER
                        && !(Resource.CLUSTER_NAME.equals(pattern.name()) && patternType == PatternType.LITERAL)) {
                    skipped.add(on + ": the cluster is the literal name " + Resource.CLUSTER_NAME);
                } else {
                    candidate(binding, target, pattern.name(), patternType, on, candidates, skipped);
                }
            }
        }

        // A binding on a name pattern covers the groups and the transactional IDs of that pattern too.
        Map<String, RoleBinding> onNames = new HashMap<>();
        for (Candidate candidate : candidates) {
            if (candidate.binding().target() == RoleBinding.Target.NAMES) {
                onNames.put(key(candidate.binding()), candidate.binding());
            }
        }
        Set<AclBinding> records = new LinkedHashSet<>();
        List<String> covered = new ArrayList<>();
        for (Candidate candidate : candidates) {
            RoleBinding binding = candidate.binding();
            RoleBinding wider = binding.target() == RoleBinding.Target.GROUPS
                    || binding.target() == RoleBinding.Target.TRANSACTIONAL_IDS ? onNames.get(key(binding)) : null;
            if (wider != null) {
                covered.add(candidate.label() + ": within " + Wording.record(BindingRecords.record(
                        wider.role().roleName(), wider.member().toString(), wider.target(), wider.name(),
                        wider.patternType())));
            } else {
                records.add(candidate.record());
            }
        }
        List<String> notes = new ArrayList<>();
        if (!onNames.isEmpty()) {
            notes.add("A binding on a name or a prefix is for the topics, the groups and the transactional IDs of"
                    + " that name: a pattern for topics is written as one.");
        }
        return new Plan("binding", List.copyOf(records), covered, skipped, leftOut, notes);
    }

    /** Why nothing of the binding can be written, or {@code null} when its patterns are to be looked at. */
    private static String notABinding(Bound binding) {
        KafkaPrincipal member = Wording.principal(binding.principal());
        if (member == null || !KafkaPrincipal.USER_TYPE.equals(member.getPrincipalType())
                && !BindingCodec.GROUP_TYPE.equals(member.getPrincipalType())) {
            return "the principal is neither User:<name> nor Group:<name>";
        }
        Role role = Role.named(binding.role()).orElse(null);
        if (role == null) {
            return "there is no role " + binding.role() + " here: the roles are " + String.join(", ", Role.NAMES);
        }
        if (!binding.otherScope().isEmpty()) {
            return "its scope has more than the Kafka cluster (" + String.join(", ", binding.otherScope())
                    + "), and a binding here cannot be narrowed to that: it is for everything that reads the"
                    + " bindings of the Kafka cluster. Take that out of the file to import it so";
        }
        if (binding.patterns().isEmpty() && role.scope() != Role.Scope.CLUSTER) {
            return role + " is bound to a name pattern, and this binding has no resource pattern";
        }
        return null;
    }

    /** The record of one pattern of a binding, when the codec reads it back as what the file says. */
    private static void candidate(Bound binding, RoleBinding.Target target, String name, PatternType patternType,
            String label, List<Candidate> candidates, List<String> skipped) {
        AclBinding record = BindingRecords.record(binding.role(), binding.principal(), target, name, patternType);
        BindingCodec.Decoded decoded = BindingCodec.decode(record);
        if (decoded instanceof BindingCodec.Malformed malformed) {
            skipped.add(label + ": " + malformed.reason());
        } else if (!(decoded instanceof RoleBinding read) || read.target() != target
                || target != RoleBinding.Target.CLUSTER
                        && !(read.name().equals(name) && read.patternType() == patternType)) {
            skipped.add(label + ": as a record it would be read as "
                    + (decoded instanceof RoleBinding other ? Wording.on(other) : "an ACL"));
        } else {
            candidates.add(new Candidate(record, read, label));
        }
    }

    /** What a binding names for a resource type of the file, or {@code null} for one nothing here authorizes. */
    private static RoleBinding.Target target(String resourceType) {
        switch (resourceType) {
            case "Topic":
                return RoleBinding.Target.NAMES;
            case "Group":
                return RoleBinding.Target.GROUPS;
            case "TransactionalId":
                return RoleBinding.Target.TRANSACTIONAL_IDS;
            case "Cluster":
                return RoleBinding.Target.CLUSTER;
            case "Subject":
                return RoleBinding.Target.SUBJECTS;
            case "Connector":
                return RoleBinding.Target.CONNECTORS;
            default:
                return null;
        }
    }

    /** Who holds which role on which name pattern: what two bindings share when one covers the other. */
    private static String key(RoleBinding binding) {
        return binding.role().roleName() + '\u0000' + binding.member() + '\u0000' + binding.patternType() + '\u0000'
                + binding.name();
    }

    // --- role-acls -------------------------------------------------------------

    /** Reads a list of roles that are sets of ACLs. */
    static Plan roleAcls(Path file) throws Json.Invalid {
        List<Group> groups = new ArrayList<>();
        for (Json object : Json.read(file).elements()) {
            String name = object.text("name");
            List<Json> members = object.field("members").elements();
            for (Json member : members) {
                member.text();
            }
            List<Acl> acls = new ArrayList<>();
            for (Json acl : object.field("acls").elements()) {
                acls.add(new Acl(acl, acl.text("resourceType"), acl.text("resourceName"), acl.text("patternType"),
                        acl.text("operation"), acl.text("permission"), acl.text("host")));
            }
            groups.add(new Group(object, name, members, acls));
        }

        Set<AclBinding> records = new LinkedHashSet<>();
        List<String> skipped = new ArrayList<>();
        for (Group group : groups) {
            String label = label(group.at()) + " role " + group.name();
            String reason = notAGroup(group);
            if (reason != null) {
                skipped.add(label + ": " + reason + "; nothing of the role is written");
                continue;
            }
            for (Json member : group.members()) {
                String who = member.text();
                AclBinding record = BindingRecords.membership(group.name(), who);
                BindingCodec.Decoded decoded = BindingCodec.decode(record);
                if (decoded instanceof BindingCodec.Membership) {
                    records.add(record);
                } else {
                    skipped.add(label(member) + " member " + who + " of the role " + group.name() + ": "
                            + (decoded instanceof BindingCodec.Malformed malformed ? malformed.reason()
                                    : "it is not a membership"));
                }
            }
            for (Acl acl : group.acls()) {
                String on = label(acl.at()) + " ACL " + acl.permission() + " " + acl.operation() + " on "
                        + acl.resourceType() + " " + acl.resourceName() + " " + acl.patternType() + " of the role "
                        + group.name();
                ResourceType resourceType = resourceType(acl.resourceType());
                PatternType patternType = PatternType.fromString(acl.patternType().toUpperCase(Locale.ROOT));
                AclOperation operation = operation(acl.operation());
                if (resourceType == ResourceType.UNKNOWN || resourceType == ResourceType.ANY) {
                    skipped.add(on + ": " + acl.resourceType() + " is not a resource type of an ACL");
                } else if (patternType != PatternType.LITERAL && patternType != PatternType.PREFIXED) {
                    skipped.add(on + ": the pattern type is neither LITERAL nor PREFIXED");
                } else if (operation == AclOperation.UNKNOWN || operation == AclOperation.ANY) {
                    skipped.add(on + ": " + acl.operation() + " is not an operation of an ACL");
                } else if (acl.resourceName().isEmpty() || acl.host().isEmpty()) {
                    skipped.add(on + ": the " + (acl.resourceName().isEmpty() ? "resource name" : "host")
                            + " is empty");
                } else {
                    AclBinding record = new AclBinding(
                            new ResourcePattern(resourceType, acl.resourceName(), patternType),
                            new AccessControlEntry(BindingCodec.GROUP_PREFIX + group.name(), acl.host(), operation,
                                    AclPermissionType.ALLOW));
                    if (BindingCodec.decode(record) instanceof BindingCodec.Ordinary) {
                        records.add(record);
                    } else {
                        skipped.add(on + ": as a record it would not be read as an ACL of the group");
                    }
                }
            }
        }
        return new Plan("record", List.copyOf(records), List.of(), skipped, List.of(), List.of());
    }

    /** Why nothing of the role can be written, or {@code null}. */
    private static String notAGroup(Group group) {
        if (group.name().isEmpty()) {
            return "its name is empty, and it would be the name of the group";
        }
        for (Acl acl : group.acls()) {
            AclPermissionType permission = AclPermissionType.fromString(acl.permission());
            if (permission == AclPermissionType.DENY) {
                return "it has a DENY ACL (" + label(acl.at()) + "), and a group cannot be denied: a DENY ACL names"
                        + " the users";
            }
            if (permission != AclPermissionType.ALLOW) {
                return "the permission " + acl.permission() + " of an ACL of it (" + label(acl.at())
                        + ") is neither ALLOW nor DENY";
            }
        }
        return null;
    }

    private static ResourceType resourceType(String text) {
        try {
            return ResourceType.fromString(text);
        } catch (IllegalArgumentException e) {
            return ResourceType.UNKNOWN;
        }
    }

    private static AclOperation operation(String text) {
        try {
            return AclOperation.fromString(text);
        } catch (IllegalArgumentException e) {
            return AclOperation.UNKNOWN;
        }
    }

    // --- both ------------------------------------------------------------------

    /**
     * Writes what the plan has, unless it is a dry run, and prints what
     * was written and what was left out.
     *
     * @return the exit status
     */
    static int run(Plan plan, boolean dryRun, AclRecords records, PrintStream out) {
        // Written first: a cluster that refuses says why, and nothing is reported as written.
        if (!dryRun && !plan.records().isEmpty()) {
            records.create(plan.records());
        }
        for (AclBinding record : plan.records()) {
            out.println("+ " + Wording.record(record));
        }
        for (String covered : plan.covered()) {
            out.println("covered " + covered);
        }
        for (String skipped : plan.skipped()) {
            out.println("skipped " + skipped);
        }
        for (String leftOut : plan.leftOut()) {
            out.println("left out " + leftOut);
        }
        int written = plan.records().size();
        StringBuilder summary = new StringBuilder(dryRun ? "Would write " : "Wrote ").append(written).append(' ')
                .append(plan.what()).append(written == 1 ? "" : "s");
        if (!plan.covered().isEmpty()) {
            summary.append("; ").append(plan.covered().size()).append(" covered by another");
        }
        if (!plan.skipped().isEmpty()) {
            summary.append("; skipped ").append(plan.skipped().size());
        }
        if (!plan.leftOut().isEmpty()) {
            summary.append("; left out ").append(plan.leftOut().size()).append(" for another cluster");
        }
        out.println(summary.append(dryRun ? ". Nothing was changed." : "."));
        for (String note : plan.notes()) {
            out.println(note);
        }
        return RbacCommand.DONE;
    }

    /** Where something is in the file: {@code [2].resourcePatterns[1] (line 17)}. */
    private static String label(Json at) {
        return at.path() + " (line " + at.line() + ")";
    }
}
