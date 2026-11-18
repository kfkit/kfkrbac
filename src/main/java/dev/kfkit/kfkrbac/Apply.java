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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

/**
 * {@code apply}: makes the bindings and the memberships of the cluster
 * those of a file.
 *
 * <p>The file is what the command itself prints: an object with
 * {@code bindings}, the array of {@code list --output json}, and
 * {@code groups}, the array of {@code group list --output json}; or the
 * array of {@code list --output json} alone, which is the bindings. A kind
 * the file does not have is not touched. Of a kind it has, what is in the
 * file and not in the cluster is created, and what is in the cluster and
 * not in the file is deleted.
 *
 * <p>Only bindings and memberships are compared, read by the codec that
 * reads them everywhere: an ordinary ACL is never created and never
 * deleted, and neither is a record that looks like a binding or a
 * membership and is not one.
 */
final class Apply {

    private static final String BINDINGS = "bindings";
    private static final String GROUPS = "groups";

    /**
     * What a file says the cluster is to have, as records.
     *
     * @param bindings the bindings, or {@code null} when the file does not speak of bindings
     * @param memberships the memberships, or {@code null} when the file does not speak of groups
     */
    record Wanted(Set<AclBinding> bindings, Set<AclBinding> memberships) {
    }

    private Apply() {
    }

    /** Reads the file: every object in it must be a binding or a membership the record format allows. */
    static Wanted read(Path file) throws Json.Invalid {
        Json root = Json.read(file);
        if (root.isList()) {
            return new Wanted(bindings(root), null);
        }
        Map<String, Json> fields = root.fields();
        for (Map.Entry<String, Json> field : fields.entrySet()) {
            if (!BINDINGS.equals(field.getKey()) && !GROUPS.equals(field.getKey())) {
                throw field.getValue().invalid("is not a field of this file, which has \"" + BINDINGS + "\" and \""
                        + GROUPS + "\"");
            }
        }
        if (fields.isEmpty()) {
            throw root.invalid("has neither \"" + BINDINGS + "\" nor \"" + GROUPS + "\"");
        }
        Json bindings = fields.get(BINDINGS);
        Json groups = fields.get(GROUPS);
        return new Wanted(bindings == null ? null : bindings(bindings), groups == null ? null : memberships(groups));
    }

    /**
     * Compares the file with the cluster, prints the difference, and makes
     * the cluster match unless it is a dry run.
     *
     * @param noDelete only create: what is in the cluster and not in the file stays
     * @return the exit status
     */
    static int run(Wanted wanted, boolean dryRun, boolean noDelete, AclRecords records, PrintStream out,
            PrintStream err) {
        Set<AclBinding> want = new HashSet<>();
        if (wanted.bindings() != null) {
            want.addAll(wanted.bindings());
        }
        if (wanted.memberships() != null) {
            want.addAll(wanted.memberships());
        }
        Set<AclBinding> have = new HashSet<>();
        for (AclBinding record : RbacCommand.everyRecord(records, err)) {
            BindingCodec.Decoded decoded = BindingCodec.decode(record);
            if (decoded instanceof RoleBinding && wanted.bindings() != null
                    || decoded instanceof BindingCodec.Membership && wanted.memberships() != null) {
                have.add(record);
            }
        }
        List<AclBinding> create = ordered(want, have);
        List<AclBinding> delete = ordered(have, want);
        for (AclBinding record : create) {
            out.println("+ " + Wording.record(record));
        }
        for (AclBinding record : delete) {
            out.println((noDelete ? "  not in the file, and left: " : "- ") + Wording.record(record));
        }
        String left = noDelete && !delete.isEmpty()
                ? "; " + delete.size() + " not in the file left as " + (delete.size() == 1 ? "it is" : "they are")
                        + ", as --no-delete says"
                : "";
        int deleting = noDelete ? 0 : delete.size();
        if (create.isEmpty() && deleting == 0) {
            out.println(left.isEmpty() ? "Nothing to change: the cluster has what the file has."
                    : "Nothing to change" + left + ".");
            return RbacCommand.DONE;
        }
        if (dryRun) {
            out.println("Would create " + create.size() + " and delete " + deleting + left
                    + ". Nothing was changed.");
            return RbacCommand.DONE;
        }
        if (!create.isEmpty()) {
            records.create(create);
        }
        int deleted = 0;
        if (deleting > 0) {
            List<AclBindingFilter> exactly = new ArrayList<>();
            for (AclBinding record : delete) {
                exactly.add(record.toFilter());
            }
            deleted = records.delete(exactly).size();
        }
        out.println("Created " + create.size() + ", deleted " + deleted + left + ".");
        return RbacCommand.DONE;
    }

    /** What is in the first set and not in the second, in the order records are printed in. */
    private static List<AclBinding> ordered(Collection<AclBinding> in, Set<AclBinding> notIn) {
        List<AclBinding> records = new ArrayList<>();
        for (AclBinding record : in) {
            if (!notIn.contains(record)) {
                records.add(record);
            }
        }
        records.sort(BindingRecords.ORDER);
        return records;
    }

    /** The bindings of an array as {@code list --output json} prints it. */
    private static Set<AclBinding> bindings(Json array) throws Json.Invalid {
        Set<AclBinding> records = new LinkedHashSet<>();
        for (Json object : array.elements()) {
            String role = object.text("role");
            String member = object.text("member");
            ResourceType resourceType = resourceType(object.field("resourceType"));
            String name = object.text("name");
            PatternType patternType = patternType(object.field("patternType"));
            if (Role.named(role).isEmpty()) {
                throw object.field("role").invalid("is " + role + ", and there is no such role: the roles are "
                        + String.join(", ", Role.NAMES));
            }
            AclBinding record = BindingRecords.record(role, member, resourceType, name, patternType);
            if (BindingCodec.decode(record) instanceof BindingCodec.Malformed malformed) {
                throw object.invalid("is not a binding: " + malformed.reason());
            }
            records.add(record);
        }
        return records;
    }

    /** The memberships of an array as {@code group list --output json} prints it. */
    private static Set<AclBinding> memberships(Json array) throws Json.Invalid {
        Set<AclBinding> records = new LinkedHashSet<>();
        for (Json object : array.elements()) {
            String group = object.text("group");
            String member = object.text("member");
            if (group.isEmpty()) {
                throw object.field("group").invalid("is empty, and must be the name of a group");
            }
            AclBinding record = BindingRecords.membership(group, member);
            BindingCodec.Decoded decoded = BindingCodec.decode(record);
            if (decoded instanceof BindingCodec.Malformed malformed) {
                throw object.invalid("is not a membership: " + malformed.reason());
            }
            if (!(decoded instanceof BindingCodec.Membership)) {
                throw object.invalid("is not a membership");
            }
            records.add(record);
        }
        return records;
    }

    private static ResourceType resourceType(Json field) throws Json.Invalid {
        String text = field.text();
        for (ResourceType type : List.of(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID,
                ResourceType.CLUSTER)) {
            if (type.name().equals(text)) {
                return type;
            }
        }
        throw field.invalid("is " + text + ", and must be TOPIC, GROUP, TRANSACTIONAL_ID or CLUSTER");
    }

    private static PatternType patternType(Json field) throws Json.Invalid {
        String text = field.text();
        for (PatternType type : List.of(PatternType.LITERAL, PatternType.PREFIXED)) {
            if (type.name().equals(text)) {
                return type;
            }
        }
        throw field.invalid("is " + text + ", and must be LITERAL or PREFIXED");
    }
}
