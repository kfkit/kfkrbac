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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * The command: role bindings and groups in an operator's own terms, the
 * answer to "who can do this" and "what may this principal do", and
 * bindings kept in a file or brought over from one.
 *
 * <p>Run it with the distribution's
 * {@code kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand <verb> ...}, with
 * {@code --bootstrap-server} and {@code --command-config} as the ACL tool
 * has them. It reads and writes ACL records through the admin client and
 * nothing else, so it needs no access the ACL tool would not need, and the
 * cluster refuses what the caller may not do. Everything it does can be done
 * with the ACL tool: it is a convenience, never the only way.
 *
 * <p>Exit status: 0 when done, 1 when the cluster refused or the call
 * failed, 2 for wrong usage and for a file that cannot be read as what it
 * must be.
 */
public final class RbacCommand {

    /** Exit status: done. */
    static final int DONE = 0;
    /** Exit status: the cluster refused, or the call failed. */
    static final int FAILED = 1;
    /** Exit status: the command was not typed the way it is used. */
    static final int WRONG_USAGE = 2;

    private static final String RUN = "kafka-run-class.sh " + RbacCommand.class.getName();

    private static final String BOOTSTRAP_SERVER = "--bootstrap-server";
    private static final String COMMAND_CONFIG = "--command-config";
    private static final String HELP = "--help";

    /** The first word of the verbs that manage groups. */
    private static final String GROUP = "group";

    private static final String CONNECTION = String.join(System.lineSeparator(),
            "  --bootstrap-server <host:port,...>  the cluster to connect to; required",
            "  --command-config <file>             properties for the admin client: the security settings",
            "  --help                              print this and do nothing");

    /** A verb: its word, the options it takes besides those of the connection, and how it is used. */
    enum Verb {
        BIND("bind", "bind a role to a member",
                Set.of("--role", "--member", "--prefix", "--name"),
                Set.of("--group-only", "--transactional-id-only", "--subject", "--connector"),
                "--role <role> --member <type:name> [--prefix <prefix> | --name <name>]",
                "[--group-only | --transactional-id-only | --subject | --connector]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Binds a role to a member: the member may do what the role grants on what the binding",
                        "names. Binding what is bound already changes nothing.",
                        "",
                        "  --role <role>              on a name pattern: " + roles(Role.Scope.PATTERN),
                        "                             on the cluster, with no pattern: " + roles(Role.Scope.CLUSTER),
                        "  --member <type:name>       who gets the role: User:alice",
                        "  --prefix <prefix>          every name that starts with the prefix",
                        "  --name <name>              that one name; * is every name",
                        "                             a pattern covers topics, groups and transactional IDs, unless:",
                        "  --group-only               groups only",
                        "  --transactional-id-only    transactional IDs only",
                        "  --subject                  subjects of a schema registry, and no broker resource",
                        "  --connector                connectors, and no broker resource");
            }
        },
        UNBIND("unbind", "remove one binding",
                BIND.valued, BIND.flags, BIND.synopsis) {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Removes exactly the binding that bind with the same options creates, and nothing else.",
                        "Fails when there is no such binding.",
                        "",
                        "  --role, --member, --prefix, --name, --group-only, --transactional-id-only, --subject,",
                        "  --connector                as for bind");
            }
        },
        LIST("list", "list the bindings",
                Set.of("--role", "--member", "--prefix", "--name", "--output"), Set.of(),
                "[--role <role>] [--member <type:name>] [--prefix <prefix> | --name <name>]",
                "[--output text|json]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Lists the role bindings, one on a line. Changes nothing.",
                        "",
                        "  --role <role>              only bindings of that role",
                        "  --member <type:name>       only bindings of that member",
                        "  --prefix <prefix>          only bindings on exactly that prefix",
                        "  --name <name>              only bindings on exactly that name",
                        "  --output text|json         json: an array with one object for each binding, with role,",
                        "                             member, resourceType, name and patternType as its record",
                        "                             has them; text by default");
            }
        },
        GROUP_ADD("group add", "put a member in a group",
                Set.of("--group", "--member"), Set.of(),
                "--group <name> --member <type:name>") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Puts a member in a group: it then holds the roles bound to the group, and the ALLOW ACLs",
                        "that name the group apply to it. A group is not created: it is there for as long as it",
                        "has a member. Adding who is in it already changes nothing.",
                        "",
                        "  --group <name>             the name of the group, without Group: in front: payments",
                        "  --member <type:name>       who joins it: User:alice; a group is not a member of a group");
            }
        },
        GROUP_REMOVE("group remove", "take a member out of a group",
                GROUP_ADD.valued, GROUP_ADD.flags, GROUP_ADD.synopsis) {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Takes a member out of a group, and nothing else: the bindings and the ACLs of the group",
                        "stay. Fails when the member is not in the group.",
                        "",
                        "  --group <name>             the name of the group, without Group: in front: payments",
                        "  --member <type:name>       who leaves it: User:alice");
            }
        },
        GROUP_LIST("group list", "list the groups and their members",
                Set.of("--group", "--output"), Set.of(),
                "[--group <name>] [--output text|json]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Lists the members of the groups, one membership on a line. Changes nothing.",
                        "",
                        "  --group <name>             only the members of that group",
                        "  --output text|json         json: an array with one object for each membership, with",
                        "                             group and member; text by default");
            }
        },
        WHO_CAN("who-can", "who may do an operation on a resource, and why",
                Set.of("--operation", "--topic", "--group", "--transactional-id", "--host"), Set.of("--cluster"),
                "--operation <operation>",
                "(--topic <name> | --group <name> | --transactional-id <name> | --cluster) [--host <address>]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Names every principal a role or an ACL lets do the operation on the resource, with the",
                        "binding or the ACL that lets it, and every principal a deny ACL stops, with that ACL.",
                        "A member of a group is named for what the group is given, with the group.",
                        "The answer is the one the authorizer gives for the ACL records of the cluster; what the",
                        "brokers' own settings allow (super.users, allow.everyone.if.no.acl.found) is not in it.",
                        "Changes nothing.",
                        "",
                        "  --operation <operation>    " + Wording.list(Role.everyAclOperation()),
                        "  --topic <name>             a topic, by its name",
                        "  --group <name>             a group, by its name",
                        "  --transactional-id <name>  a transactional ID",
                        "  --cluster                  the cluster",
                        "  --host <address>           the address the request comes from, for ACLs that name a",
                        "                             host; by default an address no ACL names");
            }
        },
        WHAT_CAN("what-can", "what a principal may do, and why",
                Set.of("--principal", "--host"), Set.of(),
                "--principal <type:name> [--host <address>]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Lists what the principal may do, itself and as a member of its groups, with the binding",
                        "or the ACL behind each line, and what a deny ACL takes away from it. What the brokers'",
                        "own settings allow (super.users, allow.everyone.if.no.acl.found) is not in it. Changes",
                        "nothing.",
                        "",
                        "  --principal <type:name>    the principal: User:alice; User:* is what everyone may do,",
                        "                             Group:payments what that group gives its members",
                        "  --host <address>           the address the request comes from, for ACLs that name a",
                        "                             host; by default an address no ACL names");
            }
        },
        APPLY("apply", "make the bindings and the groups of the cluster those of a file",
                Set.of("--file"), Set.of("--dry-run", "--no-delete"),
                "--file <file> [--dry-run] [--no-delete]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Compares a file of bindings and groups with the cluster, prints the difference and makes",
                        "the cluster match: creates what is in the file and not in the cluster, and deletes the",
                        "bindings and the memberships that are in the cluster and not in the file. Ordinary ACLs",
                        "are never touched.",
                        "",
                        "  --file <file>              JSON: {\"bindings\": [...], \"groups\": [...]}, where bindings",
                        "                             is what list --output json prints and groups what group list",
                        "                             --output json prints; or the array of list --output json",
                        "                             alone. Bindings or groups that the file does not have at all",
                        "                             are left as they are",
                        "  --dry-run                  print the difference and change nothing",
                        "  --no-delete                only create: delete nothing");
            }
        },
        IMPORT("import", "write the bindings of a file that has another shape",
                Set.of("--file", "--format", "--cluster-id"), Set.of("--dry-run"),
                "--file <file> --format role-bindings|role-acls [--cluster-id <id>] [--dry-run]") {
            @Override
            String explanation() {
                return String.join(System.lineSeparator(),
                        "Reads a file in one of two shapes and writes what it holds; the fields of both are in",
                        "docs/import.md. What cannot be written as the file has it is skipped and reported, never",
                        "guessed. Writing what is there already changes nothing.",
                        "",
                        "  --file <file>              the JSON file",
                        "  --format role-bindings     a list of bindings of named roles: written as bindings",
                        "  --format role-acls         a list of roles that are sets of ACLs: each is written as a",
                        "                             group with the role's members and ALLOW ACLs of the group",
                        "  --cluster-id <id>          for role-bindings: only the bindings for that Kafka cluster,",
                        "                             and those that name none",
                        "  --dry-run                  print what would be written and what is skipped, and change",
                        "                             nothing");
            }
        };

        final String word;
        final String summary;
        final Set<String> valued;
        final Set<String> flags;
        final String[] synopsis;

        Verb(String word, String summary, Set<String> valued, Set<String> flags, String... synopsis) {
            this.word = word;
            this.summary = summary;
            this.valued = valued;
            this.flags = flags;
            this.synopsis = synopsis;
        }

        /** What the verb does and what each of its options means. */
        abstract String explanation();

        /** How the verb is used: what wrong usage prints. */
        String usage() {
            String start = "Usage: " + RUN + " " + word;
            StringBuilder usage = new StringBuilder(start)
                    .append(" --bootstrap-server <host:port> [--command-config <file>]");
            for (String line : synopsis) {
                usage.append(System.lineSeparator()).append("           ").append(line);
            }
            return usage.append(System.lineSeparator()).append(System.lineSeparator())
                    .append(explanation()).append(System.lineSeparator())
                    .append(CONNECTION).append(System.lineSeparator()).toString();
        }

        /** The verb of that word, or of those two: {@code list}, {@code group add}. */
        static Verb of(String word) {
            for (Verb verb : values()) {
                if (verb.word.equals(word)) {
                    return verb;
                }
            }
            return null;
        }

        /** How many words the verb is: {@code group add} is two. */
        int words() {
            return word.split(" ").length;
        }

        private static String roles(Role.Scope scope) {
            List<String> names = new ArrayList<>();
            for (Role role : Role.values()) {
                if (role.scope() == scope) {
                    names.add(role.roleName());
                }
            }
            return String.join(", ", names);
        }
    }

    /** What a verb does once its options are understood and the cluster is reached. */
    interface Task {
        int run(AclRecords records, PrintStream out, PrintStream err);
    }

    private RbacCommand() {
    }

    /** Runs the command and exits with its status. */
    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, properties -> new AdminAclRecords(Admin.create(properties))));
    }

    /**
     * Runs the command.
     *
     * @param cluster reaches the ACL records of the cluster the admin client's properties name
     * @return the exit status
     */
    static int run(String[] args, PrintStream out, PrintStream err, Function<Properties, AclRecords> cluster) {
        if (args.length == 0) {
            err.print(help());
            return WRONG_USAGE;
        }
        if (HELP.equals(args[0]) || "help".equals(args[0])) {
            out.print(help());
            return DONE;
        }
        if (GROUP.equals(args[0]) && (args.length == 1 || Verb.of(GROUP + " " + args[1]) == null)) {
            boolean asked = args.length > 1 && HELP.equals(args[1]);
            PrintStream to = asked ? out : err;
            if (!asked) {
                err.println(GROUP + " is followed by add, remove or list"
                        + (args.length == 1 ? "" : ", not by " + args[1]));
                err.println();
            }
            to.print(Verb.GROUP_ADD.usage());
            to.println();
            to.print(Verb.GROUP_REMOVE.usage());
            to.println();
            to.print(Verb.GROUP_LIST.usage());
            return asked ? DONE : WRONG_USAGE;
        }
        Verb verb = Verb.of(GROUP.equals(args[0]) ? GROUP + " " + args[1] : args[0]);
        if (verb == null) {
            err.println("Unknown verb: " + args[0]);
            err.println();
            err.print(help());
            return WRONG_USAGE;
        }
        Task task;
        Properties properties;
        try {
            Set<String> valued = new HashSet<>(verb.valued);
            valued.add(BOOTSTRAP_SERVER);
            valued.add(COMMAND_CONFIG);
            Set<String> flags = new HashSet<>(verb.flags);
            flags.add(HELP);
            Arguments arguments =
                    Arguments.parse(Arrays.asList(args).subList(verb.words(), args.length), valued, flags);
            if (arguments.has(HELP)) {
                out.print(verb.usage());
                return DONE;
            }
            task = task(verb, arguments);
            properties = properties(arguments);
        } catch (Arguments.WrongUsage e) {
            err.println(e.getMessage());
            err.println();
            err.print(verb.usage());
            return WRONG_USAGE;
        } catch (Json.Invalid e) {
            // The file says where it is wrong; how the verb is used is not what is wrong.
            err.println("error: " + e.getMessage());
            return WRONG_USAGE;
        }
        try (AclRecords records = cluster.apply(properties)) {
            return task.run(records, out, err);
        } catch (RuntimeException e) {
            // A refusal carries the reason the cluster gave; anything else says what it can.
            String reason = e.getMessage() == null ? e.toString() : e.getMessage();
            Throwable cause = e instanceof AclRecords.Failure ? null : e.getCause();
            err.println("error: " + reason
                    + (cause == null || cause.getMessage() == null ? "" : ": " + cause.getMessage()));
            return FAILED;
        }
    }

    /** Every verb and every option. */
    static String help() {
        StringBuilder help = new StringBuilder(
                "Role bindings for Apache Kafka: bind and list them, manage groups, ask who can do what, and")
                .append(System.lineSeparator())
                .append("keep bindings in a file or bring them over from one.")
                .append(System.lineSeparator()).append(System.lineSeparator())
                .append("Usage: ").append(RUN).append(" <verb> --bootstrap-server <host:port> [options]")
                .append(System.lineSeparator()).append(System.lineSeparator())
                .append("Verbs:").append(System.lineSeparator());
        for (Verb verb : Verb.values()) {
            help.append(String.format("  %-13s %s%n", verb.word, verb.summary));
        }
        help.append(System.lineSeparator())
                .append("Exit status: 0 done, 1 refused by the cluster or failed, 2 wrong usage or a file that")
                .append(System.lineSeparator())
                .append("cannot be read as what it must be.")
                .append(System.lineSeparator());
        for (Verb verb : Verb.values()) {
            help.append(System.lineSeparator()).append(verb.usage());
        }
        return help.toString();
    }

    private static Task task(Verb verb, Arguments arguments) throws Arguments.WrongUsage, Json.Invalid {
        switch (verb) {
            case BIND:
                return bind(arguments);
            case UNBIND:
                return unbind(arguments);
            case LIST:
                return list(arguments);
            case GROUP_ADD:
                return groupAdd(arguments);
            case GROUP_REMOVE:
                return groupRemove(arguments);
            case GROUP_LIST:
                return groupList(arguments);
            case WHO_CAN:
                return whoCan(arguments);
            case WHAT_CAN:
                return whatCan(arguments);
            case APPLY:
                return apply(arguments);
            default:
                return importFrom(arguments);
        }
    }

    private static Properties properties(Arguments arguments) throws Arguments.WrongUsage {
        Properties properties = new Properties();
        String file = arguments.value(COMMAND_CONFIG).orElse(null);
        if (file != null) {
            try (InputStream in = Files.newInputStream(Path.of(file))) {
                properties.load(in);
            } catch (IOException | IllegalArgumentException e) {
                throw new Arguments.WrongUsage("Cannot read " + COMMAND_CONFIG + " " + file + ": " + e.getMessage());
            }
        }
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, arguments.required(BOOTSTRAP_SERVER));
        return properties;
    }

    // --- bind, unbind ----------------------------------------------------------

    private static Task bind(Arguments arguments) throws Arguments.WrongUsage {
        AclBinding record = bindingRecord(arguments);
        return (records, out, err) -> {
            records.create(List.of(record));
            out.println("Bound " + Wording.binding(binding(record)));
            return DONE;
        };
    }

    private static Task unbind(Arguments arguments) throws Arguments.WrongUsage {
        AclBinding record = bindingRecord(arguments);
        return (records, out, err) -> {
            Collection<AclBinding> deleted = records.delete(List.of(record.toFilter()));
            if (deleted.isEmpty()) {
                err.println("error: no such binding: " + Wording.binding(binding(record)));
                return FAILED;
            }
            out.println("Unbound " + Wording.binding(binding(record)));
            return DONE;
        };
    }

    /**
     * The record of the binding the options describe. The codec that reads
     * records says whether it is a binding the format allows, and it must
     * read it back as what was asked for.
     */
    private static AclBinding bindingRecord(Arguments arguments) throws Arguments.WrongUsage {
        String role = arguments.required("--role");
        String member = arguments.required("--member");
        Role named = Role.named(role).orElseThrow(() -> new Arguments.WrongUsage(
                "There is no role " + role + ": the roles are " + String.join(", ", Role.NAMES)));
        List<String> patterns = arguments.given("--prefix", "--name");
        List<String> kinds = arguments.given("--group-only", "--transactional-id-only", "--subject", "--connector");
        if (patterns.size() > 1) {
            throw new Arguments.WrongUsage("Give --prefix or --name, not both");
        }
        if (kinds.size() > 1) {
            throw new Arguments.WrongUsage("Give one of " + String.join(", ", kinds) + ", not " + kinds.size());
        }
        RoleBinding.Target target;
        String name;
        PatternType patternType;
        if (patterns.isEmpty()) {
            if (!kinds.isEmpty()) {
                throw new Arguments.WrongUsage(kinds.get(0) + " narrows a pattern: give --prefix or --name");
            }
            if (named.scope() == Role.Scope.PATTERN) {
                throw new Arguments.WrongUsage(named + " is bound to a name pattern: give --prefix or --name");
            }
            target = RoleBinding.Target.CLUSTER;
            name = Resource.CLUSTER_NAME;
            patternType = PatternType.LITERAL;
        } else {
            target = kinds.isEmpty() ? RoleBinding.Target.NAMES
                    : arguments.has("--group-only") ? RoleBinding.Target.GROUPS
                    : arguments.has("--transactional-id-only") ? RoleBinding.Target.TRANSACTIONAL_IDS
                    : arguments.has("--subject") ? RoleBinding.Target.SUBJECTS
                    : RoleBinding.Target.CONNECTORS;
            name = arguments.required(patterns.get(0));
            patternType = "--prefix".equals(patterns.get(0)) ? PatternType.PREFIXED : PatternType.LITERAL;
        }
        AclBinding record = BindingRecords.record(role, member, target, name, patternType);
        BindingCodec.Decoded decoded = BindingCodec.decode(record);
        if (decoded instanceof BindingCodec.Malformed malformed) {
            throw new Arguments.WrongUsage("Not a binding: " + malformed.reason());
        }
        if (!(decoded instanceof RoleBinding binding) || binding.target() != target || !binding.name().equals(name)
                || binding.patternType() != patternType) {
            throw new Arguments.WrongUsage("Not the binding asked for: " + patterns.get(0) + " " + name
                    + " would be read as " + (decoded instanceof RoleBinding read ? Wording.on(read) : "an ACL")
                    + ": a name that starts with " + BindingCodec.SUBJECT_PREFIX + " or "
                    + BindingCodec.CONNECTOR_PREFIX + " is given without it, with --subject or --connector");
        }
        return record;
    }

    /** The binding a record stands for; the record was read back when it was made. */
    private static RoleBinding binding(AclBinding record) {
        return (RoleBinding) BindingCodec.decode(record);
    }

    // --- list ------------------------------------------------------------------

    private static Task list(Arguments arguments) throws Arguments.WrongUsage {
        String role = arguments.value("--role").orElse(null);
        if (role != null && Role.named(role).isEmpty()) {
            throw new Arguments.WrongUsage("There is no role " + role + ": the roles are "
                    + String.join(", ", Role.NAMES));
        }
        String member = arguments.value("--member").orElse(null);
        if (arguments.given("--prefix", "--name").size() > 1) {
            throw new Arguments.WrongUsage("Give --prefix or --name, not both");
        }
        String prefix = arguments.value("--prefix").orElse(null);
        String name = arguments.value("--name").orElse(null);
        boolean json = json(arguments);
        return (records, out, err) -> {
            List<AclBinding> all = everyRecord(records, err);
            List<String[]> rows = new ArrayList<>();
            List<String> objects = new ArrayList<>();
            for (AclBinding record : all) {
                if (!(BindingCodec.decode(record) instanceof RoleBinding binding)
                        || role != null && !binding.role().roleName().equals(role)
                        || member != null && !binding.member().toString().equals(member)
                        || prefix != null
                                && !(binding.patternType() == PatternType.PREFIXED && binding.name().equals(prefix))
                        || name != null
                                && !(binding.patternType() == PatternType.LITERAL && binding.name().equals(name)
                                        && binding.target() != RoleBinding.Target.CLUSTER)) {
                    continue;
                }
                rows.add(new String[] {binding.role().roleName(), binding.member().toString(), Wording.on(binding)});
                objects.add(Wording.json(record, binding));
            }
            if (json) {
                array(objects, out);
            } else if (rows.isEmpty()) {
                out.println("No bindings.");
            } else {
                rows.add(0, new String[] {"ROLE", "MEMBER", "ON"});
                table(rows, "", out);
            }
            return DONE;
        };
    }

    // --- group add, group remove, group list -------------------------------------

    private static Task groupAdd(Arguments arguments) throws Arguments.WrongUsage {
        AclBinding record = membershipRecord(arguments);
        return (records, out, err) -> {
            records.create(List.of(record));
            BindingCodec.Membership membership = membership(record);
            out.println("Added " + membership.member() + " to " + membership.group());
            return DONE;
        };
    }

    private static Task groupRemove(Arguments arguments) throws Arguments.WrongUsage {
        AclBinding record = membershipRecord(arguments);
        return (records, out, err) -> {
            BindingCodec.Membership membership = membership(record);
            Collection<AclBinding> deleted = records.delete(List.of(record.toFilter()));
            if (deleted.isEmpty()) {
                err.println("error: no such membership: " + Wording.membership(membership));
                return FAILED;
            }
            out.println("Removed " + membership.member() + " from " + membership.group());
            return DONE;
        };
    }

    private static Task groupList(Arguments arguments) throws Arguments.WrongUsage {
        String group = arguments.value("--group").orElse(null);
        if (group != null) {
            groupName(group);
        }
        boolean json = json(arguments);
        return (records, out, err) -> {
            List<String[]> rows = new ArrayList<>();
            List<String> objects = new ArrayList<>();
            for (AclBinding record : everyRecord(records, err)) {
                if (BindingCodec.decode(record) instanceof BindingCodec.Membership membership
                        && (group == null || membership.group().getName().equals(group))) {
                    rows.add(new String[] {membership.group().getName(), membership.member().toString()});
                    objects.add(Wording.json(membership));
                }
            }
            if (json) {
                array(objects, out);
            } else if (rows.isEmpty()) {
                out.println(group == null ? "No groups."
                        : BindingCodec.GROUP_PREFIX + group + " has no members.");
            } else {
                rows.add(0, new String[] {"GROUP", "MEMBER"});
                table(rows, "", out);
            }
            return DONE;
        };
    }

    /**
     * The record of the membership the options describe. The codec that
     * reads records says whether it is a membership the format allows, and
     * it must read it back as what was asked for.
     */
    private static AclBinding membershipRecord(Arguments arguments) throws Arguments.WrongUsage {
        String group = groupName(arguments.required("--group"));
        String member = arguments.required("--member");
        AclBinding record = BindingRecords.membership(group, member);
        BindingCodec.Decoded decoded = BindingCodec.decode(record);
        if (decoded instanceof BindingCodec.Malformed malformed) {
            throw new Arguments.WrongUsage("Not a membership: " + malformed.reason());
        }
        if (!(decoded instanceof BindingCodec.Membership membership) || !membership.group().getName().equals(group)
                || !membership.member().toString().equals(member)) {
            throw new Arguments.WrongUsage("Not the membership asked for: " + member + " in the group " + group);
        }
        return record;
    }

    /** The name of a group as {@code --group} takes it: the name alone. */
    private static String groupName(String group) throws Arguments.WrongUsage {
        if (group.isEmpty()) {
            throw new Arguments.WrongUsage("The name of the group is empty");
        }
        if (group.startsWith(BindingCodec.GROUP_PREFIX)) {
            throw new Arguments.WrongUsage("--group is the name of the group, without " + BindingCodec.GROUP_PREFIX
                    + " in front: " + group.substring(BindingCodec.GROUP_PREFIX.length()));
        }
        return group;
    }

    /** The membership a record stands for; the record was read back when it was made. */
    private static BindingCodec.Membership membership(AclBinding record) {
        return (BindingCodec.Membership) BindingCodec.decode(record);
    }

    // --- who-can, what-can -----------------------------------------------------

    private static Task whoCan(Arguments arguments) throws Arguments.WrongUsage {
        AclOperation operation = operation(arguments.required("--operation"));
        List<String> resources = arguments.given("--topic", "--group", "--transactional-id", "--cluster");
        if (resources.size() != 1) {
            throw new Arguments.WrongUsage("Give one of --topic, --group, --transactional-id, --cluster");
        }
        ResourcePattern resource;
        switch (resources.get(0)) {
            case "--topic":
                resource = named(ResourceType.TOPIC, arguments.required("--topic"));
                break;
            case "--group":
                resource = named(ResourceType.GROUP, arguments.required("--group"));
                break;
            case "--transactional-id":
                resource = named(ResourceType.TRANSACTIONAL_ID, arguments.required("--transactional-id"));
                break;
            default:
                resource = named(ResourceType.CLUSTER, Resource.CLUSTER_NAME);
                break;
        }
        InetAddress host = host(arguments);
        return (records, out, err) -> {
            try (Access access = new Access(everyRecord(records, err), host)) {
                List<Access.Answer> answers = access.whoCan(operation, resource);
                out.println(Wording.operation(operation) + " on " + Wording.resources(resource) + ":");
                out.println();
                if (answers.isEmpty()) {
                    out.println("Nobody, by the roles and the ACLs.");
                }
                for (Access.Answer answer : answers) {
                    out.println(answer.principal() + "  " + (answer.allowed() ? "allowed" : "denied"));
                    for (Access.Reason by : answer.deniedBy()) {
                        out.println("    denied by " + reason(by.record(), by.through()));
                    }
                    for (Access.Reason by : answer.allowedBy()) {
                        out.println((answer.allowed() ? "    allowed by " : "    without that, allowed by ")
                                + reason(by.record(), by.through()));
                    }
                }
                notes(access, out);
            }
            return DONE;
        };
    }

    private static Task whatCan(Arguments arguments) throws Arguments.WrongUsage {
        String text = arguments.required("--principal");
        KafkaPrincipal principal = Wording.principal(text);
        if (principal == null) {
            throw new Arguments.WrongUsage("--principal is <type>:<name>, as in User:alice, not " + text);
        }
        InetAddress host = host(arguments);
        return (records, out, err) -> {
            try (Access access = new Access(everyRecord(records, err), host)) {
                List<Access.Line> granted = access.granted(principal);
                List<Access.Line> denied = new ArrayList<>();
                List<Access.Line> deniedAsAMember = new ArrayList<>();
                for (Access.Line line : access.denied(principal)) {
                    (line.through() == null ? denied : deniedAsAMember).add(line);
                }
                boolean group = BindingCodec.GROUP_TYPE.equals(principal.getPrincipalType());
                String who = group ? "A member of " + principal : principal.toString();
                if (group) {
                    List<KafkaPrincipal> members = access.membersOf(principal);
                    out.println(members.isEmpty() ? principal + " has no members."
                            : principal + " has the members " + names(members) + ".");
                } else if (!access.groupsOf(principal).isEmpty()) {
                    out.println(principal + " is in " + names(access.groupsOf(principal)) + ".");
                }
                if (granted.isEmpty()) {
                    out.println(who + " may do nothing, by the roles and the ACLs.");
                } else {
                    out.println(who + " may:");
                    table(rows(granted), "  ", out);
                }
                if (!denied.isEmpty()) {
                    out.println(granted.isEmpty() ? "It is denied:" : "but not, whatever allows it:");
                    table(rows(denied), "  ", out);
                }
                if (!deniedAsAMember.isEmpty()) {
                    out.println("and not by a role or a group, only by an ACL that names " + principal + ":");
                    table(rows(deniedAsAMember), "  ", out);
                }
                notes(access, out);
            }
            return DONE;
        };
    }

    private static List<String[]> rows(List<Access.Line> lines) {
        List<String[]> rows = new ArrayList<>();
        for (Access.Line line : lines) {
            rows.add(new String[] {line.on(), line.operations(), "by " + reason(line.by(), line.through())});
        }
        return rows;
    }

    /** The record behind a line, and the group through which it applies when it names one. */
    private static String reason(AclBinding record, KafkaPrincipal through) {
        return Wording.record(record) + (through == null ? "" : ", as a member of " + through);
    }

    private static String names(List<KafkaPrincipal> principals) {
        List<String> names = new ArrayList<>();
        for (KafkaPrincipal principal : principals) {
            names.add(principal.toString());
        }
        return String.join(", ", names);
    }

    /** What the answer leaves out, and what it depends on. */
    private static void notes(Access access, PrintStream out) {
        out.println();
        if (access.dependsOnHost()) {
            out.println("Some ACLs name a host: asked as from " + access.host().getHostAddress()
                    + "; --host asks as from another address.");
        }
        out.println("Not counted: what the brokers' own settings allow (super.users,"
                + " allow.everyone.if.no.acl.found).");
    }

    private static AclOperation operation(String text) throws Arguments.WrongUsage {
        String wanted = text.replace("_", "");
        for (AclOperation operation : Role.everyAclOperation()) {
            if (operation.name().replace("_", "").equalsIgnoreCase(wanted)) {
                return operation;
            }
        }
        throw new Arguments.WrongUsage("There is no operation " + text + ": the operations are "
                + Wording.list(Role.everyAclOperation()));
    }

    private static ResourcePattern named(ResourceType type, String name) throws Arguments.WrongUsage {
        if (name.isEmpty()) {
            throw new Arguments.WrongUsage("The name of the " + Wording.type(type) + " is empty");
        }
        return new ResourcePattern(type, name, PatternType.LITERAL);
    }

    private static InetAddress host(Arguments arguments) throws Arguments.WrongUsage {
        String host = arguments.value("--host").orElse(null);
        if (host == null) {
            return null;
        }
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new Arguments.WrongUsage("--host is an address, not " + host);
        }
    }

    // --- apply, import ---------------------------------------------------------

    private static Task apply(Arguments arguments) throws Arguments.WrongUsage, Json.Invalid {
        Path file = file(arguments);
        boolean dryRun = arguments.has("--dry-run");
        boolean noDelete = arguments.has("--no-delete");
        Apply.Wanted wanted = Apply.read(file);
        return (records, out, err) -> Apply.run(wanted, dryRun, noDelete, records, out, err);
    }

    private static Task importFrom(Arguments arguments) throws Arguments.WrongUsage, Json.Invalid {
        Path file = file(arguments);
        String format = arguments.required("--format");
        String clusterId = arguments.value("--cluster-id").orElse(null);
        boolean dryRun = arguments.has("--dry-run");
        Import.Plan plan;
        switch (format) {
            case "role-bindings":
                plan = Import.roleBindings(file, clusterId);
                break;
            case "role-acls":
                if (clusterId != null) {
                    throw new Arguments.WrongUsage("--cluster-id selects among the bindings of --format role-bindings");
                }
                plan = Import.roleAcls(file);
                break;
            default:
                throw new Arguments.WrongUsage("--format is role-bindings or role-acls, not " + format);
        }
        return (records, out, err) -> Import.run(plan, dryRun, records, out);
    }

    /**
     * The file a verb reads. Asked for after the cluster to connect to, so
     * that a command without one says so before anything is read.
     */
    private static Path file(Arguments arguments) throws Arguments.WrongUsage {
        String file = arguments.required("--file");
        arguments.required(BOOTSTRAP_SERVER);
        try {
            return Path.of(file);
        } catch (InvalidPathException e) {
            throw new Arguments.WrongUsage("--file is not the path of a file: " + file);
        }
    }

    // --- shared ----------------------------------------------------------------

    /** Whether {@code --output} asks for JSON. */
    private static boolean json(Arguments arguments) throws Arguments.WrongUsage {
        String output = arguments.value("--output").orElse("text");
        if (!"text".equals(output) && !"json".equals(output)) {
            throw new Arguments.WrongUsage("--output is text or json, not " + output);
        }
        return "json".equals(output);
    }

    /** Prints JSON objects as one array, one object on a line. */
    private static void array(List<String> objects, PrintStream out) {
        out.println(objects.isEmpty() ? "[]"
                : "[" + System.lineSeparator() + "  "
                        + String.join("," + System.lineSeparator() + "  ", objects)
                        + System.lineSeparator() + "]");
    }

    /**
     * Every ACL record of the cluster, in a fixed order. A record that has
     * a role as its principal type and is not a binding grants nothing, and
     * that is said: it is easy to take for one. So it is of a record that
     * is not a group membership, and of a deny ACL of a group.
     */
    static List<AclBinding> everyRecord(AclRecords records, PrintStream err) {
        List<AclBinding> all = new ArrayList<>(records.describe(AclBindingFilter.ANY));
        all.sort(BindingRecords.ORDER);
        for (AclBinding record : all) {
            if (BindingCodec.decode(record) instanceof BindingCodec.Malformed malformed) {
                err.println("warning: " + notWhatItSays(malformed.kind()) + ": " + record.entry().principal()
                        + " on " + Wording.resources(record.pattern()) + ": " + malformed.reason());
            }
        }
        return all;
    }

    /** What a record is that is not what its principal says, and what becomes of it. */
    private static String notWhatItSays(BindingCodec.Malformed.Kind kind) {
        switch (kind) {
            case MEMBERSHIP:
                return "not a group membership, puts nobody in a group";
            case GROUP_DENY:
                return "a DENY for a group, takes away only what roles and groups give its members";
            default:
                return "not a binding, grants nothing";
        }
    }

    /** Prints rows in columns as wide as their widest cell; the last column is as long as it is. */
    private static void table(List<String[]> rows, String indent, PrintStream out) {
        int columns = rows.get(0).length;
        int[] width = new int[columns];
        for (String[] row : rows) {
            for (int i = 0; i < columns - 1; i++) {
                width[i] = Math.max(width[i], row[i].length());
            }
        }
        for (String[] row : rows) {
            StringBuilder line = new StringBuilder(indent);
            for (int i = 0; i < columns; i++) {
                line.append(row[i]);
                if (i < columns - 1) {
                    line.append(" ".repeat(width[i] - row[i].length() + 2));
                }
            }
            out.println(line);
        }
    }
}
