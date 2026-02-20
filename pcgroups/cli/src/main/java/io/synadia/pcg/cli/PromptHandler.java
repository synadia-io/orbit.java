// Copyright 2024-2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.synadia.pcg.cli;

import io.nats.client.Connection;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.synadia.pcg.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Shared interactive prompt handler for both static and elastic modes.
 * Mirrors the Go prompt() function behavior.
 */
class PromptHandler {

    private boolean isStatic;
    private final String context;
    private final Connection nc;
    private Duration processingDuration = Duration.ofMillis(20);
    private boolean consuming = false;
    private String currentStream;
    private String currentGroup;
    private String currentMember;

    PromptHandler(boolean isStatic, String context, Connection nc) {
        this.isStatic = isStatic;
        this.context = context;
        this.nc = nc;
    }

    int run() {
        System.out.println("Interactive prompt mode - type 'help' for commands, 'exit' to quit");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            try {
                if (isStatic) {
                    System.out.print("[static]");
                } else {
                    System.out.print("[elastic]");
                }
                if (consuming) {
                    System.out.printf("[%s/%s/%s]> ", currentStream, currentGroup, currentMember);
                } else {
                    System.out.print("> ");
                }

                String line = reader.readLine();
                if (line == null) break;

                line = line.trim();
                if (line.isEmpty()) continue;

                String command;
                String argsString = null;
                String[] args = null;
                int spaceIndex = line.indexOf(' ');
                if (spaceIndex >= 0) {
                    command = line.substring(0, spaceIndex);
                    argsString = line.substring(spaceIndex + 1).trim();
                    args = argsString.split("\\s+");
                } else {
                    command = line;
                    args = new String[0];
                }

                switch (command) {
                    case "exit":
                    case "quit":
                        System.out.println("Exiting...");
                        return 0;
                    case "help":
                    case "?":
                        printHelp();
                        break;
                    case "static":
                        isStatic = true;
                        break;
                    case "elastic":
                        isStatic = false;
                        break;
                    case "processing-time":
                        handleProcessingTime(reader, args, argsString);
                        break;
                    case "list":
                    case "ls":
                        handleList(reader, args, argsString);
                        break;
                    case "info":
                        handleInfo(reader, args);
                        break;
                    case "create":
                        handleCreate(reader);
                        break;
                    case "delete":
                    case "rm":
                        handleDelete(reader, args);
                        break;
                    case "add":
                        handleAdd(reader, args);
                        break;
                    case "drop":
                        handleDrop(reader, args);
                        break;
                    case "createmapping":
                    case "create-mapping":
                    case "cm":
                        handleCreateMapping(reader, args);
                        break;
                    case "deletemapping":
                    case "delete-mapping":
                    case "dm":
                        handleDeleteMapping(reader, args);
                        break;
                    case "memberinfo":
                    case "member-info":
                    case "minfo":
                        handleMemberInfo(reader, args);
                        break;
                    case "stepdown":
                    case "step-down":
                    case "sd":
                        handleStepDown(reader, args);
                        break;
                    case "consume":
                    case "join":
                        handleConsume(reader, args);
                        break;
                    default:
                        System.out.println("Unknown command: " + command + ". Type 'help' for available commands.");
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
        return 0;
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  exit/quit - exit the program");
        System.out.println("  list/ls <stream name> - list partitioned consumer groups");
        System.out.println("  info <stream name> <partitioned consumer group name> - get partitioned consumer group info");
        System.out.println("  create <stream name> <partitioned consumer group name> <max members> <filter> <comma separated partitioning wildcard indexes> - create a partitioned consumer group");
        System.out.println("  delete/rm <stream name> <partitioned consumer group name> - delete a partitioned consumer group");
        System.out.println("  memberinfo/minfo <stream name> <partitioned consumer group name> <member name> - get partitioned consumer group member info");
        System.out.println("  add <stream name> <partitioned consumer group name> <member name> [...] - add a member to a partitioned consumer group");
        System.out.println("  drop <stream name> <partitioned consumer group name> <member name> [...] - remove a member from a partitioned consumer group");
        System.out.println("  deletemapping <stream name> <partitioned consumer group name> - delete all member mappings for a partitioned consumer group");
        System.out.println("  createmapping <stream name> <partitioned consumer group name> - create member mappings for a partitioned consumer group");
        System.out.println("  stepdown/sd <stream name> <partitioned consumer group name> <member name> - initiate a step down for a member");
        System.out.println("  consume/join <stream name> <partitioned consumer group name> <member name> - join a partitioned consumer group");
        System.out.println("  processing-time <duration> - set message processing time (e.g., 20ms, 5s, 1m)");
        System.out.println("  static - static consumer groups mode");
        System.out.println("  elastic - elastic consumer groups mode");
    }

    private void handleProcessingTime(BufferedReader reader, String[] args, String argsString) {
        try {
            String input;
            if (args.length != 1) {
                System.out.print("processing time: ");
                input = reader.readLine();
                if (input == null) return;
                input = input.trim();
            } else {
                input = argsString;
            }
            processingDuration = DurationConverter.parseDuration(input);
            System.out.printf("processing time set to %s%n", CliUtils.formatDuration(processingDuration));
        } catch (Exception e) {
            System.out.printf("error: can't parse processing time: %s%n", e.getMessage());
        }
    }

    private void handleList(BufferedReader reader, String[] args, String argsString) {
        try {
            if (args.length != 1) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
            } else {
                currentStream = argsString;
            }

            List<String> groups;
            if (isStatic) {
                groups = StaticConsumerGroup.list(nc, currentStream);
                System.out.println("static consumer groups: " + groups);
            } else {
                groups = ElasticConsumerGroup.list(nc, currentStream);
                System.out.println("elastic consumer groups: " + groups);
            }
        } catch (Exception e) {
            System.out.printf("error: can't list partitioned consumer groups: %s%n", e.getMessage());
        }
    }

    private void handleInfo(BufferedReader reader, String[] args) {
        try {
            if (args.length != 2) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
            }

            if (isStatic) {
                StaticConsumerGroupConfig config = StaticConsumerGroup.getConfig(nc, currentStream, currentGroup);
                System.out.printf("config: max members=%d, filter=%s%n", config.getMaxMembers(), config.getFilter());
                if (!config.getMembers().isEmpty()) {
                    System.out.printf("members: %s%n", config.getMembers());
                } else if (!config.getMemberMappings().isEmpty()) {
                    System.out.printf("Member mappings: %s%n", config.getMemberMappings());
                } else {
                    System.out.println("no members or mappings defined");
                }
                List<String> activeMembers = StaticConsumerGroup.listActiveMembers(nc, currentStream, currentGroup);
                System.out.printf("currently active members: %s%n", activeMembers);
            } else {
                ElasticConsumerGroupConfig config = ElasticConsumerGroup.getConfig(nc, currentStream, currentGroup);
                System.out.printf("config: max members=%d, filter=%s, partitioning wildcards %s%n",
                        config.getMaxMembers(), config.getFilter(), Arrays.toString(config.getPartitioningWildcards()));
                if (!config.getMembers().isEmpty()) {
                    System.out.printf("members: %s%n", config.getMembers());
                } else if (!config.getMemberMappings().isEmpty()) {
                    System.out.printf("Member mappings: %s%n", config.getMemberMappings());
                } else {
                    System.out.println("no members or mappings defined");
                }
                List<String> activeMembers = ElasticConsumerGroup.listActiveMembers(nc, currentStream, currentGroup);
                System.out.printf("currently active members: %s%n", activeMembers);
            }
        } catch (Exception e) {
            if (isStatic) {
                System.out.printf("can't get static partitioned consumer group config: %s%n", e.getMessage());
            } else {
                System.out.printf("can't get elastic partitioned consumer group config: %s%n", e.getMessage());
            }
        }
    }

    private void handleCreate(BufferedReader reader) {
        try {
            System.out.print("stream name: ");
            String input = reader.readLine();
            if (input == null) return;
            currentStream = input.trim();

            System.out.print("consumer group name: ");
            input = reader.readLine();
            if (input == null) return;
            currentGroup = input.trim();

            System.out.print("max members: ");
            input = reader.readLine();
            if (input == null) return;
            int maxMembers = Integer.parseInt(input.trim());

            System.out.print("filter: ");
            input = reader.readLine();
            if (input == null) return;
            String filter = input.trim();

            if (isStatic) {
                System.out.print("space separated set of members (hit return to set member mappings instead): ");
                input = reader.readLine();
                if (input == null) return;
                input = input.trim();

                if (input.isEmpty()) {
                    System.out.println("enter the member mappings");
                    List<String> mappingArgs = inputMemberMappings(reader);
                    if (mappingArgs.isEmpty()) {
                        System.out.println("member mappings not defined, can't create the partitioned consumer group");
                        return;
                    }
                    List<MemberMapping> memberMappings = CliUtils.parseMemberMappings(mappingArgs);
                    StaticConsumerGroup.create(nc, currentStream, currentGroup, maxMembers, filter, new ArrayList<>(), memberMappings);
                    System.out.println("static partitioned consumer group created");
                } else {
                    List<String> memberNames = Arrays.asList(input.split("\\s+"));
                    StaticConsumerGroup.create(nc, currentStream, currentGroup, maxMembers, filter, memberNames, new ArrayList<>());
                    System.out.println("static partitioned consumer group created");
                }
            } else {
                System.out.print("space separated partitioning wildcard indexes: ");
                input = reader.readLine();
                if (input == null) return;
                String[] pwciArgs = input.trim().split("\\s+");
                int[] wildcards = new int[pwciArgs.length];
                for (int i = 0; i < pwciArgs.length; i++) {
                    wildcards[i] = Integer.parseInt(pwciArgs[i]);
                }

                System.out.print("max buffered messages (0 for no limit): ");
                input = reader.readLine();
                if (input == null) return;
                long maxBufferedMsgs = input.trim().isEmpty() ? 0 : Long.parseLong(input.trim());

                System.out.print("max buffered bytes (0 for no limit): ");
                input = reader.readLine();
                if (input == null) return;
                long maxBufferedBytes = input.trim().isEmpty() ? 0 : Long.parseLong(input.trim());

                ElasticConsumerGroup.create(nc, currentStream, currentGroup, maxMembers, filter, wildcards, maxBufferedMsgs, maxBufferedBytes);
                System.out.println("elastic partitioned consumer group created");
            }
        } catch (Exception e) {
            System.out.printf("can't create partitioned consumer group: %s%n", e.getMessage());
        }
    }

    private void handleDelete(BufferedReader reader, String[] args) {
        try {
            if (args.length != 2) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
            }

            if (isStatic) {
                StaticConsumerGroup.delete(nc, currentStream, currentGroup);
                System.out.println("static consumer group deleted");
            } else {
                ElasticConsumerGroup.delete(nc, currentStream, currentGroup);
                System.out.println("elastic consumer group deleted");
            }
        } catch (Exception e) {
            System.out.printf("can't delete partitioned consumer group: %s%n", e.getMessage());
        }
    }

    private void handleAdd(BufferedReader reader, String[] args) {
        try {
            if (isStatic) {
                System.out.println("can not add members to a static partitioned consumer groups, you must delete and recreate them");
                return;
            }

            List<String> memberNames;
            if (args.length < 3) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
                System.out.print("member name (or space separated list of names): ");
                input = reader.readLine();
                if (input == null) return;
                memberNames = Arrays.asList(input.trim().split("\\s+"));
            } else {
                currentStream = args[0];
                currentGroup = args[1];
                memberNames = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));
            }

            List<String> members = ElasticConsumerGroup.addMembers(nc, currentStream, currentGroup, memberNames);
            System.out.println("added members: " + members);
        } catch (Exception e) {
            System.out.printf("can't add members: %s%n", e.getMessage());
        }
    }

    private void handleDrop(BufferedReader reader, String[] args) {
        try {
            if (isStatic) {
                System.out.println("can not drop members from a static partitioned consumer groups, you must delete and recreate them");
                return;
            }

            List<String> memberNames;
            if (args.length < 3) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
                System.out.print("member name (or space separated list of names): ");
                input = reader.readLine();
                if (input == null) return;
                memberNames = Arrays.asList(input.trim().split("\\s+"));
            } else {
                currentStream = args[0];
                currentGroup = args[1];
                memberNames = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));
            }

            List<String> members = ElasticConsumerGroup.deleteMembers(nc, currentStream, currentGroup, memberNames);
            System.out.println("dropped members: " + members);
        } catch (Exception e) {
            System.out.printf("can't drop members: %s%n", e.getMessage());
        }
    }

    private void handleCreateMapping(BufferedReader reader, String[] args) {
        try {
            if (args.length != 2) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
            }

            List<String> mappingArgs = inputMemberMappings(reader);
            List<MemberMapping> memberMappings = CliUtils.parseMemberMappings(mappingArgs);

            if (isStatic) {
                // For static, we'd need to recreate - not directly supported
                System.out.println("can not set member mappings on a static partitioned consumer group, you must delete and recreate it");
            } else {
                ElasticConsumerGroup.setMemberMappings(nc, currentStream, currentGroup, memberMappings);
                System.out.printf("member mappings set: %s%n", memberMappings);
            }
        } catch (Exception e) {
            System.out.printf("can't set member mappings: %s%n", e.getMessage());
        }
    }

    private void handleDeleteMapping(BufferedReader reader, String[] args) {
        try {
            if (args.length != 2) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
            }

            if (!CliUtils.confirm("WARNING: this operation will cause all existing consumer members to terminate consuming are you sure?")) {
                return;
            }

            if (isStatic) {
                System.out.println("can not delete member mappings on a static partitioned consumer group, you must delete and recreate it");
            } else {
                ElasticConsumerGroup.deleteMemberMappings(nc, currentStream, currentGroup);
                System.out.println("member mappings deleted");
            }
        } catch (Exception e) {
            System.out.printf("can't delete member mappings: %s%n", e.getMessage());
        }
    }

    private void handleMemberInfo(BufferedReader reader, String[] args) {
        try {
            if (args.length != 3) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
                System.out.print("member name: ");
                input = reader.readLine();
                if (input == null) return;
                currentMember = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
                currentMember = args[2];
            }

            if (isStatic) {
                StaticConsumerGroupConfig config = StaticConsumerGroup.getConfig(nc, currentStream, currentGroup);
                List<String> activeMembers = StaticConsumerGroup.listActiveMembers(nc, currentStream, currentGroup);

                if (config.isInMembership(currentMember)) {
                    System.out.printf("member %s is part of the consumer group membership%n", currentMember);
                    if (activeMembers.contains(currentMember)) {
                        System.out.printf("member %s is active%n", currentMember);
                    } else {
                        System.out.printf("***Warning*** member %s is part of the consumer group membership but has NO active instance%n", currentMember);
                    }
                } else {
                    System.out.printf("member %s is not part of the consumer group membership%n", currentMember);
                }
            } else {
                boolean[] status = ElasticConsumerGroup.isInMembershipAndActive(nc, currentStream, currentGroup, currentMember);
                boolean inMembership = status[0];
                boolean isActive = status[1];

                if (inMembership) {
                    if (isActive) {
                        System.out.printf("member %s is part of the consumer group membership and is active%n", currentMember);
                    } else {
                        System.out.printf("member %s is part of the consumer group membership%n", currentMember);
                        System.out.printf("***Warning*** member %s is part of the consumer group membership but has NO active instance%n", currentMember);
                    }
                } else {
                    System.out.printf("member %s is not currently part of the consumer group membership%n", currentMember);
                }
            }
        } catch (Exception e) {
            System.out.printf("can't get partitioned consumer group member info: %s%n", e.getMessage());
        }
    }

    private void handleStepDown(BufferedReader reader, String[] args) {
        try {
            if (args.length != 3) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
                System.out.print("member name: ");
                input = reader.readLine();
                if (input == null) return;
                currentMember = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
                currentMember = args[2];
            }

            if (isStatic) {
                StaticConsumerGroup.memberStepDown(nc, currentStream, currentGroup, currentMember);
            } else {
                ElasticConsumerGroup.memberStepDown(nc, currentStream, currentGroup, currentMember);
            }
            System.out.printf("member %s step down initiated%n", currentMember);
        } catch (Exception e) {
            System.out.printf("can't step down member: %s%n", e.getMessage());
        }
    }

    private void handleConsume(BufferedReader reader, String[] args) {
        try {
            if (consuming) {
                System.out.println("already consuming");
                return;
            }

            if (args.length != 3) {
                System.out.print("stream name: ");
                String input = reader.readLine();
                if (input == null) return;
                currentStream = input.trim();
                System.out.print("consumer group name: ");
                input = reader.readLine();
                if (input == null) return;
                currentGroup = input.trim();
                System.out.print("member name: ");
                input = reader.readLine();
                if (input == null) return;
                currentMember = input.trim();
            } else {
                currentStream = args[0];
                currentGroup = args[1];
                currentMember = args[2].trim();
            }

            Duration processingTime = processingDuration;
            String memberName = currentMember;

            System.out.println("consuming...");
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .maxAckPending(1)
                    .ackWait(Duration.ofSeconds(2))
                    .ackPolicy(AckPolicy.Explicit)
                    .build();

            ConsumerGroupConsumeContext ctx;
            if (isStatic) {
                ctx = StaticConsumerGroup.consume(nc, currentStream, currentGroup, memberName,
                        msg -> {
                            String pid = msg.getPinnedId();
                            try {
                                long seqNumber = msg.getMessage().metaData().streamSequence();
                                System.out.printf("[%s] subject=%s, seq=%d, pinnedID=%s. Processing for %s ... ",
                                        memberName, msg.getSubject(), seqNumber, pid, CliUtils.formatDuration(processingTime));
                                Thread.sleep(processingTime.toMillis());
                                msg.ackSync(Duration.ofSeconds(5));
                                System.out.println("acked");
                            } catch (Exception e) {
                                System.out.println("message could not be acked! (it will be or may already have been re-delivered): " + e.getMessage());
                            }
                        },
                        consumerConfig);
            } else {
                ctx = ElasticConsumerGroup.consume(nc, currentStream, currentGroup, memberName,
                        msg -> {
                            String pid = msg.getPinnedId();
                            try {
                                long seqNumber = msg.getMessage().metaData().streamSequence();
                                System.out.printf("[%s] subject=%s, seq=%d, pinnedID=%s. Processing for %s ... ",
                                        memberName, msg.getSubject(), seqNumber, pid, CliUtils.formatDuration(processingTime));
                                Thread.sleep(processingTime.toMillis());
                                msg.ackSync(Duration.ofSeconds(5));
                                System.out.println("acked");
                            } catch (Exception e) {
                                System.out.println("message could not be acked! (it will be or may already have been re-delivered): " + e.getMessage());
                            }
                        },
                        consumerConfig);
            }

            consuming = true;

            // Wait for completion
            try {
                ctx.done().get();
                System.out.println("instance returned with no error");
            } catch (Exception e) {
                System.out.println("instance returned with an error: " + e.getMessage());
            }
            consuming = false;
        } catch (Exception e) {
            System.out.printf("can't join the partitioned consumer group: %s%n", e.getMessage());
            consuming = false;
        }
    }

    private List<String> inputMemberMappings(BufferedReader reader) {
        List<String> mappings = new ArrayList<>();
        System.out.println("enter member mappings in format member:partition1,partition2,... (empty line to finish)");
        try {
            while (true) {
                System.out.print("mapping (or empty to finish): ");
                String input = reader.readLine();
                if (input == null || input.trim().isEmpty()) break;
                mappings.add(input.trim());
            }
        } catch (Exception e) {
            System.err.println("Error reading input: " + e.getMessage());
        }
        return mappings;
    }
}
