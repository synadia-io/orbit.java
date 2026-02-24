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
import picocli.CommandLine.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Static consumer group CLI commands.
 */
@Command(name = "static", description = "Static consumer groups mode",
        subcommands = {
                StaticCommands.Ls.class,
                StaticCommands.Info.class,
                StaticCommands.Create.class,
                StaticCommands.Delete.class,
                StaticCommands.MemberInfo.class,
                StaticCommands.StepDown.class,
                StaticCommands.Consume.class,
                StaticCommands.Prompt.class
        })
public class StaticCommands implements Callable<Integer> {

    @ParentCommand
    CgCommand parent;

    @Override
    public Integer call() {
        System.out.println("Use 'cg static --help' for available subcommands");
        return 0;
    }

    @Command(name = "ls", aliases = {"list"}, description = "List static consumer groups for a stream")
    static class Ls implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                List<String> groups = StaticConsumerGroup.list(nc, streamName);
                System.out.println("static consumer groups: " + groups);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "info", description = "Get static consumer group info")
    static class Info implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                StaticConsumerGroupConfig config = StaticConsumerGroup.getConfig(nc, streamName, consumerGroupName);

                System.out.printf("config: max members=%d, filter=%s%n", config.getMaxMembers(), config.getFilter());

                if (!config.getMembers().isEmpty()) {
                    System.out.printf("members: %s%n", config.getMembers());
                } else if (!config.getMemberMappings().isEmpty()) {
                    System.out.printf("Member mappings: %s%n", config.getMemberMappings());
                } else {
                    System.out.println("no members or mappings defined");
                }

                List<String> activeMembers = StaticConsumerGroup.listActiveMembers(nc, streamName, consumerGroupName);
                System.out.printf("currently active members: %s%n", activeMembers);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "create", description = "Create a static partitioned consumer group",
            subcommands = {StaticCommands.CreateBalanced.class, StaticCommands.CreateMapped.class})
    static class Create implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Override
        public Integer call() {
            System.out.println("Use 'cg static create balanced' or 'cg static create mapped'");
            return 0;
        }
    }

    @Command(name = "balanced", description = "Create a static consumer group with balanced members")
    static class CreateBalanced implements Callable<Integer> {
        @ParentCommand
        private Create createParent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Max number of members")
        int maxMembers;

        @Parameters(index = "3", description = "Filter")
        String filter;

        @Parameters(index = "4..*", arity = "1..*", description = "Member names")
        List<String> memberNames;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(createParent.parent.parent.context)) {
                StaticConsumerGroup.create(nc, streamName, consumerGroupName, maxMembers, filter, memberNames, new ArrayList<>());
                System.out.println("static partitioned consumer group created");
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "mapped", description = "Create a static consumer group with member mappings")
    static class CreateMapped implements Callable<Integer> {
        @ParentCommand
        private Create createParent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Max number of members")
        int maxMembers;

        @Parameters(index = "3", description = "Filter")
        String filter;

        @Parameters(index = "4..*", description = "Mappings in format member:partition1,partition2,...")
        List<String> mappingArgs;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(createParent.parent.parent.context)) {
                List<MemberMapping> memberMappings = CliUtils.parseMemberMappings(mappingArgs);
                StaticConsumerGroup.create(nc, streamName, consumerGroupName, maxMembers, filter, new ArrayList<>(), memberMappings);
                System.out.println("static partitioned consumer group created");
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "delete", aliases = {"rm"}, description = "Delete a static partitioned consumer group")
    static class Delete implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Option(names = {"-f", "--force"}, description = "Force delete without confirmation")
        boolean force;

        @Override
        public Integer call() {
            if (!force) {
                if (!CliUtils.confirm("WARNING: this operation will cause all existing consumer members to terminate consuming. Are you sure?")) {
                    System.out.println("Operation canceled");
                    return 1;
                }
            }

            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                StaticConsumerGroup.delete(nc, streamName, consumerGroupName);
                System.out.println("static consumer group deleted");
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "member-info", aliases = {"memberinfo", "minfo"}, description = "Get static consumer group member info")
    static class MemberInfo implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Member name")
        String memberName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                StaticConsumerGroupConfig config = StaticConsumerGroup.getConfig(nc, streamName, consumerGroupName);
                List<String> activeMembers = StaticConsumerGroup.listActiveMembers(nc, streamName, consumerGroupName);

                if (config.isInMembership(memberName)) {
                    System.out.printf("member %s is part of the consumer group membership%n", memberName);
                    if (activeMembers.contains(memberName)) {
                        System.out.printf("member %s is active%n", memberName);
                    } else {
                        System.out.printf("***Warning*** member %s is part of the consumer group membership but has NO active instance%n", memberName);
                    }
                } else {
                    System.out.printf("member %s is not part of the consumer group membership%n", memberName);
                }
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "step-down", aliases = {"stepdown", "sd"}, description = "Initiate a step down for a member")
    static class StepDown implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Member name")
        String memberName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                StaticConsumerGroup.memberStepDown(nc, streamName, consumerGroupName, memberName);
                System.out.printf("member %s step down initiated%n", memberName);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "consume", aliases = {"join"}, description = "Join a static partitioned consumer group")
    static class Consume implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Member name")
        String memberName;

        @Option(names = "--sleep", description = "Sleep to simulate processing time (e.g., 20ms, 5s, 1m)", defaultValue = "20ms", converter = DurationConverter.class)
        Duration processingDuration = Duration.ofMillis(20);

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                Duration processingTime = processingDuration;

                System.out.println("consuming...");
                ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                        .maxAckPending(1)
                        .ackWait(Duration.ofSeconds(2))
                        .ackPolicy(AckPolicy.Explicit)
                        .build();
                ConsumerGroupConsumeContext ctx = StaticConsumerGroup.consume(nc, streamName, consumerGroupName, memberName,
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

                // Wait for completion
                try {
                    ctx.done().get();
                    System.out.println("instance returned with no error");
                } catch (Exception e) {
                    System.out.println("instance returned with an error: " + e.getMessage());
                }

                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "prompt", description = "Interactive prompt mode")
    static class Prompt implements Callable<Integer> {
        @ParentCommand
        private StaticCommands parent;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                return new PromptHandler(true, parent.parent.context, nc).run();
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }
}
