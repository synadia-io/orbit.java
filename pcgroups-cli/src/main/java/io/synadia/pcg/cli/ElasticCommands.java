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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Elastic consumer group CLI commands.
 */
@Command(name = "elastic", description = "Elastic consumer groups mode",
        subcommands = {
                ElasticCommands.Ls.class,
                ElasticCommands.Info.class,
                ElasticCommands.Create.class,
                ElasticCommands.Delete.class,
                ElasticCommands.Add.class,
                ElasticCommands.Drop.class,
                ElasticCommands.CreateMapping.class,
                ElasticCommands.DeleteMapping.class,
                ElasticCommands.MemberInfo.class,
                ElasticCommands.StepDown.class,
                ElasticCommands.Consume.class,
                ElasticCommands.Prompt.class
        })
public class ElasticCommands implements Callable<Integer> {

    @ParentCommand
    CgCommand parent;

    @Override
    public Integer call() {
        System.out.println("Use 'cg elastic --help' for available subcommands");
        return 0;
    }

    @Command(name = "ls", aliases = {"list"}, description = "List elastic consumer groups for a stream")
    static class Ls implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                List<String> groups = ElasticConsumerGroup.list(nc, streamName);
                System.out.println("elastic consumer groups: " + groups);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "info", description = "Get elastic consumer group info")
    static class Info implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                ElasticConsumerGroupConfig config = ElasticConsumerGroup.getConfig(nc, streamName, consumerGroupName);

                System.out.printf("config: max members=%d, filter=%s, partitioning wildcards %s%n",
                        config.getMaxMembers(), config.getFilter(), Arrays.toString(config.getPartitioningWildcards()));

                if (!config.getMembers().isEmpty()) {
                    System.out.printf("members: %s%n", config.getMembers());
                } else if (!config.getMemberMappings().isEmpty()) {
                    System.out.printf("Member mappings: %s%n", config.getMemberMappings());
                } else {
                    System.out.println("no members or mappings defined");
                }

                List<String> activeMembers = ElasticConsumerGroup.listActiveMembers(nc, streamName, consumerGroupName);
                System.out.printf("currently active members: %s%n", activeMembers);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "create", description = "Create an elastic partitioned consumer group")
    static class Create implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Max number of members")
        int maxMembers;

        @Parameters(index = "3", description = "Filter")
        String filter;

        @Parameters(index = "4..*", description = "Partitioning wildcard indexes")
        List<String> wildcardArgs;

        @Option(names = "--max-buffered-msgs", description = "Max number of buffered messages", defaultValue = "0")
        long maxBufferedMsgs;

        @Option(names = "--max-buffered-bytes", description = "Max number of buffered bytes", defaultValue = "0")
        long maxBufferedBytes;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                int[] wildcards = CliUtils.parsePartitioningWildcards(wildcardArgs);
                ElasticConsumerGroup.create(nc, streamName, consumerGroupName, maxMembers, filter, wildcards, maxBufferedMsgs, maxBufferedBytes);
                System.out.println("elastic partitioned consumer group created");
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "delete", aliases = {"rm"}, description = "Delete an elastic partitioned consumer group")
    static class Delete implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

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
                ElasticConsumerGroup.delete(nc, streamName, consumerGroupName);
                System.out.println("elastic consumer group deleted");
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "add", description = "Add members to an elastic consumer group")
    static class Add implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2..*", description = "Member names")
        List<String> memberNames;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                List<String> members = ElasticConsumerGroup.addMembers(nc, streamName, consumerGroupName, memberNames);
                System.out.println("added members: " + members);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "drop", description = "Drop members from an elastic consumer group")
    static class Drop implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2..*", description = "Member names")
        List<String> memberNames;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                List<String> members = ElasticConsumerGroup.deleteMembers(nc, streamName, consumerGroupName, memberNames);
                System.out.println("dropped members: " + members);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "create-mapping", aliases = {"cm", "createmapping"}, description = "Create member mappings for an elastic consumer group")
    static class CreateMapping implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2..*", description = "Mappings in format member:partition1,partition2,...")
        List<String> mappingArgs;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                List<MemberMapping> memberMappings = CliUtils.parseMemberMappings(mappingArgs);
                ElasticConsumerGroup.setMemberMappings(nc, streamName, consumerGroupName, memberMappings);
                System.out.println("member mapping: " + memberMappings);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "delete-mapping", aliases = {"dm", "deletemapping"}, description = "Delete member mappings for an elastic consumer group")
    static class DeleteMapping implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                ElasticConsumerGroup.deleteMemberMappings(nc, streamName, consumerGroupName);
                System.out.println("member mappings deleted");
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "member-info", aliases = {"memberinfo", "minfo"}, description = "Get elastic consumer group member info")
    static class MemberInfo implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Member name")
        String memberName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                boolean[] status = ElasticConsumerGroup.isInMembershipAndActive(nc, streamName, consumerGroupName, memberName);
                boolean inMembership = status[0];
                boolean isActive = status[1];

                if (inMembership) {
                    if (isActive) {
                        System.out.printf("member %s is part of the consumer group membership and is active%n", memberName);
                    } else {
                        System.out.printf("member %s is part of the consumer group membership%n", memberName);
                        System.out.printf("***Warning*** member %s is part of the consumer group membership but has NO active instance%n", memberName);
                    }
                } else {
                    System.out.printf("member %s is not currently part of the consumer group membership%n", memberName);
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
        private ElasticCommands parent;

        @Parameters(index = "0", description = "Stream name")
        String streamName;

        @Parameters(index = "1", description = "Consumer group name")
        String consumerGroupName;

        @Parameters(index = "2", description = "Member name")
        String memberName;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                ElasticConsumerGroup.memberStepDown(nc, streamName, consumerGroupName, memberName);
                System.out.printf("member %s step down initiated%n", memberName);
                return 0;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "consume", aliases = {"join"}, description = "Join an elastic partitioned consumer group")
    static class Consume implements Callable<Integer> {
        @ParentCommand
        private ElasticCommands parent;

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
                ConsumerGroupConsumeContext ctx = ElasticConsumerGroup.consume(nc, streamName, consumerGroupName, memberName,
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
        private ElasticCommands parent;

        @Override
        public Integer call() {
            try (Connection nc = CliUtils.connect(parent.parent.context)) {
                return new PromptHandler(false, parent.parent.context, nc).run();
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                return 1;
            }
        }
    }
}
