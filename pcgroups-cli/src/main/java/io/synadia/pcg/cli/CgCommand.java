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

import picocli.CommandLine;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

/**
 * Main CLI entry point for the Partitioned Consumer Group CLI tool.
 * Run with: java -jar cg.jar [command] [options]
 */
@Command(name = "cg",
        version = "0.1.0",
        description = "Partitioned Consumer Group CLI tool",
        mixinStandardHelpOptions = true,
        subcommands = {
                StaticCommands.class,
                ElasticCommands.class
        })
public class CgCommand implements Callable<Integer> {

    @Option(names = "--context", description = "NATS CLI context to use")
    String context;

    @Override
    public Integer call() {
        System.out.println("Partitioned Consumer Group CLI tool v0.1.0");
        System.out.println();
        System.out.println("Usage: cg <command> [options]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  static   Static consumer groups mode");
        System.out.println("  elastic  Elastic consumer groups mode");
        System.out.println();
        System.out.println("Use 'cg <command> --help' for more information about a command.");
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CgCommand())
                .setAbbreviatedOptionsAllowed(true)
                .setAbbreviatedSubcommandsAllowed(true)
                .execute(args);
        System.exit(exitCode);
    }
}
