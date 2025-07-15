// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.chaos;

public interface ChaosPrinter {
    void out(Object... objects);
    void err(Object... objects);
}
