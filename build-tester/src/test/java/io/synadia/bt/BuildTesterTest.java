// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.bt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BuildTesterTest {

    @Test
    public void testBuildTester() throws Exception {
		BuildTester bt = new BuildTester(42);
        assertEquals(42, bt.x);
    }
}
