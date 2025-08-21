// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.api.KeyResult;
import io.synadia.ekv.codec.StringKeyCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MiscTests {

    @Test
    public void testEncodedKeyResult() {
        KeyResult kr = new KeyResult();
        EncodedKeyResult<String> ekr = new EncodedKeyResult<>(kr, new StringKeyCodec());
        assertNull(ekr.getKey());
        assertNull(ekr.getException());
        assertFalse(ekr.isKey());
        assertFalse(ekr.isException());
        assertTrue(ekr.isDone());

        kr = new KeyResult("key");
        ekr = new EncodedKeyResult<>(kr, new StringKeyCodec());
        assertNotNull(ekr.getKey());
        assertEquals(kr.getKey(), ekr.getKey());
        assertNull(ekr.getException());
        assertTrue(ekr.isKey());
        assertFalse(ekr.isException());
        assertFalse(ekr.isDone());

        kr = new KeyResult(new Exception("message"));
        ekr = new EncodedKeyResult<>(kr, new StringKeyCodec());
        assertNull(ekr.getKey());
        assertNotNull(ekr.getException());
        assertTrue(ekr.getException().getMessage().contains("message"));
        assertFalse(ekr.isKey());
        assertTrue(ekr.isException());
        assertTrue(ekr.isDone());
    }
}
