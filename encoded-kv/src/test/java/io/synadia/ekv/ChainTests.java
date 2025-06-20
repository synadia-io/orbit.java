// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.synadia.ekv.codec.*;
import io.synadia.ekv.misc.Data;
import io.synadia.ekv.misc.GeneralType;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

public class ChainTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    @Test
    public void testKeyChain() {
        GeneralStringKeyCodec main = new GeneralStringKeyCodec(GeneralType.BASE64);
        GeneralStringKeyCodec chain1 = new GeneralStringKeyCodec(GeneralType.HEX);
        GeneralStringKeyCodec chain2 = new GeneralStringKeyCodec(GeneralType.BASE64);

        String value = "test.chain";

        String mainString = main.encode(value);
        String chain1String = chain1.encode(mainString);
        String lastString = chain2.encode(chain1String);

        String mainFilterStart = mainString.split("\\.")[0];
        String chain1FilterStart = chain1String.split("\\.")[0];
        String lastFilterStart = lastString.split("\\.")[0];

        String mainFilterEnd = mainString.split("\\.")[1];
        String chain1FilterEnd = chain1String.split("\\.")[1];
        String lastFilterEnd = lastString.split("\\.")[1];

        String chain2Decoded = chain2.decode(lastString);
        String chain1Decoded = chain1.decode(chain2Decoded);
        String mainDecoded = main.decode(chain1Decoded);

        assertEquals(mainString, chain1Decoded);
        assertEquals(chain1String, chain2Decoded);
        assertEquals(value, mainDecoded);

        // test both constructor
        ChainedKeyCodec<String> cvc = new ChainedKeyCodec<>(main, chain1, chain2);
        assertEquals(lastString, cvc.encode(value));
        assertEquals(value, cvc.decode(lastString));

        cvc = new ChainedKeyCodec<>(main, Arrays.asList(chain1, chain2));
        assertEquals(lastString, cvc.encode(value));
        assertEquals(value, cvc.decode(lastString));

        assertTrue(cvc.allowsFiltering());

        value = "test.chain";

        String mainFilter = main.encodeFilter(value);
        String chain1Filter = chain1.encodeFilter(mainFilter);
        String lastFilter = chain2.encodeFilter(chain1Filter);

        assertTrue(mainFilter.startsWith(mainFilterStart));
        assertTrue(chain1Filter.startsWith(chain1FilterStart));
        assertTrue(lastFilter.startsWith(lastFilterStart));

        assertTrue(mainFilter.endsWith(mainFilterEnd));
        assertTrue(chain1Filter.endsWith(chain1FilterEnd));
        assertTrue(lastFilter.endsWith(lastFilterEnd));

        value = "test.>";
        mainFilter = main.encodeFilter(value);
        chain1Filter = chain1.encodeFilter(mainFilter);
        lastFilter = chain2.encodeFilter(chain1Filter);

        assertTrue(mainFilter.startsWith(mainFilterStart));
        assertTrue(chain1Filter.startsWith(chain1FilterStart));
        assertTrue(lastFilter.startsWith(lastFilterStart));

        assertEquals(mainFilterStart + ".>", mainFilter);
        assertEquals(chain1FilterStart + ".>", chain1Filter);
        assertEquals(lastFilterStart + ".>", lastFilter);

        value = "test.*.chain";
        mainFilter = main.encodeFilter(value);
        chain1Filter = chain1.encodeFilter(mainFilter);
        lastFilter = chain2.encodeFilter(chain1Filter);

        assertTrue(mainFilter.startsWith(mainFilterStart));
        assertTrue(chain1Filter.startsWith(chain1FilterStart));
        assertTrue(lastFilter.startsWith(lastFilterStart));

        assertEquals(mainFilterStart + ".*." + mainFilterEnd, mainFilter);
        assertEquals(chain1FilterStart + ".*." + chain1FilterEnd, chain1Filter);
        assertEquals(lastFilterStart + ".*." + lastFilterEnd, lastFilter);
    }

    @Test
    public void testValueChain() {
        DataValueCodec main = new DataValueCodec(GeneralType.BASE64);
        GeneralByteValueCodec chain1 = new GeneralByteValueCodec(GeneralType.HEX);
        GeneralByteValueCodec chain2 = new GeneralByteValueCodec(GeneralType.BASE64);

        Data value = new Data("test", "chain", false);

        byte[] mainBytes = main.encode(value);
        byte[] chain1Bytes = chain1.encode(mainBytes);
        byte[] lastBytes = chain2.encode(chain1Bytes);

        byte[] chain2Decoded = chain2.decode(lastBytes);
        byte[] chain1Decoded = chain1.decode(chain2Decoded);
        Data mainDecoded = main.decode(chain1Decoded);

        assertArrayEquals(mainBytes, chain1Decoded);
        assertArrayEquals(chain1Bytes, chain2Decoded);
        assertEquals(value, mainDecoded);

        // test both constructors
        ChainedValueCodec<Data> cvc = new ChainedValueCodec<>(main, chain1, chain2);
        assertArrayEquals(lastBytes, cvc.encode(value));
        assertEquals(value, cvc.decode(lastBytes));

        cvc = new ChainedValueCodec<>(main, Arrays.asList(chain1, chain2));
        assertArrayEquals(lastBytes, cvc.encode(value));
        assertEquals(value, cvc.decode(lastBytes));
    }
}
