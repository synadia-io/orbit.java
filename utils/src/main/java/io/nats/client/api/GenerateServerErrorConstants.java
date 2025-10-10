// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.nats.client.api;

import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class GenerateServerErrorConstants {
    public static final Comparator<GenerateServerErrorConstants> FILE_ORDER = null;
    public static final Comparator<GenerateServerErrorConstants> CONSTANTS_ALPHA = Comparator.comparing(x -> x.constant);
    public static final Comparator<GenerateServerErrorConstants> ERROR_CODE_ASCENDING = Comparator.comparing(x -> x.errorCode);

    private static final String TARGET_CLASS_NAME = "ServerErrorConstants";
    private static final String TARGET_CLASS_FILE = "C:\\temp\\" + TARGET_CLASS_NAME + ".java";
    private static final String ERRORS_JSON = "https://raw.githubusercontent.com/nats-io/nats-server/refs/heads/main/server/errors.json";
    private static final Comparator<GenerateServerErrorConstants> COMPARATOR = ERROR_CODE_ASCENDING;

    public static byte[] getJson() throws Exception {
        URL url = new URL(ERRORS_JSON);
        try (InputStream inputStream = url.openStream();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            return outputStream.toByteArray();
        }
    }

    final String constant;
    final int code;
    final int errorCode;
    final String description;

    public GenerateServerErrorConstants(JsonValue item) {
        constant = item.map.get("constant").string;
        code = item.map.get("code").i;
        errorCode = item.map.get("error_code").i;
        description = item.map.get("description").string;
    }

    public String getConstantLine() {
        return String.format("    public static final Error %s = new Error(%d, %d, \"%s\");",
            constant, code, errorCode, description);
    }

    public String getStaticLine() {
        return String.format("        temp.put(%d, %s);", errorCode, constant);
    }

    @Override
    public String toString() {
        return getConstantLine();
    }

    public static void main(String[] args) {
        try {
            JsonValue jv = JsonParser.parse(getJson());

            List<GenerateServerErrorConstants> list = new ArrayList<>();
            for (JsonValue item : jv.array) {
                list.add(new GenerateServerErrorConstants(item));
            }

            //noinspection ConstantValue
            if (COMPARATOR != null) {
                list.sort(COMPARATOR);
            }

            try (FileOutputStream out = new FileOutputStream(TARGET_CLASS_FILE)) {

                write(out, "package io.nats.client.api;\n\n" +
                    "import java.util.Collections;\n" +
                    "import java.util.HashMap;\n" +
                    "import java.util.Map;\n\n" +
                    "public final class ");
                write(out, TARGET_CLASS_NAME);
                writeln(out, " {\n" +
                    "    public static final Map<Integer, Error> ERROR_BY_API_ERROR_CODE;\n");

                for (GenerateServerErrorConstants constant : list) {
                    System.out.println(constant);
                    writeln(out, constant.getConstantLine());
                }
                writeln(out, "\n    static {\n" +
                    "        Map<Integer, Error> temp = new HashMap<>();");
                for (GenerateServerErrorConstants constant : list) {
                    writeln(out, constant.getStaticLine());
                }
                writeln(out, "        ERROR_BY_API_ERROR_CODE = Collections.unmodifiableMap(temp);\n    }\n}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void write(FileOutputStream out, String s) throws IOException {
        out.write(s.replace("\\n", System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
    }

    private static void writeln(FileOutputStream out, String s) throws IOException {
        write(out, s);
        out.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
    }
}
