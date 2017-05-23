package com.mesosphere.sdk.offer.taskdata;

import java.util.Map;
import java.util.TreeMap;

import org.apache.mesos.Protos.Environment;

/**
 * Utilities relating to environment variable manipulation.
 */
public class CommonEnvUtils {

    private CommonEnvUtils() {
        // do not instantiate
    }

    /**
     * Returns a Map representation of the provided {@link Environment}.
     * In the event of duplicate labels, the last duplicate wins.
     * This is the inverse of {@link #toProto(Map)}.
     */
    public static Map<String, String> toMap(Environment environment) {
        // sort labels alphabetically for convenience in debugging/logging:
        Map<String, String> map = new TreeMap<>();
        for (Environment.Variable variable : environment.getVariablesList()) {
            map.put(variable.getName(), variable.getValue());
        }
        return map;
    }

    /**
     * Returns a Protobuf representation of the provided {@link Map}.
     */
    static Environment toProto(Map<String, String> environmentMap) {
        Environment.Builder envBuilder = Environment.newBuilder();
        for (Map.Entry<String, String> entry : environmentMap.entrySet()) {
            envBuilder.addVariablesBuilder()
                .setName(entry.getKey())
                .setValue(entry.getValue());
        }
        return envBuilder.build();
    }
}
