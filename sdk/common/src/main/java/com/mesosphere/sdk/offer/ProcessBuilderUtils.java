package com.mesosphere.sdk.offer;

import java.util.Map;

import org.apache.mesos.Protos;

import com.mesosphere.sdk.offer.taskdata.EnvUtils;

/**
 * Utilities relating to construction of {@link ProcessBuilder}s.
 */
public class ProcessBuilderUtils {
    public static ProcessBuilder buildProcess(Protos.CommandInfo cmd) {
        return buildProcess(cmd.getValue(), EnvUtils.toMap(cmd.getEnvironment()));
    }

    public static ProcessBuilder buildProcess(String cmd, Map<String, String> env) {
        ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", cmd).inheritIO();
        builder.environment().putAll(env);
        return builder;
    }
}
