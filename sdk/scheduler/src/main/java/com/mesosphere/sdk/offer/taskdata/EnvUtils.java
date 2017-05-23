package com.mesosphere.sdk.offer.taskdata;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Scheduler-specific utilities relating to task environment construction.
 */
public class EnvUtils {

    /** Provides the Task/Pod index of the instance, starting at 0. */
    static final String POD_INSTANCE_INDEX_TASKENV = "POD_INSTANCE_INDEX";
    /** Prefix used for port environment variables which advertise reserved ports by their name. */
    static final String PORT_NAME_TASKENV_PREFIX = "PORT_";
    /** Prefix used for config file templates to be handled by the 'bootstrap' utility executable. */
    static final String CONFIG_TEMPLATE_TASKENV_PREFIX = "CONFIG_TEMPLATE_";
    /** Provides the configured name of the framework/service. */
    static final String FRAMEWORK_NAME_TASKENV = "FRAMEWORK_NAME";
    /** Provides the name of the pod/task within the service. */
    static final String TASK_NAME_TASKENV = "TASK_NAME";

    private static final Pattern ENVVAR_INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9_]");

    private EnvUtils() {
        // do not instantiate
    }

    /**
     * Converts the provided string to a conventional environment variable name, consisting of numbers, uppercase
     * letters, and underscores. Strictly speaking, lowercase characters are not invalid, but this avoids them to follow
     * convention.
     *
     * For example: {@code hello.There999!} => {@code HELLO_THERE999_}
     */
    public static String toEnvName(String str) {
        return ENVVAR_INVALID_CHARS.matcher(str.toUpperCase()).replaceAll("_");
    }

    /**
     * Returns a environment variable-style rendering of the specified port.
     */
    static String getPortEnvName(String portName, Optional<String> customEnvKey) {
        String draftEnvName = customEnvKey.isPresent()
                ? customEnvKey.get() // use custom name as-is
                : PORT_NAME_TASKENV_PREFIX + portName; // PORT_[name]
        // Envvar should be uppercased with invalid characters replaced with underscores:
        return toEnvName(draftEnvName);
    }
}
