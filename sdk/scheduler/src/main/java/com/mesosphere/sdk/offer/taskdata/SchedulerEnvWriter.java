package com.mesosphere.sdk.offer.taskdata;

import java.util.Map;
import java.util.Optional;

import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.HealthCheck;
import org.apache.mesos.Protos.TaskInfo;

import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.specification.CommandSpec;
import com.mesosphere.sdk.specification.ConfigFileSpec;
import com.mesosphere.sdk.specification.PodInstance;
import com.mesosphere.sdk.specification.TaskSpec;

/**
 * Provides write access to task environment variables which are (only) written by the Scheduler.
 */
public class SchedulerEnvWriter {
    private final TaskDataWriter taskAndHealthCheckEnv;
    private final TaskDataWriter taskOnlyEnv;

    public SchedulerEnvWriter() {
        this.taskAndHealthCheckEnv = new TaskDataWriter();
        this.taskOnlyEnv = new TaskDataWriter();
    }

    public SchedulerEnvWriter setEnv(
            String serviceName,
            PodInstance podInstance,
            TaskSpec taskSpec,
            CommandSpec commandSpec,
            String configDownloadDir,
            Map<String, String> planParameters) {

        // Task envvars from either of the following sources:
        // - ServiceSpec (provided by developer)
        // - TASKCFG_<podname>_* (provided by user, handled when parsing YAML, potentially overrides ServiceSpec)
        taskAndHealthCheckEnv.putAll(commandSpec.getEnvironment());

        // Default envvars for use by executors/developers:

        // Inject Pod Instance Index
        taskAndHealthCheckEnv.put(EnvConstants.POD_INSTANCE_INDEX_TASKENV, String.valueOf(podInstance.getIndex()));
        // Inject Framework Name
        taskAndHealthCheckEnv.put(EnvConstants.FRAMEWORK_NAME_TASKENV, serviceName);
        // Inject TASK_NAME as KEY:VALUE
        taskAndHealthCheckEnv.put(EnvConstants.TASK_NAME_TASKENV, TaskSpec.getInstanceName(podInstance, taskSpec));
        // Inject TASK_NAME as KEY for conditional mustache templating
        taskAndHealthCheckEnv.put(TaskSpec.getInstanceName(podInstance, taskSpec), "true");

        if (taskSpec.getConfigFiles() != null) {
            for (ConfigFileSpec configSpec : taskSpec.getConfigFiles()) {
                // Comma-separated components in the env value:
                // 1. where the template file was downloaded (by the mesos fetcher)
                // 2. where the rendered result should go
                String configEnvVal =
                        String.format("%s%s,%s", configDownloadDir, configSpec.getName(), configSpec.getRelativePath());
                taskOnlyEnv.put(
                        EnvConstants.CONFIG_TEMPLATE_TASKENV_PREFIX + EnvUtils.toEnvName(configSpec.getName()),
                        configEnvVal);
            }
        }

        taskOnlyEnv.putAll(planParameters);

        return this;
    }

    /**
     * Stores the value used for a dynamic port. This allows a degree of stickiness for dynamic ports, keeping them the
     * same across (scheduler and/or task) restarts to avoid constantly re-reserving port resources.
     *
     * This also updates the environment of the embedded Readiness Check, if one is present.
     */
    public static void setDynamicPort(
            TaskInfo.Builder taskInfoBuilder, String portName, Optional<String> customEnvKey, long port)
                    throws TaskException {
        String portEnvName = EnvUtils.getPortEnvName(portName, customEnvKey);
        String portVal = Long.toString(port);

        SchedulerLabelWriter labelWriter = new SchedulerLabelWriter(taskInfoBuilder);
        // 1. Update task label:
        labelWriter.setDynamicPort(portName, port);

        // 2. Update readiness check env (embedded in label), if any:
        Optional<HealthCheck> readinessCheckOptional = labelWriter.getReadinessCheck();
        if (readinessCheckOptional.isPresent()) {
            // Update readiness check env (embedded in label):
            HealthCheck.Builder readinessCheck = readinessCheckOptional.get().toBuilder();
            TaskDataWriter writer = new TaskDataWriter(EnvUtils.toMap(readinessCheck.getCommand().getEnvironment()));
            writer.put(portEnvName, portVal);
            readinessCheck.getCommandBuilder().setEnvironment(EnvUtils.toProto(writer.map()));
            labelWriter.setReadinessCheck(readinessCheck.build());
        }
        taskInfoBuilder.setLabels(labelWriter.toProto());

        // 3. Update health check env, if any:
        if (taskInfoBuilder.hasHealthCheck()) {
            TaskDataWriter writer =
                    new TaskDataWriter(EnvUtils.toMap(taskInfoBuilder.getHealthCheck().getCommand().getEnvironment()));
            writer.put(portEnvName, portVal);
            taskInfoBuilder.getHealthCheckBuilder().getCommandBuilder().setEnvironment(EnvUtils.toProto(writer.map()));
        }

        // 4. Update main task env:
        TaskDataWriter writer = new TaskDataWriter(EnvUtils.toMap(taskInfoBuilder.getCommand().getEnvironment()));
        writer.put(portEnvName, portVal);
        taskInfoBuilder.getCommandBuilder().setEnvironment(EnvUtils.toProto(writer.map()));
    }

    public Environment getTaskEnv() {
        return EnvUtils.toProto(new TaskDataWriter()
                .putAll(taskOnlyEnv.map())
                .putAll(taskAndHealthCheckEnv.map())
                .map());
    }

    public Environment getHealthCheckEnv() {
        return EnvUtils.toProto(taskAndHealthCheckEnv.map());
    }
}
