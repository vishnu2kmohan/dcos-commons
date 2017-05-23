package com.mesosphere.sdk.offer.taskdata;

import java.util.Map;
import java.util.Optional;

import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.ExecutorInfo;
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
        taskAndHealthCheckEnv.put(EnvUtils.POD_INSTANCE_INDEX_TASKENV, String.valueOf(podInstance.getIndex()));
        // Inject Framework Name
        taskAndHealthCheckEnv.put(EnvUtils.FRAMEWORK_NAME_TASKENV, serviceName);
        // Inject TASK_NAME as KEY:VALUE
        taskAndHealthCheckEnv.put(EnvUtils.TASK_NAME_TASKENV, TaskSpec.getInstanceName(podInstance, taskSpec));
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
                        EnvUtils.CONFIG_TEMPLATE_TASKENV_PREFIX + EnvUtils.toEnvName(configSpec.getName()),
                        configEnvVal);
            }
        }

        taskOnlyEnv.putAll(planParameters);

        return this;
    }

    /**
     * Returns the environment variables to be stored against the root TaskInfo.
     */
    public Environment getTaskEnv() {
        return CommonEnvUtils.toProto(new TaskDataWriter()
                .putAll(taskOnlyEnv.map())
                .putAll(taskAndHealthCheckEnv.map())
                .map());
    }

    /**
     * Returns the environment variables to be stored against the health check and/or readiness check, if either is
     * applicable.
     */
    public Environment getHealthCheckEnv() {
        return CommonEnvUtils.toProto(taskAndHealthCheckEnv.map());
    }

    /**
     * Updates task environment variables to reflect a reserved port value in the following places:
     * <ul>
     * <li>Environment variables in readiness check and/or health checks (if either is enabled)</li>
     * <li>Environment variables in the main task environment</li>
     * </ul>
     *
     * @throws TaskException in the event of an error when deserializing a readiness check
     */
    public static void setPort(
            TaskInfo.Builder taskInfoBuilder, String portName, Optional<String> customEnvKey, long port)
                    throws TaskException {
        String portEnvName = EnvUtils.getPortEnvName(portName, customEnvKey);
        String portStr = Long.toString(port);

        // 1. Update readiness check env, if any (embedded in task label):
        SchedulerLabelWriter labelWriter = new SchedulerLabelWriter(taskInfoBuilder);
        Optional<HealthCheck> readinessCheckOptional = labelWriter.getReadinessCheck();
        if (readinessCheckOptional.isPresent()) {
            // Update readiness check env (embedded in label):
            HealthCheck.Builder readinessCheck = readinessCheckOptional.get().toBuilder();
            TaskDataWriter readinesscheckWriter =
                    new TaskDataWriter(CommonEnvUtils.toMap(readinessCheck.getCommand().getEnvironment()));
            readinesscheckWriter.put(portEnvName, portStr);
            readinessCheck.getCommandBuilder().setEnvironment(CommonEnvUtils.toProto(readinesscheckWriter.map()));
            labelWriter.setReadinessCheck(readinessCheck.build());
        }
        taskInfoBuilder.setLabels(labelWriter.toProto());

        // 2. Update health check env, if any:
        if (taskInfoBuilder.hasHealthCheck()) {
            TaskDataWriter healthcheckWriter = new TaskDataWriter(
                    CommonEnvUtils.toMap(taskInfoBuilder.getHealthCheck().getCommand().getEnvironment()));
            healthcheckWriter.put(portEnvName, portStr);
            taskInfoBuilder.getHealthCheckBuilder().getCommandBuilder()
                    .setEnvironment(CommonEnvUtils.toProto(healthcheckWriter.map()));
        }

        // 3. Update main task env:
        TaskDataWriter taskWriter = new TaskDataWriter(
                CommonEnvUtils.toMap(taskInfoBuilder.getCommand().getEnvironment()));
        taskWriter.put(portEnvName, portStr);
        taskInfoBuilder.getCommandBuilder().setEnvironment(CommonEnvUtils.toProto(taskWriter.map()));
    }

    /**
     * Updates executor environment variables to reflect a reserved port value. Unlike with the task, this doesn't
     * affect any readiness or health checks.
     */
    public static void setPort(
            ExecutorInfo.Builder executorInfoBuilder, String portName, Optional<String> customEnvKey, long port) {
        TaskDataWriter taskWriter = new TaskDataWriter(
                CommonEnvUtils.toMap(executorInfoBuilder.getCommand().getEnvironment()));
        taskWriter.put(EnvUtils.getPortEnvName(portName, customEnvKey), Long.toString(port));
        executorInfoBuilder.getCommandBuilder().setEnvironment(CommonEnvUtils.toProto(taskWriter.map()));
    }
}
