package com.mesosphere.sdk.testutils;

import org.apache.mesos.Protos;

import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.offer.taskdata.SchedulerEnvWriter;
import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 * This class provides utility method for Tests concerned with Tasks.
 */
public class TaskTestUtils {
    private static Random random = new Random();
    public static Protos.TaskInfo getTaskInfo(Protos.Resource resource) {
        return getTaskInfo(Arrays.asList(resource));
    }

    public static Protos.TaskInfo getTaskInfo(List<Protos.Resource> resources, Integer index) {
        Protos.TaskInfo.Builder taskBuilder = Protos.TaskInfo.newBuilder()
                .setTaskId(TestConstants.TASK_ID)
                .setName(TestConstants.TASK_NAME)
                .setSlaveId(TestConstants.AGENT_ID)
                .setCommand(TestConstants.COMMAND_INFO)
                .setContainer(TestConstants.CONTAINER_INFO)
                .setLabels(TestConstants.getRequiredTaskLabels(index));
        List<Protos.Resource> updatedResources = new ArrayList<>();
        for (Protos.Resource r : resources) {
            String resourceId = "";
            String dynamicPortAssignment = null;
            String vipAssignment = null;
            for (Protos.Label l : r.getReservation().getLabels().getLabelsList()) {
                if (Objects.equals(l.getKey(), "resource_id")) {
                   resourceId = l.getValue();
                } else if (Objects.equals(l.getKey(), TestConstants.HAS_DYNAMIC_PORT_ASSIGNMENT_LABEL)) {
                    dynamicPortAssignment = l.getValue();
                } else if (Objects.equals(l.getKey(), TestConstants.HAS_VIP_LABEL)) {
                    vipAssignment = l.getValue();
                }
            }

            if (Objects.equals(r.getName(), "ports")) {
                Long portValue = dynamicPortAssignment == null ?
                        r.getRanges().getRange(0).getBegin() : Long.parseLong(dynamicPortAssignment);
                if (!resourceId.isEmpty()) {
                    // Update task env(s), and resource labels:
                    try {
                        SchedulerEnvWriter.setPort(
                                taskBuilder, "some-port", Optional.of(TestConstants.PORT_ENV_NAME), portValue);
                    } catch (TaskException e) {
                        throw new IllegalStateException(e);
                    }
                    r = new SchedulerResourceLabelWriter(r).setPortValue("some-port", portValue).toProto();

                    if (vipAssignment != null) {
                        taskBuilder.getDiscoveryBuilder()
                                .setVisibility(Protos.DiscoveryInfo.Visibility.CLUSTER)
                                .setName(taskBuilder.getName())
                                .getPortsBuilder().addPortsBuilder()
                                        .setNumber((int) (long) portValue)
                                        .getLabelsBuilder().addLabelsBuilder()
                                                .setKey("VIP_" + UUID.randomUUID().toString())
                                                .setValue(vipAssignment);
                    }
                }
            }
            updatedResources.add(r);
        }
        return taskBuilder.addAllResources(updatedResources).build();
    }

    // This suppression is OK in test code only.  There's a chance you hit MIN_VALUE
    // of Integer, and then you don't actually get a positive integer when it's absolute
    // valued.  Read Math.abs docs for more information.  It's extremely unlikely, and
    // working around it is more code than it's worth.
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("RV_ABSOLUTE_VALUE_OF_RANDOM_INT")
    public static Protos.TaskInfo getTaskInfo(List<Protos.Resource> resources) {
        return getTaskInfo(resources, Math.abs(random.nextInt()));
    }

    public static Protos.ExecutorInfo getExecutorInfo(Protos.Resource resource) {
        return getExecutorInfo(Arrays.asList(resource));
    }

    public static Protos.ExecutorInfo getExecutorInfo(List<Protos.Resource> resources) {
        return getExecutorInfoBuilder().addAllResources(resources).build();
    }

    public static Protos.ExecutorInfo getExistingExecutorInfo(Protos.Resource resource) {
        return getExecutorInfoBuilder()
                .addResources(resource)
                .setExecutorId(TestConstants.EXECUTOR_ID)
                .build();
    }

    public static Protos.Environment.Variable createEnvironmentVariable(String key, String value) {
        return Protos.Environment.Variable.newBuilder().setName(key).setValue(value).build();
    }

    private static Protos.ExecutorInfo.Builder getExecutorInfoBuilder() {
        Protos.CommandInfo cmd = Protos.CommandInfo.newBuilder().build();
        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(""))
                .setName(TestConstants.EXECUTOR_NAME)
                .setCommand(cmd);
    }

    public static Protos.TaskStatus generateStatus(
            Protos.TaskID taskID,
            Protos.TaskState taskState) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(taskID)
                .setState(taskState)
                .build();
    }
}
