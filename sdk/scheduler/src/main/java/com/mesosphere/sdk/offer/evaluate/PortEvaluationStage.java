package com.mesosphere.sdk.offer.evaluate;

import com.mesosphere.sdk.offer.*;
import com.mesosphere.sdk.offer.taskdata.SchedulerEnvWriter;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelReader;
import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelReader;
import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.mesosphere.sdk.offer.evaluate.EvaluationOutcome.*;

/**
 * This class evaluates an offer for a single port against an {@link OfferRequirement}, finding a port dynamically
 * in the offer where specified by the framework and modifying {@link org.apache.mesos.Protos.TaskInfo} and
 * {@link org.apache.mesos.Protos.ExecutorInfo} where appropriate so that the port is available in their respective
 * environments.
 */
public class PortEvaluationStage extends ResourceEvaluationStage {
    private static final Logger LOGGER = LoggerFactory.getLogger(PortEvaluationStage.class);

    private final String portName;
    private final int port;
    private final Optional<String> customEnvKey;

    private String resourceId;

    public PortEvaluationStage(
            Protos.Resource resource, String taskName, String portName, int port, Optional<String> customEnvKey) {
        super(resource, taskName);
        this.portName = portName;
        this.port = port;
        this.customEnvKey = customEnvKey;
    }

    @Override
    public EvaluationOutcome evaluate(MesosResourcePool mesosResourcePool, PodInfoBuilder podInfoBuilder) {
        int portToUse;
        if (port != 0) {
            // Fixed port
            portToUse = port;
        } else {
            // Dynamic port: reuse previous selection if any, or do a new selection
            Optional<Integer> taskPort = getCurrentDynamicPortValue(podInfoBuilder);
            if (taskPort.isPresent()) {
                // Keep/reuse the existing port.
                portToUse = taskPort.get();
            } else {
                // No previous port was found, select a port from what's available.
                Optional<Integer> dynamicPort = selectDynamicPort(mesosResourcePool, podInfoBuilder);
                if (!dynamicPort.isPresent()) {
                    return fail(this,
                            "No ports were available for dynamic claim in offer: %s",
                            mesosResourcePool.getOffer().toString());
                }

                portToUse = dynamicPort.get();
            }
        }

        // If this is not the first port evaluation stage in this evaluation run, and this is a new pod being launched,
        // we want to use the reservation ID we created for the first port in this cycle for all subsequent ports.
        try {
            Protos.Resource resource = getTaskName().isPresent()
                    ? ResourceUtils.getResource(
                            podInfoBuilder.getTaskBuilder(getTaskName().get()), Constants.PORTS_RESOURCE_TYPE)
                    : ResourceUtils.getResource(
                            podInfoBuilder.getExecutorBuilder().get(), Constants.PORTS_RESOURCE_TYPE);
            resourceId = new SchedulerResourceLabelReader(resource).getResourceId().get();
        } catch (IllegalArgumentException e) {
            // There have been no previous ports in this evaluation cycle, so there's no resource on the task builder
            // to get the resource id from.
            resourceId = "";
        }
        super.setResourceRequirement(getPortRequirement(getResourceRequirement(), portToUse));

        return super.evaluate(mesosResourcePool, podInfoBuilder);
    }

    @Override
    protected void setProtos(PodInfoBuilder podInfoBuilder, Protos.Resource resource) {
        long port = resource.getRanges().getRange(0).getBegin();
        if (!getTaskName().isPresent()) {
            return; // Changes are only applied to TaskInfos
        }
        String taskName = getTaskName().get();
        Protos.TaskInfo.Builder taskBuilder = podInfoBuilder.getTaskBuilder(taskName);

        // Update resource labels, or add new resource entry if a match isn't found:
        List<Protos.Resource> allResources = new ArrayList<>();
        boolean foundMatchingResource = false;
        for (Resource.Builder r : taskBuilder.getResourcesBuilderList()) {
            if (r.getName().equals(resource.getName())) {
                allResources.add(new SchedulerResourceLabelWriter(r).setPortValue(portName, port).toProto());
                foundMatchingResource = true;
            } else {
                allResources.add(r.build());
            }
        }
        if (!foundMatchingResource) {
            allResources.add(new SchedulerResourceLabelWriter(resource).setPortValue(portName, port).toProto());
        }
        taskBuilder
                .clearResources()
                .addAllResources(allResources);

        // Update advertised port in task env (and readiness/health check env if applicable):
        try {
            SchedulerEnvWriter.setPort(taskBuilder, portName, customEnvKey, port);
        } catch (TaskException e) {
            LOGGER.error(String.format(
                    "Failed to add PORT envvar to Task %s", taskBuilder.getName()), e);
        }
    }

    @Override
    protected Protos.Resource toFulfilledResource(Protos.Resource resource) {
        Protos.Resource reservedResource = super.toFulfilledResource(resource);
        if (!StringUtils.isBlank(resourceId)) {
            reservedResource = ResourceBuilder.fromExistingResource(reservedResource)
                    .setResourceId(resourceId)
                    .build();
        }
        return reservedResource;
    }

    private Optional<Integer> getCurrentDynamicPortValue(PodInfoBuilder podInfoBuilder) {
        // Figures out the current value, if any, that's being used for a requested dynamic port reservation.
        if (!getTaskName().isPresent()) {
            // We don't assign ports at the executor level. Nothing to return.
            return Optional.empty();
        }
        Protos.TaskInfo.Builder taskBuilder = podInfoBuilder.getTaskBuilder(getTaskName().get());
        return SchedulerLabelReader.getDynamicPortValue(taskBuilder.getResourcesList(), portName);
    }

    private static Optional<Integer> selectDynamicPort(
            MesosResourcePool mesosResourcePool, PodInfoBuilder podInfoBuilder) {
        // We don't want to accidentally dynamically consume a port that's explicitly claimed by this pod, so compile a
        // list of those to check against the offered ports.
        Set<Integer> consumedPorts = new HashSet<>();
        for (Protos.Resource resource : podInfoBuilder.getOfferRequirement().getResources()) {
            if (resource.getName().equals(Constants.PORTS_RESOURCE_TYPE)) {
                resource.getRanges().getRangeList().stream()
                        .flatMap(r -> IntStream.rangeClosed((int) r.getBegin(), (int) r.getEnd()).boxed())
                        .filter(p -> p != 0)
                        .forEach(consumedPorts::add);
            }
        }

        // Also check other dynamically allocated ports set by other evaluation stages.
        for (Protos.Resource.Builder resourceBuilder : podInfoBuilder.getResourceBuilders()) {
            if (resourceBuilder.getName().equals(Constants.PORTS_RESOURCE_TYPE)) {
                resourceBuilder.getRanges().getRangeList().stream()
                        .flatMap(r -> IntStream.rangeClosed((int) r.getBegin(), (int) r.getEnd()).boxed())
                        .filter(p -> p != 0)
                        .forEach(consumedPorts::add);
            }
        }

        Protos.Value availablePorts = mesosResourcePool.getUnreservedMergedPool().get(Constants.PORTS_RESOURCE_TYPE);
        Optional<Integer> dynamicPort = Optional.empty();
        if (availablePorts != null) {
            dynamicPort = availablePorts.getRanges().getRangeList().stream()
                    .flatMap(r -> IntStream.rangeClosed((int) r.getBegin(), (int) r.getEnd()).boxed())
                    .filter(p -> !consumedPorts.contains(p))
                    .findFirst();
        }

        return dynamicPort;
    }

    private static ResourceRequirement getPortRequirement(ResourceRequirement resourceRequirement, int port) {
        Protos.Resource.Builder builder = resourceRequirement.getResource().toBuilder();
        builder.clearRanges().getRangesBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(port).setEnd(port));

        return new ResourceRequirement(builder.build());
    }
}
