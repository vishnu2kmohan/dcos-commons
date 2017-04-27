package com.mesosphere.sdk.offer;

/**
 * This class encapsulates constants of relevance to SDK Scheduler internals.
 *
 * @see com.mesosphere.sdk.offer.taskdata.EnvConstants
 * @see com.mesosphere.sdk.offer.taskdata.LabelConstants
 */
public class Constants {

    /** The name used for the task deployment plan. */
    public static final String DEPLOY_PLAN_NAME = "deploy";

    /** The name used for reserved network port resources. */
    public static final String PORTS_RESOURCE_TYPE = "ports";
    /** The name used for reserved storage/disk resources. */
    public static final String DISK_RESOURCE_TYPE = "disk";
}
