package com.mesosphere.sdk.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo.Source;

import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelReader;
import com.mesosphere.sdk.testutils.TestConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ResourceUtilsTest {

    @Test
    public void testCreateDesiredMountVolume() {
        Resource desiredMountVolume = ResourceUtils.getDesiredMountVolume(
                TestConstants.ROLE,
                TestConstants.PRINCIPAL,
                1000,
                TestConstants.CONTAINER_PATH);
        Assert.assertNotNull(desiredMountVolume);
        Assert.assertTrue(desiredMountVolume.getDisk().hasPersistence());
        Assert.assertEquals("", desiredMountVolume.getDisk().getPersistence().getId());
        Assert.assertEquals("", new ResourceRequirement(desiredMountVolume).getResourceId());
        Assert.assertEquals(Source.Type.MOUNT, desiredMountVolume.getDisk().getSource().getType());
    }

    @Test
    public void testCreateDesiredRootVolume() {
        Resource desiredRootVolume = ResourceUtils.getDesiredRootVolume(
                TestConstants.ROLE,
                TestConstants.PRINCIPAL,
                1000,
                TestConstants.CONTAINER_PATH);
        Assert.assertNotNull(desiredRootVolume);
        Assert.assertTrue(desiredRootVolume.getDisk().hasPersistence());
        Assert.assertEquals("", desiredRootVolume.getDisk().getPersistence().getId());
        Assert.assertEquals("", new ResourceRequirement(desiredRootVolume).getResourceId());
        Assert.assertFalse(desiredRootVolume.getDisk().hasSource());
    }

    @Test
    public void testGetUnreservedRanges() {
        List<Protos.Value.Range> testRanges = getTestRanges();
        Resource resource = ResourceUtils.getUnreservedRanges("ports", testRanges);
        validateRanges(testRanges, resource.getRanges().getRangeList());
    }

    @Test
    public void testGetDesiredRanges() {
        List<Protos.Value.Range> testRanges = getTestRanges();
        Resource resource = ResourceUtils.getDesiredRanges(
                TestConstants.ROLE,
                TestConstants.PRINCIPAL,
                "ports",
                testRanges);

        validateRanges(testRanges, resource.getRanges().getRangeList());
        validateRolePrincipal(resource);
    }

    @Test
    public void testGetExpectedRanges() {
        String expectedResourceId = UUID.randomUUID().toString();
        List<Protos.Value.Range> testRanges = getTestRanges();
        Resource resource = ResourceUtils.getExpectedRanges(
                "ports",
                testRanges,
                expectedResourceId,
                TestConstants.ROLE,
                TestConstants.PRINCIPAL);

        validateRanges(testRanges, resource.getRanges().getRangeList());
        validateRolePrincipal(resource);
        Assert.assertEquals(expectedResourceId, new SchedulerResourceLabelReader(resource).getResourceId().get());
    }

    private void validateRanges(List<Protos.Value.Range> expectedRanges, List<Protos.Value.Range> actualRanges) {
        Assert.assertEquals(expectedRanges.size(), actualRanges.size());

        for (int i=0; i<expectedRanges.size(); i++) {
            Assert.assertEquals(expectedRanges.get(i), actualRanges.get(i));
        }
    }

    private void validateRolePrincipal(Resource resource) {
        Assert.assertEquals(TestConstants.ROLE, resource.getRole());
        Assert.assertEquals(TestConstants.PRINCIPAL, resource.getReservation().getPrincipal());
    }

    public List<Protos.Value.Range> getTestRanges() {
        long begin0 = 1;
        long end0 = 10;
        Protos.Value.Range range0 = Protos.Value.Range.newBuilder()
                .setBegin(begin0)
                .setEnd(end0)
                .build();

        long begin1 = 20;
        long end1 = 30;
        Protos.Value.Range range1 = Protos.Value.Range.newBuilder()
                .setBegin(begin1)
                .setEnd(end1)
                .build();

        return Arrays.asList(range0, range1);
    }
}
