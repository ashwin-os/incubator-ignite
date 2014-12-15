/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.loadbalancing.weightedrandom;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management MBean for {@link WeightedRandomLoadBalancingSpi} SPI.
 */
@IgniteMBeanDescription("MBean that provides access to weighted random load balancing SPI configuration.")
public interface WeightedRandomLoadBalancingSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Checks whether node weights are considered when doing
     * random load balancing.
     *
     * @return If {@code true} then random load is distributed according
     *      to node weights.
     */
    @IgniteMBeanDescription("Whether node weights are considered when doing random load balancing.")
    public boolean isUseWeights();

    /**
     * Gets weight of this node.
     *
     * @return Weight of this node.
     */
    @IgniteMBeanDescription("Weight of this node.")
    public int getNodeWeight();
}