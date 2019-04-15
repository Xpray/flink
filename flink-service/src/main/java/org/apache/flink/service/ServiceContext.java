package org.apache.flink.service;

import org.apache.flink.metrics.MetricGroup;

/**
 * interface of accessing runtime properties for User Defined Service.
 */
public interface ServiceContext {

	/**
	 * Access the {@link MetricGroup}.
	 * @return MetricGroup of the environment.
	 */
	MetricGroup getMetricGroup();

	/**
	 * Get the number of instances of the service.
	 * @return number of instances of the service.
	 */
	int getNumberOfInstances();

	/**
	 * Get the index of this instance, start with 0.
	 * @return index of this instance.
	 */
	int getIndexOfCurrentInstance();
}
