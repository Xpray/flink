package org.apache.flink.table.interactive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.TableFactory;

/**
 * Describe a TableFactory for FlinkTableServiceSource and FlinkTableServiceSink.
 */
public class FlinkTableServiceFactoryDescriptor {
	private TableFactory tableFactory;

	/**
	 * Properties for FlinkTableServiceSource and FlinkTableServiceSink.
	 */
	private Configuration configuration;

	public FlinkTableServiceFactoryDescriptor(TableFactory tableFactory, Configuration configuration) {
		this.tableFactory = tableFactory;
		this.configuration = configuration;
	}

	public TableFactory getTableFactory() {
		return tableFactory;
	}

	public void setTableFactory(TableFactory tableFactory) {
		this.tableFactory = tableFactory;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
