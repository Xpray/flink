package org.apache.flink.api.common;

import org.apache.flink.util.AbstractID;

import java.io.Serializable;
import java.net.InetAddress;

public class ResultLocation implements Serializable {

	private final InetAddress address;

	private final int dataPort;

	private final AbstractID producerId;

	public ResultLocation(InetAddress address, int dataPort, AbstractID producerId) {
		this.address = address;
		this.dataPort = dataPort;
		this.producerId = producerId;
	}

	public InetAddress getAddress() {
		return address;
	}

	public int getDataPort() {
		return dataPort;
	}

	public AbstractID getProducerId() {
		return producerId;
	}
}
