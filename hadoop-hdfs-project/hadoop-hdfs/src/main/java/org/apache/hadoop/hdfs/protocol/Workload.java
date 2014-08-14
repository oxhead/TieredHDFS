package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.StorageType;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Workload {

	public enum Type {
		READ, WRITE;
	}

	private Type type;
	private String datanodeUuid;
	private ExtendedBlock block;
	private String storageUuid;
	private StorageType storageType;
	private long timestamp;
	private long elapsedTime;
	private long offset;
	private long length;
	private String clientName;

	public Workload(Type type, String datanodeUuid, ExtendedBlock block, String storageUuid, StorageType storageType, long timestamp, long elapsedTime, long offset, long length, String clientName) {
		this.type = type;
		this.datanodeUuid = datanodeUuid;
		this.block = block;
		this.storageUuid = storageUuid;
		this.storageType = storageType;
		this.timestamp = timestamp;
		this.elapsedTime = elapsedTime;
		this.offset = offset;
		this.length = length;
		this.clientName = clientName;
	}

	public Type getType() {
		return type;
	}

	public String getDatanodeUuid() {
		return datanodeUuid;
	}

	public ExtendedBlock getBlock() {
		return block;
	}

	public String getStorageUuid() {
		return storageUuid;
	}

	public StorageType getStorageType() {
		return storageType;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getElapsedTime() {
		return elapsedTime;
	}

	public long getOffset() {
		return offset;
	}

	public long getLength() {
		return length;
	}

	public String getClientName() {
		return clientName;
	}

}
