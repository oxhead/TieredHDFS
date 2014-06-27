package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.StorageType;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Workload {
	private ExtendedBlock block;
	private String storageUuid;
	private StorageType storageType;
	private long timestamp;
	private long elapsedTime;
	private long offset;
	private long length;

	public Workload(ExtendedBlock block, String storageUuid, StorageType storageType, long timestamp, long elapsedTime, long offset, long length) {
		this.block = block;
		this.storageUuid = storageUuid;
		this.storageType = storageType;
		this.timestamp = timestamp;
		this.elapsedTime = elapsedTime;
		this.offset = offset;
		this.length = length;
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

}
