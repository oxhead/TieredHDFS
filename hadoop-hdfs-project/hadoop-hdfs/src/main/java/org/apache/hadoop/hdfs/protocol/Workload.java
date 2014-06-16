package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Workload {
	private ExtendedBlock block;
	private int accessCount;

	public Workload(ExtendedBlock block, int accessCount) {
		this.block = block;
		this.accessCount = accessCount;
	}

	public ExtendedBlock getBlock() {
		return block;
	}

	public int getAccessCount() {
		return accessCount;
	}

}
