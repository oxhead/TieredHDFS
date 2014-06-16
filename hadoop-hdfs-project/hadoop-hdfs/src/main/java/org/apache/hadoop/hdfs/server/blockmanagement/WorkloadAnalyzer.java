package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.Workload;

public class WorkloadAnalyzer {
	private Context context;
	private Map<DatanodeDescriptor, Map<Block, Integer>> records = new HashMap<DatanodeDescriptor, Map<Block,Integer>>();
	
	public WorkloadAnalyzer(Context context) {
		this.context = context;
	}

	public void addRecord(DatanodeDescriptor node, List<Workload> workloads) {
		
		Map<Block, Integer> record = getNodeRecord(node);
		
		for (Workload workload : workloads) {
			int originalAccessCount = getAccessCount(record, workload.getBlock().getLocalBlock());
			record.put(workload.getBlock().getLocalBlock(), originalAccessCount + workload.getAccessCount());
		}
	}
	
	public Map<Block, Integer> getNodeRecord(DatanodeDescriptor node) {
		if (records.containsKey(node)) {
		    return records.get(node);
		} else {
			Map<Block, Integer> record = new HashMap<Block, Integer>();
			records.put(node, record);
			return record;
		}
	}
	
	public int getAccessCount(DatanodeDescriptor node, Block block) {
		return getAccessCount(getNodeRecord(node), block);
	}
	
	public int getAccessCount(Map<Block, Integer> record, Block block) {
		return record.containsKey(block) ? record.get(block) : 0;
	}
	
}


class BlockAccessRecord implements Comparable<BlockAccessRecord> {
	ExtendedBlock block;
	int accessTimes;

	public BlockAccessRecord(ExtendedBlock block, int accessTimes) {
		this.block = block;
		this.accessTimes = accessTimes;
	}

	@Override
	public int compareTo(BlockAccessRecord arg0) {
		return Integer.valueOf(this.accessTimes).compareTo(Integer.valueOf(arg0.accessTimes));
	}

	@Override
	public String toString() {
		return "BlockAccessRecord [block=" + block + ", accessTimes=" + accessTimes + "]";
	}
}