package org.apache.hadoop.hdfs.server.datanode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.Workload;

public class WorkloadCollector {

	public static final Log LOG = LogFactory.getLog(WorkloadCollector.class);
	
	private static Map<ExtendedBlock, Integer> accessRecords = new HashMap<ExtendedBlock, Integer>();
	private static boolean enable = false;

	public static void reportReadOperation(ExtendedBlock block) {
		if (enable) {
			LOG.fatal("[Collector] record one read operation: " + block);
			int count = accessRecords.containsKey(block) ? accessRecords.get(block) : 0;
			accessRecords.put(block, count + 1);
		}
	}

	public static void clear() {
		accessRecords.clear();
	}

	public static synchronized List<Workload> pollWorkloads() {
		List<Workload> workloads = new ArrayList<Workload>(accessRecords.size());
		for (Map.Entry<ExtendedBlock, Integer> entry : accessRecords.entrySet()) {
			workloads.add(new Workload(entry.getKey(), entry.getValue()));
		}
		accessRecords.clear();
		return workloads;
	}

	public static void enable() {
		enable = true;
	}
	
	public static void disable() {
		enable = false;
	}
}
