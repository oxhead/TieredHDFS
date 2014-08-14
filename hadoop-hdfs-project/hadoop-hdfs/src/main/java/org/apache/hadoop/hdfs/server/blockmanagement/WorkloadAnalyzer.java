package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.Workload;

public class WorkloadAnalyzer {

	public static final Log LOG = LogFactory.getLog(WorkloadAnalyzer.class);

	private Context context;

	private Map<DatanodeDescriptor, Map<Block, Integer>> nodeRecords = new HashMap<DatanodeDescriptor, Map<Block, Integer>>();

	private File traceFile;
	private BufferedWriter trace;

	private long initTime = System.currentTimeMillis();

	private LinkedBlockingQueue<Workload> traceQueue = new LinkedBlockingQueue<Workload>();
	private Thread writeWorker = new Thread() {
		@Override
		public void run() {
			try {
				while (true) {
					LOG.fatal("getting trace data");
					Workload entry = traceQueue.take();
					long timePoint = (int) ((entry.getTimestamp() - initTime) / 1000);
					String fileName = context.namenode.getFilePath(entry.getBlock().getLocalBlock());
					String jobName = parseJobName(entry);
					String taskType = parseTaskType(entry);
					String record = String.format("%6s, %s, %s, %4s, %s, %12s, %4s, %6s, %10s, %12s, %8s, %s", timePoint, entry.getDatanodeUuid(), entry.getStorageUuid(), entry.getStorageType(), entry.getBlock().getBlockName(),
							entry.getType(), entry.getElapsedTime(), entry.getOffset(), entry.getLength(), jobName, taskType, fileName);
					trace.write(record);
					trace.newLine();
					trace.flush();
				}
			} catch (Exception e) {
				LOG.fatal("trace writer processing failed", e);
			}
		}
	};

	public WorkloadAnalyzer(Context context) {
		this.context = context;
		try {
			this.traceFile = File.createTempFile("workload_", ".trace", new File("/tmp"));
			LOG.fatal("trace file=" + traceFile);
			this.trace = new BufferedWriter(new FileWriter(traceFile));
			writeWorker.start();
		} catch (IOException e) {
			LOG.fatal("Unable to create the trace file", e);
		}
	}

	public void addRecords(DatanodeDescriptor node, List<Workload> workloads) {
		Map<Block, Integer> record = getNodeRecord(node);
		for (Workload workload : workloads) {
			int originalAccessCount = getAccessCount(record, workload.getBlock().getLocalBlock());
			record.put(workload.getBlock().getLocalBlock(), originalAccessCount + 1);
			LOG.fatal("add one workload record from node " + node.getHostName());
			traceQueue.add(workload);
		}
	}

	public Map<Block, Integer> getNodeRecord(DatanodeDescriptor node) {
		if (!nodeRecords.containsKey(node)) {
			nodeRecords.put(node, new HashMap<Block, Integer>());
		}
		return nodeRecords.get(node);
	}

	public int getAccessCount(DatanodeDescriptor node, Block block) {
		return getAccessCount(getNodeRecord(node), block);
	}

	public int getAccessCount(Map<Block, Integer> record, Block block) {
		return record.containsKey(block) ? record.get(block) : 0;
	}

	private String parseJobName(Workload workload) {
		String jobName = "null";
		try {
			jobName = workload.getClientName().split("_")[1];
		} catch (Exception e) {
		}
		return jobName.equalsIgnoreCase("null") ? "default" : jobName;
	}

	private String parseTaskType(Workload workload) {
		String taskType = "default";
		try {
			taskType = workload.getClientName().split("_")[5];
		} catch (Exception e) {
		}
		LOG.fatal("@ " + workload.getClientName() + ", " + taskType);
		return taskType.equalsIgnoreCase("m") ? "MAP" : taskType.equalsIgnoreCase("r") ? "REDUCE" : "default";
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
