package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.Workload;

public class WorkloadCollector {

	public static final Log LOG = LogFactory.getLog(WorkloadCollector.class);

	private boolean enable = false;

	private LinkedList<Workload> accessRecrods = new LinkedList<Workload>();
	private Lock lock = new ReentrantLock();
	
	private long initTime;
	private WorkloadCollection[] workloadRecord = new WorkloadCollection[86400];

	private BufferedWriter write;
	private LinkedBlockingDeque<WorkloadDetail> queue = new LinkedBlockingDeque<WorkloadDetail>();
	private Thread worker = new Thread() {

		@Override
		public void run() {
			try {
				while (true) {
					WorkloadDetail entry = queue.take();
					int timePoint = (int) ((entry.getTimestamp() - initTime) / 1000);
					if (workloadRecord[timePoint] == null) {
						workloadRecord[timePoint] = new WorkloadCollection();
					}
					workloadRecord[timePoint].addWorkload(entry);
					String record = String.format("%6s, %s, %4s, %s, %12s, %6s, %6s", timePoint, entry.getStorageID(), entry.getStorageType(), entry.getBlock().getBlockName(), entry.getElapsedTime(),
							entry.getOffset(), entry.getLength());
					write.write(record);
					write.newLine();
					write.flush();
				}
			} catch (Exception e) {
				LOG.fatal("[Collector] processing failed", e);
			}
		}
	};

	public void reportReadOperation(String datanodeUUID, String storageUuid, StorageType storageType, ExtendedBlock block, long offset, long length, String clientName, String remoteAddress,
			long timestamp, long elapsedTime) {
		if (enable) {
			Workload record = new Workload(block, storageUuid, storageType, timestamp, elapsedTime, offset, length, clientName);
			lock.lock();
			accessRecrods.add(record);
			lock.unlock();
			WorkloadDetail entry = new WorkloadDetail(timestamp, elapsedTime, offset, length, block, datanodeUUID, storageUuid, storageType.toString());
			queue.add(entry);
		}
	}

	public void reportWriteOperation(String datanodeUUID, String storageUuid, StorageType storageType, ExtendedBlock block, long offset, long length, String clientName, String remoteAddress,
			long timestamp, long elapsedTime) {
		if (enable) {
			Workload record = new Workload(block, storageUuid, storageType, timestamp, elapsedTime, offset, length, clientName);
			lock.lock();
			accessRecrods.add(record);
			lock.unlock();
			WorkloadDetail entry = new WorkloadDetail(timestamp, elapsedTime, offset, length, block, datanodeUUID, storageUuid, storageType.toString());
			queue.add(entry);
		}
	}

	public void initialize() throws IOException {
		enable();
		initTime = System.currentTimeMillis();
		File traceFile = File.createTempFile("workload_", ".trace", new File("/tmp"));
		write = new BufferedWriter(new FileWriter(traceFile));
		LOG.fatal("[Collector] trace file=" + traceFile);
		worker.start();
	}

	public List<Workload> pollWorkloads() {
		List<Workload> workloads = new ArrayList<Workload>(accessRecrods.size());
		Workload workload = null;
		while ((workload = accessRecrods.poll()) != null) {
			workloads.add(workload);
		}
		return workloads;
	}

	public void enable() {
		enable = true;
	}

	public void disable() {
		enable = false;
	}
}

class WorkloadDetail {
	private long timestamp;
	private long elapsedTime;
	private long offset;
	private long length;
	private ExtendedBlock block;
	private String datanodeUUID;
	private String storageID;
	private String storageType;

	public WorkloadDetail(long timestamp, long elapsedTime, long offset, long length, ExtendedBlock block, String datanodeUUID, String storageID, String storageType) {
		this.timestamp = timestamp;
		this.elapsedTime = elapsedTime;
		this.offset = offset;
		this.length = length;
		this.block = block;
		this.datanodeUUID = datanodeUUID;
		this.storageID = storageID;
		this.storageType = storageType;
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

	public ExtendedBlock getBlock() {
		return block;
	}

	public String getDatanodeUUID() {
		return datanodeUUID;
	}

	public String getStorageID() {
		return storageID;
	}

	public String getStorageType() {
		return storageType;
	}

}

class WorkloadCollection {
	private List<WorkloadDetail> list = new LinkedList<WorkloadDetail>();

	public void addWorkload(WorkloadDetail entry) {
		list.add(entry);
	}

	public List<WorkloadDetail> getWorkloads() {
		return list;
	}
}
