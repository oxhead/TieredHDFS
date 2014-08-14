package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.Workload;

public class WorkloadCollector {

	public static final Log LOG = LogFactory.getLog(WorkloadCollector.class);

	private boolean enable = false;

	private List<Workload> accessRecrods = Collections.synchronizedList(new LinkedList<Workload>());
	private Map<String, StorageStatistics> statistics = new HashMap<String, StorageStatistics>();

	private AtomicInteger clock = new AtomicInteger();
	private ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

	BufferedWriter writer;

	class StorageStatistics {

		String storageId;
		int currentIndex = 0;
		int samplePeriod;

		long[] readBytes;
		long[] readTime;
		long[] writeBytes;
		long[] writeTime;

		double[] aggregateReadThroughputs;
		double[] aggregateWriteThroughputs;

		public StorageStatistics(String storageId) {
			this(storageId, 15);
		}

		public StorageStatistics(String storageId, int samplePeriod) {
			this.storageId = storageId;
			this.samplePeriod = samplePeriod;
			readBytes = new long[samplePeriod];
			readTime = new long[samplePeriod];
			writeBytes = new long[samplePeriod];
			writeTime = new long[samplePeriod];
			aggregateReadThroughputs = new double[samplePeriod];
			aggregateWriteThroughputs = new double[samplePeriod];

			for (int i = 0; i < samplePeriod; i++) {
				readBytes[i] = 0L;
				readTime[i] = 0L;
				writeBytes[i] = 0L;
				writeTime[i] = 0L;
				aggregateReadThroughputs[i] = 0.0;
				aggregateWriteThroughputs[i] = 0.0;
			}
		}

		public synchronized void addReadRecord(long bytes, long time) {
			readBytes[currentIndex] += bytes;
			readTime[currentIndex] += time;
		}

		public synchronized void addWriteRecord(long bytes, long time) {
			writeBytes[currentIndex] += bytes;
			writeTime[currentIndex] += time;
		}

		public double getReadThroughput() {
			return aggregateReadThroughputs[currentIndex];
		}

		public double getWriteThroughput() {
			return aggregateWriteThroughputs[currentIndex];
		}

		public double getAggregateReadThroughput() {
			double total = 0;
			for (double throughput : aggregateReadThroughputs) {
				total += throughput;
			}
			return total;
		}

		public double getAggregateWriteThroughput() {
			double total = 0;
			for (double throughput : aggregateWriteThroughputs) {
				total += throughput;
			}
			return total;
		}

		public boolean isReadPerformanceDowngrade() {
			int previousIndex = (currentIndex - 1 + samplePeriod ) % samplePeriod;
			if (aggregateReadThroughputs[currentIndex] >= aggregateReadThroughputs[previousIndex]) {
				return false;
			} else {
				return aggregateReadThroughputs[currentIndex] < aggregateReadThroughputs[currentIndex] * 0.9;
			}
		}

		public void calculate() {
			aggregateReadThroughputs[currentIndex] = readBytes[currentIndex] == 0 ? 0 : (readBytes[currentIndex] / 1048576.0) / (readTime[currentIndex] / 1000000000.0);
			aggregateWriteThroughputs[currentIndex] = writeBytes[currentIndex] == 0 ? 0 : (writeBytes[currentIndex] / 1048576.0) / (writeTime[currentIndex] / 1000000000.0);
		}


		private synchronized void reset() {
			readBytes[currentIndex] = 0;
			readTime[currentIndex] = 0;
			writeBytes[currentIndex] = 0;
			writeTime[currentIndex] = 0;
			aggregateReadThroughputs[currentIndex] = 0.0;
			aggregateWriteThroughputs[currentIndex] = 0.0;
		}

		public void step(int clock) {
			currentIndex = clock;
			reset();
		}

		public void printInfo() {
			writeWorkload(String.format("storage=%s, type=%s, readThroughput=%s(%s), writeThroughput=%s(%s)", this.storageId, this.getReadThroughput(), this.getAggregateReadThroughput(), this.getWriteThroughput(), this.getAggregateWriteThroughput()));
		}

	}

	public int getClock() {
		return clock.get();
	}

	public boolean isPerformanceDowngrade(String storageUuid) {
		StorageStatistics s = getStorageStatistics(storageUuid);
		return s.isReadPerformanceDowngrade();
	}

	public double getAverageReadThroughput(String storageUuid) {
		StorageStatistics s = getStorageStatistics(storageUuid);
		return s.getReadThroughput();
	}

	public double getAverageWriteThroughput(String storageUuid) {
		StorageStatistics s = getStorageStatistics(storageUuid);
		return s.getWriteThroughput();
	}
	
	public double geAggregateReadThroughput(String storageUuid) {
		StorageStatistics s = getStorageStatistics(storageUuid);
		return s.getAggregateReadThroughput();
	}

	public double geAggregateWriteThroughput(String storageUuid) {
		StorageStatistics s = getStorageStatistics(storageUuid);
		return s.getAggregateWriteThroughput();
	}

	public StorageStatistics getStorageStatistics(String storageUuid) {
		if (!statistics.containsKey(storageUuid)) {
			statistics.put(storageUuid, new StorageStatistics(storageUuid));
		}
		return statistics.get(storageUuid);
	}

	public void reportReadOperation(String datanodeUuid, String storageUuid, StorageType storageType, ExtendedBlock block, long offset, long length, String clientName, String remoteAddress,
			long timestamp, long elapsedTime) {
		if (enable) {
			Workload record = new Workload(Workload.Type.READ, datanodeUuid, block, storageUuid, storageType, timestamp, elapsedTime, offset, length, clientName);
			accessRecrods.add(record);
			getStorageStatistics(storageUuid).addReadRecord(length, elapsedTime);
		}
	}

	public void reportWriteOperation(String datanodeUuid, String storageUuid, StorageType storageType, ExtendedBlock block, long offset, long length, String clientName, String remoteAddress,
			long timestamp, long elapsedTime) {
		if (enable) {
			Workload record = new Workload(Workload.Type.WRITE, datanodeUuid, block, storageUuid, storageType, timestamp, elapsedTime, offset, length, clientName);
			accessRecrods.add(record);
			getStorageStatistics(storageUuid).addWriteRecord(length, elapsedTime);
		}
	}

	public void writeWorkload(String s) {
		try {
			writer.write(s);
			writer.newLine();
			writer.flush();
		} catch (IOException e) {
			LOG.fatal("cannot write workload to file: " + s, e);
		}
	}

	public void initialize() throws IOException {
		enable();
		writer = new BufferedWriter(new FileWriter("/tmp/workload.log"));
		service.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {

				for (Map.Entry<String, StorageStatistics> entry : statistics.entrySet()) {
					StorageStatistics s = entry.getValue();
					s.calculate();
					s.printInfo();
				}

				clock.set(clock.incrementAndGet() % 15);
				for (Map.Entry<String, StorageStatistics> entry : statistics.entrySet()) {
					StorageStatistics s = entry.getValue();
					s.step(clock.get());
				}
			}
		}, 0, 1, TimeUnit.SECONDS);
	}

	public List<Workload> pollWorkloads() {
		List<Workload> workloads = new ArrayList<Workload>(accessRecrods.size());
		workloads.addAll(accessRecrods);
		accessRecrods.clear();
		LOG.fatal("poll workload records: " + workloads.size());
		return workloads;
	}

	public void enable() {
		enable = true;
	}

	public void disable() {
		enable = false;
	}
}