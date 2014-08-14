/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * Choose volumes in round-robin order.
 */
public class LoadawareVolumeChoosingPolicy<V extends FsVolumeSpi> extends AdvancedVolumeChoosingPolicy<V> {

	static final Log LOG = LogFactory.getLog(SSDFirstVolumeChoosingPolicy.class);

	private int previousIndex = 0;
	private Set<String> selectedVolumes = new HashSet<String>();
	private TreeSet<VolumeRecord> proportionalShares = new TreeSet<VolumeRecord>();

	@Override
	public synchronized V chooseVolume(final List<V> volumes, final long blockSize) throws IOException {
		return chooseVolume(volumes, blockSize, StorageType.ANY);
	}

	@Override
	public synchronized V chooseVolume(final List<V> volumes, final long blockSize, final StorageType storagePreference) throws IOException {
		if (previousIndex != getWorkloadCollector().getClock()) {
			previousIndex = getWorkloadCollector().getClock();
			selectedVolumes.clear();
			proportionalShares.clear();
			Map<V, Double> throughputRecords = new HashMap<V, Double>();
			for (V v : volumes) {
				throughputRecords.put(v, getWorkloadCollector().geAggregateWriteThroughput(v.getStorageID()));
			}
			LOG.fatal("# proportional shares at " + getWorkloadCollector().getClock());
			double minThroughput = Collections.min(throughputRecords.values());
			for (Map.Entry<V, Double> entry : throughputRecords.entrySet()) {
				int times = (int) Math.round(entry.getValue() / minThroughput);
				proportionalShares.add(new VolumeRecord(entry.getKey(), times));
				LOG.fatal("\tstorage=" + entry.getKey().getStorageID() + ", type=" + entry.getKey().getStorageType() + ", share=" + times + ", throughput=" + entry.getValue());
			}
		}
		if (volumes.size() < 1) {
			throw new DiskOutOfSpaceException("No more available volumes");
		}

		LOG.fatal("# of picked storage: " + selectedVolumes.size());

		List<V> unpikcedVolumns = getUnpickedVolumes(volumes);
		LOG.fatal("@ unpikced volumns: " + unpikcedVolumns.size());
		if (unpikcedVolumns.size() > 0) {
			List<PerformanceRecord> sortedRecordList = sortVolumnsByThroughput(unpikcedVolumns);
			for (PerformanceRecord record : sortedRecordList) {
				LOG.fatal("\t* record=" + record);
			}
			V pickedVolumn = sortedRecordList.get(0).volume;
			selectedVolumes.add(pickedVolumn.getStorageID());
			LOG.fatal("[1] pick volumn: " + pickedVolumn.getStorageID() + ", type=" + pickedVolumn.getStorageType() + ", clock=" + previousIndex);
			return pickedVolumn;
		}

		// already sorted from large to small
		for (VolumeRecord record : proportionalShares) {
			if (record.proportionalShare > 0) {
				LOG.fatal("@ choose storage=" + record.volume.getStorageID() + ", share=" + record.proportionalShare);
				record.proportionalShare--;
				LOG.fatal("[2] pick volumn: " + record.volume.getStorageID() + ", type=" + record.volume.getStorageType() + ", clock=" + previousIndex);
				return record.volume;
			}
		}

		selectedVolumes.clear();

		unpikcedVolumns = getUnpickedVolumes(volumes);
		if (unpikcedVolumns.size() > 0) {
			List<PerformanceRecord> sortedRecordList = sortVolumnsByThroughput(unpikcedVolumns);
			V pickedVolumn = sortedRecordList.get(0).volume;
			selectedVolumes.add(pickedVolumn.getStorageID());
			LOG.fatal("[3] pick volumn: " + pickedVolumn.getStorageID() + ", type=" + pickedVolumn.getStorageType() + ", clock=" + previousIndex);
			return pickedVolumn;
		}

		throw new DiskOutOfSpaceException("Out of space: The volume with the most available space is less than the block size (=" + blockSize + " B).");
	}

	private List<V> getUnpickedVolumes(List<V> volumns) {
		List<V> unpickedList = new LinkedList<V>();
		for (V v : volumns) {
			if (!selectedVolumes.contains(v.getStorageID())) {
				unpickedList.add(v);
			}
		}
		return unpickedList;
	}

	private List<PerformanceRecord> sortVolumnsByThroughput(List<V> volumns) {
		List<PerformanceRecord> sortedList = new ArrayList<PerformanceRecord>();
		for (V v : volumns) {
			double readThroughput = this.getWorkloadCollector().geAggregateReadThroughput(v.getStorageID());
			double writeThroughput = this.getWorkloadCollector().geAggregateWriteThroughput(v.getStorageID());
			sortedList.add(new PerformanceRecord(v, readThroughput, writeThroughput));
		}

		Collections.sort(sortedList);
		return sortedList;
	}

	class PerformanceRecord implements Comparable<PerformanceRecord> {

		V volume;
		double readThroughput = 0;
		double writeThroughput = 0;

		public PerformanceRecord(V volume, double readThroughput, double writeThroughput) {
			this.volume = volume;
			this.readThroughput = readThroughput;
			this.writeThroughput = writeThroughput;
		}

		@Override
		public int compareTo(PerformanceRecord o) {
			if (this.writeThroughput == o.writeThroughput) {
				return this.volume.getStorageType().equals(StorageType.SSD) ? -1 : 1;
			}
			return -new Double(writeThroughput).compareTo(new Double(o.writeThroughput));
		}

		@Override
		public String toString() {
			return "PerformanceRecord [volume=" + volume + ", storage=" + volume.getStorageID() + ", readThroughput=" + readThroughput + ", writeThroughput=" + writeThroughput + "]";
		}

	}

	class VolumeRecord implements Comparable<VolumeRecord> {
		V volume;
		int proportionalShare;

		public VolumeRecord(V volume, int proportionalShare) {
			this.volume = volume;
			this.proportionalShare = proportionalShare;
		}

		@Override
		public String toString() {
			return "VolumeRecord [volume=" + volume + ", proportionalShares=" + proportionalShares + "]";
		}

		@Override
		public int compareTo(VolumeRecord o) {
			if (this.proportionalShare == o.proportionalShare) {
				return this.volume.getStorageType().equals(StorageType.SSD) ? -1 : 1;
			}
			return -new Integer(this.proportionalShare).compareTo(new Integer(o.proportionalShare));
		}

	}

}
