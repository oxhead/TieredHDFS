package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

public class TieredStorageManager {

	static final Log LOG = LogFactory.getLog(TieredStorageManager.class);

	private ScheduledExecutorService serviceExecutor = Executors.newScheduledThreadPool(1);
	private Configuration conf;

	private BlockManager blockManager;
	private FSNamesystem fsNamesystem;
	private NameNodeConnector nnc;

	Random random = new Random();

	private MigrationService migrationService;

	public TieredStorageManager(Configuration conf, BlockManager blockManager, FSNamesystem fsNamesystem) {
		this.conf = conf;
		this.blockManager = blockManager;
		this.fsNamesystem = fsNamesystem;
		this.nnc = new NameNodeConnector(this.fsNamesystem, this.blockManager, conf);
		this.migrationService = new MigrationService(this.nnc);
	}

	public void start() {
		LOG.fatal("Service started");
		serviceExecutor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				TieredStorageManager.this.run();
			}
		}, 60, 15, TimeUnit.SECONDS);
	}

	public boolean checkBlockExists(DatanodeDescriptor node, List<DatanodeStorageInfo> storageInfos) {
		for (DatanodeStorageInfo info : storageInfos) {
			if (info.getDatanodeDescriptor().equals(node)) {
				return true;
			}
		}
		return false;

	}

	// calculate data size in SSD
	private boolean requireMigration() {
		int totalBlocks = 0;
		for (DatanodeDescriptor dd : this.nnc.getDatanodes()) {
			totalBlocks += dd.numBlocks(StorageType.SSD);
		}
		// 64MB * 16 = 1GB
		return totalBlocks > 16;
	}

	private void removeAccessRecord(ExtendedBlock block) {
		fsNamesystem.getBlockAccessRecord().remove(block);
	}

	public TreeSet<BlockAccessRecord> getLeaseUsedBlocks() {
		Map<ExtendedBlock, Integer> accessRecords = fsNamesystem.getBlockAccessRecord();
		TreeSet<BlockAccessRecord> records = new TreeSet<BlockAccessRecord>();
		for (Map.Entry<ExtendedBlock, Integer> entry : accessRecords.entrySet()) {
			BlockAccessRecord record = new BlockAccessRecord(entry.getKey(), entry.getValue());
			records.add(record);
		}
		return records;
	}

	public DatanodeDescriptor pickOneNode(List<DatanodeDescriptor> nodes, List<DatanodeStorageInfo> excludeStorages) {
		List<DatanodeDescriptor> validNodes = new LinkedList<DatanodeDescriptor>(nodes);
		for (DatanodeStorageInfo dsInfo : excludeStorages) {
			validNodes.remove(dsInfo.getDatanodeDescriptor());
		}
		if (validNodes.size() < 1) {
			return null;
		}
		return validNodes.get(randInt(validNodes.size()));
	}

	private DatanodeStorageInfo pickOneStorage(DatanodeDescriptor sourceNode, StorageType storageType) {
		List<DatanodeStorageInfo> dsInfos = new LinkedList<DatanodeStorageInfo>();
		for (DatanodeStorageInfo info : sourceNode.getStorageInfos()) {
			if (info.getStorageType().equals(storageType)) {
				dsInfos.add(info);
			}
		}
		if (dsInfos.size() < 1) {
			return null;
		}
		return dsInfos.get(randInt(dsInfos.size()));
	}

	public void run() {
		LOG.fatal("Manager thread runs");
		try {
			if (!requireMigration()) {
				LOG.fatal("Doesn't require migration");
				return;
			}

			List<DatanodeDescriptor> datanodes = nnc.getDatanodes();
			if (datanodes.size() < 1) {
				LOG.fatal("No datanodes available");
				return;
			}

			LOG.fatal("[Tier] start evaluating");
			// consider only SSD blocks
			TreeSet<BlockAccessRecord> leastUsedBlocks = getLeaseUsedBlocks();

			int maxMigrateBlocks = (int) (0.1 * leastUsedBlocks.size());
			int migrateBlocks = 0;
			LOG.fatal("[Tier] max migrated blocks=" + maxMigrateBlocks);

			Iterator<BlockAccessRecord> iterator = leastUsedBlocks.iterator();
			while (migrateBlocks < maxMigrateBlocks && iterator.hasNext()) {
				BlockAccessRecord blockRecord = iterator.next();
				LOG.fatal("[Tier] LRU=" + blockRecord);
				if (migrationService.isMigrating(blockRecord.block.getLocalBlock())) {
					LOG.fatal("[Tier] block is already migrating: " + blockRecord);
					continue;
				}
				Block block = blockRecord.block.getLocalBlock();
				// get all block locations
				List<DatanodeStorageInfo> locations = nnc.getLocationInfos(block);
				for (DatanodeStorageInfo dsInfo : locations) {
					// choose only the SSD block. We have only one replication.
					// Should'd be a problem.
					if (dsInfo.getStorageType().equals(StorageType.SSD)) {
						// source
						DatanodeDescriptor sourceNode = dsInfo.getDatanodeDescriptor();

						// dest + storage
						// need to check null
						DatanodeDescriptor destNode = pickOneNode(datanodes, locations);
						DatanodeStorageInfo storage = pickOneStorage(destNode, StorageType.DISK);

						removeAccessRecord(blockRecord.block);
						migrateBlocks = migrateBlocks + 1;
						migrationService.addTask(block, sourceNode, destNode, storage);
					}
				}
			}

		} catch (Exception ex) {
			LOG.fatal("[move] unable to complete task", ex);
		}

	}

	public void printArray(Object[] array) {
		for (Object o : array) {
			LOG.fatal("\t" + o);
		}
	}

	public int randInt(int len) {
		return randInt(0, len);
	}

	public int randInt(int start, int end) {
		return start + random.nextInt(end - start);
	}

	public String pickStorageID(DatanodeDescriptor node, StorageType storgeType) {
		for (DatanodeStorageInfo info : node.getStorageInfos()) {
			if (info.getStorageType().equals(storgeType)) {
				return info.getStorageID();
			}
		}
		return "";
	}

	public List<DatanodeDescriptor> getNodesWithStorage(DatanodeDescriptor original, List<DatanodeDescriptor> list, StorageType storageType) {
		List<DatanodeDescriptor> newList = new ArrayList<DatanodeDescriptor>(list);
		int index = -1;
		for (int i = 0; i < newList.size(); i++) {
			if (newList.get(i).getHostName().equals(original.getHostName())) {
				index = i;
			}
		}
		newList.remove(index);

		Iterator<DatanodeDescriptor> iterator = newList.iterator();
		while (iterator.hasNext()) {
			DatanodeDescriptor dd = iterator.next();
			boolean found = false;
			for (DatanodeStorageInfo info : dd.getStorageInfos()) {
				if (info.getStorageType().equals(storageType)) {
					found = true;
					continue;
				}
			}
			if (!found) {
				iterator.remove();
			}
		}
		return newList;
	}

	public void stop() {
		serviceExecutor.shutdown();
	}

}

class NameNodeConnector {
	private Log LOG = LogFactory.getLog(NameNodeConnector.class);

	private FSNamesystem fsNamesystem;
	private BlockManager blockManager;
	private BlockTokenSecretManager blockTokenSecretManager;
	private boolean isBlockTokenEnabled;
	private Configuration conf;
	private String blockPoolID;

	public NameNodeConnector(FSNamesystem fsNamesystem, BlockManager blockManager, Configuration conf) {
		this.fsNamesystem = fsNamesystem;
		this.blockManager = blockManager;
		this.conf = conf;
		this.blockPoolID = fsNamesystem.getNamespaceInfo(0).getBlockPoolID();
		final ExportedBlockKeys keys = this.blockManager.getBlockKeys();
		this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
		if (this.isBlockTokenEnabled) {
			long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
			long blockTokenLifetime = keys.getTokenLifetime();
			LOG.info("Block token params received from NN: keyUpdateInterval=" + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime=" + blockTokenLifetime / (60 * 1000) + " min(s)");
			String encryptionAlgorithm = conf.get(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
			this.blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval, blockTokenLifetime, this.blockPoolID, encryptionAlgorithm);
		}
	}

	public List<DatanodeDescriptor> getDatanodes() {
		return blockManager.getDatanodeManager().getDatanodeListForReport(DatanodeReportType.LIVE);
	}

	public BlockWithLocations[] getBlocks(DatanodeInfo datanode) throws IOException {
		return blockManager.getBlocks(datanode, 1024).getBlocks();
	}

	public BlockWithLocations[] getBlocks(DatanodeInfo datanode, long size) throws IOException {
		return blockManager.getBlocks(datanode, size).getBlocks();
	}

	public List<DatanodeStorageInfo> getLocationInfos(Block block) {
		return blockManager.getLocationInfos(block);
	}

	public List<DatanodeStorageInfo> getStorageInfos(Block block) {
		Iterable<DatanodeStorageInfo> iterator = blockManager.getStorages(block);
		List<DatanodeStorageInfo> list = new ArrayList<DatanodeStorageInfo>();
		for (DatanodeStorageInfo ds : iterator) {
			list.add(ds);
		}
		return list;
	}

	public Token<BlockTokenIdentifier> getAccessToken(ExtendedBlock eb) throws IOException {
		if (!isBlockTokenEnabled) {
			return BlockTokenSecretManager.DUMMY_TOKEN;
		} else {
			return blockTokenSecretManager.generateToken(null, eb, EnumSet.of(BlockTokenSecretManager.AccessMode.REPLACE, BlockTokenSecretManager.AccessMode.COPY));
		}
	}

	public String getBlockPoolId() {
		return this.blockPoolID;
	}
}

class BlockMove implements Runnable {
	static final Log LOG = LogFactory.getLog(BlockMove.class);
	public static final int BLOCK_MOVE_READ_TIMEOUT = 20 * 60 * 1000; // 20
																		// minutes
	Block block;
	DatanodeDescriptor source;
	DatanodeDescriptor target;
	DatanodeStorageInfo storage;
	NameNodeConnector nameNodeConnector;

	public BlockMove(Block block, DatanodeDescriptor source, DatanodeDescriptor target, DatanodeStorageInfo storage, NameNodeConnector nameNodeConnector) {
		this.block = block;
		this.source = source;
		this.target = target;
		this.storage = storage;
		this.nameNodeConnector = nameNodeConnector;
	}

	/* Send a block replace request to the output stream */
	private void sendRequest(DataOutputStream out) throws IOException {
		final ExtendedBlock eb = new ExtendedBlock(nameNodeConnector.getBlockPoolId(), this.block);
		final Token<BlockTokenIdentifier> accessToken = nameNodeConnector.getAccessToken(eb);
		LOG.fatal("[move] move block to " + storage.getStorageID());
		new Sender(out).replaceBlock(eb, accessToken, source.getDatanodeUuid(), this.source, storage.getStorageID(), StorageType.ANY);
	}

	/* Receive a block copy response from the input stream */
	private void receiveResponse(DataInputStream in) throws IOException {
		BlockOpResponseProto response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
		if (response.getStatus() != Status.SUCCESS) {
			if (response.getStatus() == Status.ERROR_ACCESS_TOKEN)
				throw new IOException("block move failed due to access token error");
			throw new IOException("block move is failed: " + response.getMessage());
		}
	}

	@Override
	public void run() {
		LOG.fatal("[move] run -> block=" + this.block + ", src=" + this.source + ", dest=" + this.target);
		Socket sock = new Socket();
		DataOutputStream out = null;
		DataInputStream in = null;
		try {
			sock.connect(NetUtils.createSocketAddr(target.getXferAddr()), HdfsServerConstants.READ_TIMEOUT);
			/*
			 * Unfortunately we don't have a good way to know if the Datanode is
			 * taking a really long time to move a block, OR something has gone
			 * wrong and it's never going to finish. To deal with this scenario,
			 * we set a long timeout (20 minutes) to avoid hanging the balancer
			 * indefinitely.
			 */
			sock.setSoTimeout(BLOCK_MOVE_READ_TIMEOUT);

			sock.setKeepAlive(true);

			OutputStream unbufOut = sock.getOutputStream();
			InputStream unbufIn = sock.getInputStream();
			out = new DataOutputStream(new BufferedOutputStream(unbufOut, HdfsConstants.IO_FILE_BUFFER_SIZE));
			in = new DataInputStream(new BufferedInputStream(unbufIn, HdfsConstants.IO_FILE_BUFFER_SIZE));

			LOG.fatal("[move] before send");
			sendRequest(out);
			LOG.fatal("[move] after send");
			receiveResponse(in);
			LOG.info("Successfully moved " + this);
		} catch (IOException e) {
			LOG.warn("Failed to move " + this + ": " + e.getMessage());
			/*
			 * proxy or target may have an issue, insert a small delay before
			 * using these nodes further. This avoids a potential storm of
			 * "threads quota exceeded" Warnings when the balancer gets out of
			 * sync with work going on in datanode.
			 */
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
			IOUtils.closeSocket(sock);
		}
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

class MigrationService {

	static final Log LOG = LogFactory.getLog(TieredStorageManager.class);

	private NameNodeConnector nnc;
	private Set<Block> migratingBlocks = new HashSet<Block>();

	private BlockingQueue<TimedBlockMove> taskQueue = new LinkedBlockingQueue<TimedBlockMove>();
	private MigrationStatistics statistics = new MigrationStatistics();

	public MigrationService(NameNodeConnector nnc) {
		this.nnc = nnc;
		for (int i = 0; i < 5; i++) {
			MigrationWorker worker = new MigrationWorker(this, statistics);
			worker.run();
		}
	}

	public boolean isMigrating(Block block) {
		return migratingBlocks.contains(block);
	}

	public synchronized void addTask(Block block, DatanodeDescriptor sourceNode, DatanodeDescriptor destNode, DatanodeStorageInfo storage) {
		LOG.fatal("[Migrator] add migration task: block=" + block + ", src=" + sourceNode.getHostName() + ", dest=" + destNode.getHostName() + ", storage=" + storage);
		TimedBlockMove blockMove = new TimedBlockMove(block, sourceNode, destNode, storage, this.nnc);
		taskQueue.add(blockMove);
		migratingBlocks.add(block);
	}

	public TimedBlockMove takeTask() throws InterruptedException {
		TimedBlockMove task = taskQueue.take();
		return task;
	}

	public void completTask(TimedBlockMove task) {
		migratingBlocks.remove(task.block);
	}

	class TimedBlockMove extends BlockMove {
		private long elapsedTime;

		public TimedBlockMove(Block block, DatanodeDescriptor source, DatanodeDescriptor target, DatanodeStorageInfo storage, NameNodeConnector nameNodeConnector) {
			super(block, source, target, storage, nameNodeConnector);
		}

		@Override
		public void run() {
			long startTime = System.currentTimeMillis();
			super.run();
			elapsedTime = System.currentTimeMillis() - startTime;
		}

		public long getElapsedTime() {
			return elapsedTime;
		}

	}

	class MigrationStatistics {
		private DescriptiveStatistics statiscits = new DescriptiveStatistics();

		public boolean shouldDelay(TimedBlockMove blockMove) {
			double elapseTime = blockMove.getElapsedTime() / 1000000d;
			boolean delay = true;
			if (statiscits.getN() == 0) {
				delay = true;
			} else {
				if (blockMove.elapsedTime > statiscits.getMean() + 2 * statiscits.getStandardDeviation()) {
					delay = false;
				} else {
					delay = true;
				}
			}
			statiscits.addValue(elapseTime);
			return delay;
		}
	}

	class MigrationWorker extends Thread {
		final Log LOG = LogFactory.getLog(MigrationWorker.class);

		private MigrationService service;
		private MigrationStatistics statistics;

		public MigrationWorker(MigrationService service, MigrationStatistics statistics) {
			this.service = service;
			this.statistics = statistics;
		}

		@Override
		public void run() {
			while (true) {
				try {
					TimedBlockMove blockMove = service.takeTask();
					blockMove.run();
					service.completTask(blockMove);
					if (statistics.shouldDelay(blockMove)) {
						Thread.sleep(1000);
					}
				} catch (Exception e) {
					LOG.fatal("Unable to complet migration task", e);
				}
			}
		}

	}

}
