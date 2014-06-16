package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.Workload;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.MigrationTask.Type;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.security.token.Token;

public class TieredStorageManager {

	static final Log LOG = LogFactory.getLog(TieredStorageManager.class);

	private ScheduledExecutorService serviceExecutor = Executors.newScheduledThreadPool(1);
	private Configuration conf;

	private Context context;

	private WorkloadAnalyzer workloadAnalyzer;
	private MigrationManager migrationManager;

	public TieredStorageManager(Configuration conf, BlockManager blockManager, FSNamesystem fsNamesystem) {
		this.conf = conf;
		this.context = new Context(fsNamesystem, blockManager, conf);
		this.migrationManager = new MigrationManager(this.context);
		this.workloadAnalyzer = new WorkloadAnalyzer(this.context);
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

	public void stop() {
		serviceExecutor.shutdown();
	}

	/**
	 * receive workload report from nodes
	 *
	 * @param workloads
	 */
	public void workloadReport(DatanodeRegistration nodeReg, List<Workload> workloads) {
		DatanodeDescriptor node = context.namenode.lookupDataNode(nodeReg.getDatanodeUuid());
		if (node == null) {
			LOG.fatal("[Tier] Unknown node record=" + nodeReg);
			return;
		}

		LOG.fatal("[Tier] workload report node=" + node.getHostName() + ", size=" + workloads.size());
		this.workloadAnalyzer.addRecord(node, workloads);
	}

	/**
	 * Two steps: 1) calculate whether to migration 2) hand to migration manager
	 */
	public void run() {
		LOG.fatal("[Tier] start runing migration thread");
		MigrationAlgorithm algorithm = new BalanceMigrationAlgorithm(this.context, this.workloadAnalyzer, this.migrationManager);
		LOG.fatal("[Tier] algorithm is evaluating");
		long startTime = System.currentTimeMillis();
		algorithm.evaluate();
		long period = System.currentTimeMillis() - startTime;
		LOG.fatal("[Tier] algorithm execution time=" + period);
		if (!algorithm.needsMigrations()) {
			LOG.fatal("[Tier] no need to migrate");
			return;
		}

		LOG.fatal("[Tier] adding migration tasks");
		Collection<MigrationTask> migrationTasks = algorithm.getTasks();
		this.migrationManager.addTasks(migrationTasks);
	}

}

class Context {
	private Log LOG = LogFactory.getLog(Context.class);

	private FSNamesystem fsNamesystem;
	private BlockManager blockManager;
	private Configuration conf;

	// exposed interface
	public NameNodeConext namenode;
	public BlockConext block;

	public Context(FSNamesystem fsNamesystem, BlockManager blockManager, Configuration conf) {
		this.fsNamesystem = fsNamesystem;
		this.blockManager = blockManager;
		this.conf = conf;
		this.namenode = new NameNodeConext();
		this.block = new BlockConext();
		namenode.init();
		block.init();
	}

	public class NameNodeConext {
		private BlockTokenSecretManager blockTokenSecretManager;
		private boolean isBlockTokenEnabled;

		public void init() {
			final ExportedBlockKeys keys = blockManager.getBlockKeys();
			this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
			if (this.isBlockTokenEnabled) {
				long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
				long blockTokenLifetime = keys.getTokenLifetime();
				LOG.info("Block token params received from NN: keyUpdateInterval=" + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime=" + blockTokenLifetime / (60 * 1000) + " min(s)");
				String encryptionAlgorithm = conf.get(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
				this.blockTokenSecretManager = new BlockTokenSecretManager(blockKeyUpdateInterval, blockTokenLifetime, this.getBlockPoolId(), encryptionAlgorithm);
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

		public String getBlockPoolId() {
			return fsNamesystem.getNamespaceInfo(0).getBlockPoolID();
		}

		public Token<BlockTokenIdentifier> getAccessToken(ExtendedBlock eb) throws IOException {
			if (!isBlockTokenEnabled) {
				return BlockTokenSecretManager.DUMMY_TOKEN;
			} else {
				return blockTokenSecretManager.generateToken(null, eb, EnumSet.of(BlockTokenSecretManager.AccessMode.REPLACE, BlockTokenSecretManager.AccessMode.COPY));
			}
		}

		public DatanodeDescriptor lookupDataNode(String uuid) {
			for (DatanodeDescriptor node : getDatanodes()) {
				if (node.getDatanodeUuid().equals(uuid)) {
					return node;
				}
			}
			return null;
		}

		public DatanodeStorageInfo pickStorage(DatanodeDescriptor node, StorageType type) {
			List<DatanodeStorageInfo> dsInfos = new LinkedList<DatanodeStorageInfo>();
			for (DatanodeStorageInfo dsInfo : node.getStorageInfos()) {
				if (dsInfo.getStorageType().equals(type)) {
					dsInfos.add(dsInfo);
				}
			}
			Collections.shuffle(dsInfos);
			if (dsInfos.size() > 0) {
				return dsInfos.get(0);
			}
			return null;
		}
	}

	public class BlockConext {
		public void init() {
		}

		public BlockInfo convert(Block block) {
			return blockManager.getStoredBlock(block);
		}

		public List<DatanodeStorageInfo> getStorageInfos(Block block) {
			Iterable<DatanodeStorageInfo> iterator = blockManager.getStorages(block);
			List<DatanodeStorageInfo> list = new ArrayList<DatanodeStorageInfo>();
			for (DatanodeStorageInfo ds : iterator) {
				list.add(ds);
			}
			return list;
		}
	}
}

abstract class MigrationAlgorithm {
	protected Context context;
	protected WorkloadAnalyzer workloadAnalyzer;
	protected Collection<MigrationTask> tasks = new ArrayList<MigrationTask>();
	protected MigrationManager migrationManager;

	public MigrationAlgorithm(Context context, WorkloadAnalyzer workloadAnalyzer, MigrationManager migrationManager) {
		this.context = context;
		this.workloadAnalyzer = workloadAnalyzer;
		this.migrationManager = migrationManager;
	}

	public abstract void evaluate();

	public boolean needsMigrations() {
		return this.tasks.size() > 0;
	}

	public Collection<MigrationTask> getTasks() {
		return this.tasks;
	}
}

class BalanceMigrationAlgorithm extends MigrationAlgorithm {

	static final Log LOG = LogFactory.getLog(BalanceMigrationAlgorithm.class);

	private double boundary = 0.005;

	public BalanceMigrationAlgorithm(Context context, WorkloadAnalyzer workloadAnalyzer, MigrationManager migrationManager) {
		super(context, workloadAnalyzer, migrationManager);
	}

	private long calculateTotalSpace(DatanodeStorageInfo[] dsInfos) {
		long capacity = 0L;
		for (DatanodeStorageInfo dsInfo : dsInfos) {
			capacity += dsInfo.getCapacity();
		}
		return capacity;
	}

	private long calculateUsedSpace(DatanodeStorageInfo[] dsInfos) {
		long used = 0L;
		for (DatanodeStorageInfo dsInfo : dsInfos) {
			used += dsInfo.getDfsUsed();
		}
		return used;
	}

	private double calculateUsedPercentage(DatanodeStorageInfo[] dsInfos) {
		return (double)calculateUsedSpace(dsInfos) / calculateTotalSpace(dsInfos);
	}

	@Override
	public void evaluate() {

		this.tasks.clear();
		List<DatanodeDescriptor> nodes = context.namenode.getDatanodes();

		// order nodes by space used, small to large

		List<DatanodeDescriptor> overUtilizedNodes = new LinkedList<DatanodeDescriptor>();
		for (DatanodeDescriptor node : nodes) {
			double usedPercent = calculateUsedPercentage(node.getStorageInfos(StorageType.SSD));
			LOG.fatal("[Algorithn] usedPercent=" + usedPercent
					               + ", used=" + calculateUsedSpace(node.getStorageInfos(StorageType.SSD))
					               + ", total=" + calculateTotalSpace(node.getStorageInfos(StorageType.SSD))
					               + ", boundary=" + this.boundary
					               + ", overall=" + node.getDfsUsed());
			if (usedPercent > this.boundary) {
				LOG.fatal("[Algorithm] over utilized datanode: " + node + ", capacity=" + node.getCapacity() + ", used" + node.getDfsUsed() + ", percentage=" + node.getDfsUsedPercent());
				overUtilizedNodes.add(node);
			}

		}

		if (overUtilizedNodes.size() == 0) {
			LOG.fatal("[Algorithm] no over utilized nodes");
			return;
		}

		for (DatanodeDescriptor overUtilizedNode : overUtilizedNodes) {
			LOG.fatal("[Algorithm] processing over utilized node: " + overUtilizedNode);
			Iterator<BlockInfo> blocks = overUtilizedNode.getBlockIterator(StorageType.SSD);
			TreeSet<BlockAccessRecord> orderedBlocks = new TreeSet<BlockAccessRecord>(new BlockAccessCountComparator());
			while (blocks.hasNext()) {
				BlockInfo blockInfo = blocks.next();
				int accessCount = this.workloadAnalyzer.getAccessCount(overUtilizedNode, blockInfo);
				BlockAccessRecord record = new BlockAccessRecord(blockInfo, accessCount);
				orderedBlocks.add(record);
			}

			LOG.fatal("[Algorithm] sorted block access record ");

			Iterator<BlockAccessRecord> iterator = orderedBlocks.iterator();
			long usedSpace = calculateUsedSpace(overUtilizedNode.getStorageInfos(StorageType.SSD));
			long targetSpace = (long) (calculateTotalSpace(overUtilizedNode.getStorageInfos(StorageType.SSD)) * this.boundary);
			long currentMigrationSize = this.migrationManager.getTotalMigrationSize(overUtilizedNode);
			long featureMigrationSize = 0L;
			LOG.fatal("[Algorithm] usedSpace=" + usedSpace);
			LOG.fatal("[Algorithm] targetSpace=" + usedSpace);
			LOG.fatal("[Algorithm] currentMigrationSize=" + usedSpace);
			while (iterator.hasNext() && (usedSpace - currentMigrationSize - featureMigrationSize) > targetSpace) {
				BlockAccessRecord record = iterator.next();
				DatanodeStorageInfo storage = context.namenode.pickStorage(overUtilizedNode, StorageType.DISK);
				if (storage != null) {
					MigrationTask task = new MigrationTask(record.block, overUtilizedNode, overUtilizedNode, storage, Type.NORMAL);
					tasks.add(task);
					featureMigrationSize += record.block.getNumBytes();
					LOG.fatal("[Algorithm] featureMigrationSize=" + usedSpace);
				} else {
					LOG.fatal("[Algorithm] no DISK storage available");
				}
			}
		}

	}

	class BlockAccessRecord {
		public Block block;
		public int accessCount;

		public BlockAccessRecord(Block block, int accessCount) {
			this.block = block;
			this.accessCount = accessCount;
		}
	}

	class NodeUtilizationComparator implements Comparator<DatanodeDescriptor> {
		@Override
		public int compare(DatanodeDescriptor o1, DatanodeDescriptor o2) {
			return Long.valueOf(o1.getDfsUsed()).compareTo(Long.valueOf(o2.getDfsUsed()));
		}
	}

	class BlockAccessCountComparator implements Comparator<BlockAccessRecord> {

		@Override
		public int compare(BlockAccessRecord o1, BlockAccessRecord o2) {
			int c1 = Integer.valueOf(o1.accessCount).compareTo(o2.accessCount);
			if (c1 == 0) {
				return Long.valueOf(o1.block.getGenerationStamp()).compareTo(o2.block.getGenerationStamp());
			}
			return c1;
		}
	}

}
