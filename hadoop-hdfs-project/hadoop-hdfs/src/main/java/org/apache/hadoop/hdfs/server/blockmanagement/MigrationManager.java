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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

public class MigrationManager {

	static final Log LOG = LogFactory.getLog(TieredStorageManager.class);

	private Context context;

	private MigrationStatistics statistics = new MigrationStatistics();

	private BlockingQueue<MigrationTask> criticalTaskQueue = new LinkedBlockingQueue<MigrationTask>();
	private BlockingQueue<MigrationTask> normalTaskQueue = new LinkedBlockingQueue<MigrationTask>();
	private HashMap<DatanodeDescriptor, Set<Block>> migratingRecords = new HashMap<DatanodeDescriptor, Set<Block>>();

	public MigrationManager(Context context) {
		this.context = context;
		for (int i = 0; i < 5; i++) {
			MigrationWorker worker = new MigrationWorker(context, this, statistics);
			worker.setName("MigrationWorker-" + i);
			worker.start();
		}
	}

	public boolean isMigrating(DatanodeDescriptor node, Block block) {
		return getMigratingRecord(node).contains(block);
	}

	public void addTask(MigrationTask task) {
		BlockingQueue<MigrationTask> taskQueue = getTaskQueue(task.getType());
		taskQueue.add(task);
		getMigratingRecord(task.getSourceNode()).add(task.getBlock());
	}

	public void addTasks(Collection<MigrationTask> tasks) {
		for (MigrationTask task : tasks) {
			LOG.fatal("[Migration] add one task: " + task);
			addTask(task);
		}
	}

	public BlockingQueue<MigrationTask> getTaskQueue(MigrationTask.Type type) {
		if (type.equals(MigrationTask.Type.CRITIAL)) {
			return criticalTaskQueue;
		} else {
			return normalTaskQueue;
		}
	}

	public synchronized Set<Block> getMigratingRecord(DatanodeDescriptor node) {
		if (!migratingRecords.containsKey(node)) {
			Set<Block> set = Collections.synchronizedSet(new HashSet<Block>());
			migratingRecords.put(node, set);
		}
		return migratingRecords.get(node);
	}

	/**
	 * Blocking method to take one migration task from queue.
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public MigrationTask takeTask() throws InterruptedException {
		return takeTask(MigrationTask.Type.NORMAL);
	}

	public MigrationTask takeTask(MigrationTask.Type type) throws InterruptedException {
		return getTaskQueue(type).take();
	}

	public void completeTask(MigrationTask task) {
		getMigratingRecord(task.getSourceNode()).remove(task.getBlock());
	}

	public void failedTask(MigrationTask task) {
		getMigratingRecord(task.getSourceNode()).remove(task.getBlock());
	}
	
	public int getTaskSize() {
		return getTaskQueue(MigrationTask.Type.NORMAL).size() + getTaskQueue(MigrationTask.Type.CRITIAL).size();
	}
	

	public long getTotalMigrationSize(DatanodeDescriptor sourceNode) {
		long total = 0L;
		Set<Block> blocks = getMigratingRecord(sourceNode);
		for (Block block : blocks) {
			total += block.getNumBytes();
		}
		return total;
	}

}

class MigrationStatistics {
	private DescriptiveStatistics statiscits = new DescriptiveStatistics();

	public boolean shouldDelay(long elapsedTime) {
		double elapsedTimeInMS = elapsedTime / 1000000d;
		boolean delay = true;
		if (statiscits.getN() == 0) {
			delay = false;
		} else {
			if (elapsedTimeInMS > statiscits.getMean() + statiscits.getStandardDeviation()) {
				delay = true;
			} else {
				delay = false;
			}
		}
		statiscits.addValue(elapsedTimeInMS);
		return delay;
	}
	
	@Override
	public String toString() {
		return "Mean=" + statiscits.getMean() + ", Std=" + statiscits.getStandardDeviation();
	}
}

class MigrationWorker extends Thread {
	final Log LOG = LogFactory.getLog(MigrationWorker.class);

	private Context context;
	private MigrationManager manager;
	private MigrationStatistics statistics;
	
	public MigrationWorker(Context context, MigrationManager manager, MigrationStatistics statistics) {
		this.context = context;
		this.manager = manager;
		this.statistics = statistics;
	}

	@Override
	public void run() {
		while (true) {
			MigrationTask task = null;
			try {
				task = manager.takeTask();
				LOG.fatal(this.getName() + " -> start one task: " + task + ", remaing=" + manager.getTaskSize());
				BlockMove blockMove = new BlockMove(task, context);
				long startTime = System.currentTimeMillis();
				blockMove.run();
				long elpaseTime = System.currentTimeMillis() - startTime;
				manager.completeTask(task);
				LOG.fatal(this.getName() + " -> complete one task: " + task + ", elapsed=" + elpaseTime);
				LOG.fatal(this.getName() + " -> statistic -> " + statistics);
				if (statistics.shouldDelay(elpaseTime)) {
					LOG.fatal(this.getName() + " -> should delay: " + statistics);
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				LOG.fatal(this.getName() + " -> Unable to complet migration task", e);
				this.manager.failedTask(task);
			}
		}
	}

}

class BlockMove {
	static final Log LOG = LogFactory.getLog(BlockMove.class);
	public static final int BLOCK_MOVE_READ_TIMEOUT = 20 * 60 * 1000; // 20
																		// minutes
	Block block;
	DatanodeDescriptor source;
	DatanodeDescriptor target;
	DatanodeStorageInfo storage;
	Context context;

	public BlockMove(MigrationTask task, Context context) {
		this(task.getBlock(), task.getSourceNode(), task.getTargetNode(), task.getTargetStorage(), context);
	}

	public BlockMove(Block block, DatanodeDescriptor source, DatanodeDescriptor target, DatanodeStorageInfo storage, Context context) {
		this.block = block;
		this.source = source;
		this.target = target;
		this.storage = storage;
		this.context = context;
	}

	/* Send a block replace request to the output stream */
	private void sendRequest(DataOutputStream out) throws IOException {
		final ExtendedBlock eb = new ExtendedBlock(context.namenode.getBlockPoolId(), this.block);
		final Token<BlockTokenIdentifier> accessToken = context.namenode.getAccessToken(eb);
		LOG.fatal("[move] move block to node=" + this.target.getHostName() + ", storage="+ storage.getStorageID());
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

	public void run() throws IOException {
		Socket sock = new Socket();
		DataOutputStream out = null;
		DataInputStream in = null;
		try {
			LOG.fatal("[Move] running: " + this);
			sock.connect(NetUtils.createSocketAddr(target.getXferAddr()), HdfsServerConstants.READ_TIMEOUT);
			sock.setSoTimeout(BLOCK_MOVE_READ_TIMEOUT);
			sock.setKeepAlive(true);

			OutputStream unbufOut = sock.getOutputStream();
			InputStream unbufIn = sock.getInputStream();
			out = new DataOutputStream(new BufferedOutputStream(unbufOut, HdfsConstants.IO_FILE_BUFFER_SIZE));
			in = new DataInputStream(new BufferedInputStream(unbufIn, HdfsConstants.IO_FILE_BUFFER_SIZE));

			sendRequest(out);
			receiveResponse(in);
			LOG.fatal("[Move] completed: " + this);
		} catch (Exception e) {
			LOG.fatal("[Move] failed: " + this);
			throw new IOException("Unable to complete move task: " + this, e);
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
			IOUtils.closeSocket(sock);
		}
	}

	@Override
	public String toString() {
		return "BlockMove [block=" + block + ", source=" + source + ", target=" + target + ", storage=" + storage + "]";
	}
}

class MigrationTask {
	public enum Type {
		CRITIAL, NORMAL,
	}

	private Block block;
	private DatanodeDescriptor sourceNode;
	private DatanodeDescriptor targetNode;
	private DatanodeStorageInfo targetStorage;
	private Type type;

	public MigrationTask(Block block, DatanodeDescriptor sourceNode, DatanodeDescriptor targetNode, DatanodeStorageInfo targetStorage, Type type) {
		this.block = block;
		this.sourceNode = sourceNode;
		this.targetNode = targetNode;
		this.targetStorage = targetStorage;
		this.type = type;
	}

	public Block getBlock() {
		return block;
	}

	public DatanodeDescriptor getSourceNode() {
		return sourceNode;
	}

	public DatanodeDescriptor getTargetNode() {
		return targetNode;
	}

	public DatanodeStorageInfo getTargetStorage() {
		return targetStorage;
	}

	public Type getType() {
		return type;
	}

	@Override
	public String toString() {
		return "MigrationTask [block=" + block + ", sourceNode=" + sourceNode + ", targetNode=" + targetNode + ", targetStorage=" + targetStorage + ", type=" + type + "]";
	}
	
}