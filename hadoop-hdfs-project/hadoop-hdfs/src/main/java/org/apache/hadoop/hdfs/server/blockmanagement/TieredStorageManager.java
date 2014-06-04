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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

public class TieredStorageManager {
	
	final private static long MAX_BLOCKS_SIZE_TO_FETCH = 2*1024*1024*1024L; //2GB
	static final Log LOG = LogFactory.getLog(TieredStorageManager.class);
	
	private ScheduledExecutorService serviceExecutor = Executors.newScheduledThreadPool(1);
	private ExecutorService taskService = Executors.newFixedThreadPool(10);
	private Configuration conf;
	
	private BlockManager blockManager;
	private FSNamesystem fsNamesystem;
	private NameNodeConnector nnc;
	
	Random random = new Random();
	
	private Set<Block> movedSet = new HashSet<Block>();
	public TieredStorageManager(Configuration conf, BlockManager blockManager, FSNamesystem fsNamesystem) {
		this.conf = conf;
		this.blockManager = blockManager;
		this.fsNamesystem = fsNamesystem;
		this.nnc = new NameNodeConnector(this.fsNamesystem, this.blockManager, conf);
	}

	public void start() {
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
	public void run() {
		try {
			List<DatanodeDescriptor> datanodes = nnc.getDatanodes();
			if (datanodes.size() < 1) {
				return;
			}
			int ri = randInt(datanodes.size());
			LOG.fatal("[move] random=" + ri);
			for (DatanodeDescriptor dd : datanodes) {
				LOG.fatal("[move] dd=" + dd);
			}
			DatanodeDescriptor datanode = datanodes.get(ri);
			
			BlockWithLocations[] newBlocks = nnc.getBlocks(datanode);
			if (newBlocks.length < 1) {
				return;
			}
			BlockWithLocations block = newBlocks[randInt(newBlocks.length)];
			LOG.fatal("[move] pick a datanode -> " + datanode);
			LOG.fatal("[move] pick a block ->" + block);
			
			if (movedSet.contains(block.getBlock())) {
				LOG.fatal("[move] block " + block + " is already moved");
				return;
			}
			
			List<DatanodeStorageInfo> dsInfos = nnc.getStorageInfos(block.getBlock());
			for (DatanodeStorageInfo dsInfo : dsInfos) {
				DatanodeDescriptor sourceNode = dsInfo.getDatanodeDescriptor();
				LOG.fatal("[move] move from block=" + block + ", node=" + sourceNode.getHostName() + ", location=" + dsInfo);
				StorageType destStorageType = dsInfo.getStorageType().equals(StorageType.SSD) ? StorageType.DISK : StorageType.SSD;
				  
				List<DatanodeDescriptor> cadidateNodes = getNodesWithStorage(sourceNode, datanodes, destStorageType);
				if (cadidateNodes.size() < 2) {
					LOG.fatal("[move] not enough datanodes to migrate to");
					continue;
				}
				DatanodeDescriptor destNode = cadidateNodes.get(randInt(cadidateNodes.size()));
				if (dsInfo.getDatanodeDescriptor().equals(destNode)) {
					LOG.fatal("The block " + block + " is already stored on node " + datanode);
					continue;
				}
				String pickedStorageID = pickStorageID(destNode, destStorageType);
				LOG.fatal("[move] move to datanode: " + destNode + ", storageype=" + pickedStorageID);
				BlockMove blockMove = new BlockMove(block.getBlock(), sourceNode, destNode, pickedStorageID, this.nnc);
				blockMove.run();
				movedSet.add(block.getBlock());
				break;
			}
			
			
			
			printArray(block.getDatanodeUuids());
			printArray(block.getStorageIDs());
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
		return start + random.nextInt(end-start);
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
		for (int i =0; i < newList.size(); i++) {
			if (newList.get(i).getHostName().equals(original.getHostName())) {
				index = i;
			}
		}
		newList.remove(index);

		Iterator<DatanodeDescriptor> iterator = newList.iterator();
		while (iterator.hasNext()) {
			DatanodeDescriptor dd = iterator.next();
			boolean found = false;
			for (DatanodeStorageInfo info :  dd.getStorageInfos()) {
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
		    LOG.info("Block token params received from NN: keyUpdateInterval="
		          + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
		          + blockTokenLifetime / (60 * 1000) + " min(s)");
		    String encryptionAlgorithm = conf.get(
		        DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
		    this.blockTokenSecretManager = new BlockTokenSecretManager(
		          blockKeyUpdateInterval, blockTokenLifetime, this.blockPoolID,
		          encryptionAlgorithm);
		}
	}
	
	public List<DatanodeDescriptor> getDatanodes() {
		return blockManager.getDatanodeManager().getDatanodeListForReport(DatanodeReportType.LIVE);
	}
	
	public BlockWithLocations[] getBlocks(DatanodeInfo datanode) throws IOException {
		return blockManager.getBlocks(datanode, 1024).getBlocks();
	}
	
	public BlockWithLocations[] getBlocks(DatanodeInfo datanode, long size) throws IOException{
		return blockManager.getBlocks(datanode, size).getBlocks();
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
	
	public String getBlockPoolId () {
		return this.blockPoolID;
	}
}

class BlockMove implements Runnable{
	static final Log LOG = LogFactory.getLog(BlockMove.class);
	public static final int BLOCK_MOVE_READ_TIMEOUT=20*60*1000; // 20 minutes
	Block block;
	DatanodeInfo source;
	DatanodeInfo target;
	String storageID;
	NameNodeConnector nameNodeConnector;
	public BlockMove(Block block, DatanodeInfo source, DatanodeInfo target, String storageID, NameNodeConnector nameNodeConnector) {
		this.block = block;
		this.source = source;
		this.target = target;
		this.storageID = storageID;
		this.nameNodeConnector = nameNodeConnector;
	}
	
	/* Send a block replace request to the output stream*/
    private void sendRequest(DataOutputStream out) throws IOException {
      final ExtendedBlock eb = new ExtendedBlock(nameNodeConnector.getBlockPoolId(), this.block);
      final Token<BlockTokenIdentifier> accessToken = nameNodeConnector.getAccessToken(eb);
      LOG.fatal("[move] move block to " + storageID);
      new Sender(out).replaceBlock(eb, accessToken, source.getDatanodeUuid(), this.source, storageID, StorageType.ANY);
    }
    
    /* Receive a block copy response from the input stream */ 
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response = BlockOpResponseProto.parseFrom(
          vintPrefixed(in));
      if (response.getStatus() != Status.SUCCESS) {
        if (response.getStatus() == Status.ERROR_ACCESS_TOKEN)
          throw new IOException("block move failed due to access token error");
        throw new IOException("block move is failed: " +
            response.getMessage());
      }
    }

	@Override
	public void run() {
		LOG.fatal("[move] run -> block=" + this.block + ", src=" + this.source + ", dest=" + this.target);
		Socket sock = new Socket();
	    DataOutputStream out = null;
	    DataInputStream in = null;
	    try {
	      sock.connect(
	          NetUtils.createSocketAddr(target.getXferAddr()),
	          HdfsServerConstants.READ_TIMEOUT);
	      /* Unfortunately we don't have a good way to know if the Datanode is
	       * taking a really long time to move a block, OR something has
	       * gone wrong and it's never going to finish. To deal with this 
	       * scenario, we set a long timeout (20 minutes) to avoid hanging
	       * the balancer indefinitely.
	       */
	      sock.setSoTimeout(BLOCK_MOVE_READ_TIMEOUT);

	      sock.setKeepAlive(true);
	      
	      OutputStream unbufOut = sock.getOutputStream();
	      InputStream unbufIn = sock.getInputStream();
	      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
	          HdfsConstants.IO_FILE_BUFFER_SIZE));
	      in = new DataInputStream(new BufferedInputStream(unbufIn,
	          HdfsConstants.IO_FILE_BUFFER_SIZE));
	      
	      LOG.fatal("[move] before send");
	      sendRequest(out);
	      LOG.fatal("[move] after send");
	      receiveResponse(in);
	      LOG.info("Successfully moved " + this);
	    } catch (IOException e) {
	      LOG.warn("Failed to move " + this + ": " + e.getMessage());
	      /* proxy or target may have an issue, insert a small delay
	       * before using these nodes further. This avoids a potential storm
	       * of "threads quota exceeded" Warnings when the balancer
	       * gets out of sync with work going on in datanode.
	       */
	    } finally {
	      IOUtils.closeStream(out);
	      IOUtils.closeStream(in);
	      IOUtils.closeSocket(sock);
	    }		
	}
}