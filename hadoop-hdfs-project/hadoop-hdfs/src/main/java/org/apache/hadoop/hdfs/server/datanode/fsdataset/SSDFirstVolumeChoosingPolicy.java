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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * Choose volumes in round-robin order.
 */
public class SSDFirstVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V> {

  static final Log LOG = LogFactory.getLog(SSDFirstVolumeChoosingPolicy.class);
  private int maxAllocations = 10;
  private static Map<String, Integer> accessRecord = new HashMap<String, Integer>();
  private static Map<String, Boolean> skipRecord = new HashMap<String, Boolean>();

  @Override
  public synchronized V chooseVolume(final List<V> volumes, final long blockSize) throws IOException {
    return chooseVolume(volumes, blockSize, StorageType.ANY);
  }
  
  @Override
  public synchronized V chooseVolume(final List<V> volumes, final long blockSize, final StorageType storagePreference) throws IOException {
	  if(volumes.size() < 1) {
	      throw new DiskOutOfSpaceException("No more available volumes");
	    }
	    
	    List<VolumeRecord> orderedVolumedList = getSortedVolumeList(volumes, blockSize, storagePreference);
	    for (int i = 0; i < orderedVolumedList.size(); i++) {
	    	LOG.fatal("[volume] record[" + i + "]: " + orderedVolumedList.get(i));
	    }
	    
	    if (orderedVolumedList.size() > 0) {
	    	VolumeRecord selectedVolumn = orderedVolumedList.get(0);
	    	LOG.fatal("[volume] choose volume=" + selectedVolumn.volume + ", accessCount=" + selectedVolumn.accessCount);
	    	incrementAccessCount(selectedVolumn.volume);
	    	return orderedVolumedList.get(0).volume;
	    } 

	    List<V> randomVolumeList = new ArrayList<V>(volumes);
	    Collections.shuffle(randomVolumeList);
	    
	    for (V v : randomVolumeList) {
	    	try {
	    		if (v.getAvailable() > 0) {
	    			incrementAccessCount(v);
	    			return v;
	    		}
	    	} catch (Exception e) {
	    		
	    	}
	    }
	    
	    throw new DiskOutOfSpaceException("Out of space: The volume with the most available space is less than the block size (=" + blockSize + " B).");
  }
  
  
  public void incrementAccessCount(V v) {
	  accessRecord.put(v.getStorageID(), accessRecord.get(v.getStorageID()) + 1);
  }
  
  public int lookupAccessCount(V v) {
	  if (accessRecord.containsKey(v.getStorageID())) {
		  return accessRecord.get(v.getStorageID());
	  } else {
		  int count = 0;
		  accessRecord.put(v.getStorageID(), count);
		  return count;
	  }
  }
  
  public boolean lookupSkipRecord(V v) {
	  if (skipRecord.containsKey(v.getStorageID())) {
		  return skipRecord.get(v.getStorageID());
	  } else {
		  return false;
	  }
  }
  
  public void setSkipRecord(V v) {
	  LOG.fatal("[volume] set skip record: " + v);
	  skipRecord.put(v.getStorageID(), true);
  }
  
  public void removeSkipRecord(V v) {
	  LOG.fatal("[volume] remove skip record: " + v);
	  skipRecord.remove(v.getStorageID());
  }
  
  public List<VolumeRecord> getSortedVolumeList(List<V> volumes, long blockSize, StorageType storageType) {
	  List<VolumeRecord> allowedList = new ArrayList<VolumeRecord>();
	  for (V v : volumes) {
		  
		  long freeSpace = -1;
		  try {
			  freeSpace = v.getAvailable();
		  } catch (Exception e) {
		  } 
		  
		  int accessCount = lookupAccessCount(v);
		  boolean skipLastTime = lookupSkipRecord(v);
		  if (freeSpace > blockSize) {
			  if (storageType == null || storageType.equals(StorageType.ANY) || (v.getStorageType().equals(storageType))) {
				  if (skipLastTime || accessCount == 0 || accessCount % this.maxAllocations != 0) {
					  removeSkipRecord(v);
					  allowedList.add(new VolumeRecord(v, accessCount, freeSpace)); 
			      } else {
			    	  setSkipRecord(v);
			      }
			  }
		  }
	  }
	  
	  Collections.sort(allowedList);
	  return allowedList;
  }
  class VolumeRecord implements Comparable<VolumeRecord> {
	V volume;  
	int accessCount;
	long freeSpace;

	public VolumeRecord(V volume, int accessCount, long freeSpace) {
		this.volume = volume;
		this.accessCount = accessCount;
		this.freeSpace = freeSpace;
	}
	

	@Override
	public String toString() {
		return "VolumeRecord [volume=" + volume + ", accessCount="
				+ accessCount + ", freeSpace=" + freeSpace + "]";
	}


	@Override
	public int compareTo(VolumeRecord o) {
		if (this.volume.getStorageType().equals(o.volume.getStorageType())) {
			//return new Integer(this.accessCount).compareTo(new Integer(o.accessCount));
			return new Long(this.freeSpace).compareTo(new Long(o.freeSpace));
		} else {
			return this.volume.getStorageType().equals(StorageType.SSD) ? -1 : 1;
		}
	}
	  
  }

}
