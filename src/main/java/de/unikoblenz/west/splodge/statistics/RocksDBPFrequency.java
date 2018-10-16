package de.unikoblenz.west.splodge.statistics;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * A RocksDB implementation of {@link PFrequency}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBPFrequency implements PFrequency {

  private final RocksDB pFrequency;

  private Map<Long, Long> entriesInBatch;

  private final int maxBatchEntries;

  public RocksDBPFrequency(String databaseFile) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(100);
    options.setAllowOsBuffer(true);
    options.setWriteBufferSize(64 * 1024 * 1024);
    File indexDir = new File(databaseFile);
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }
    try {
      pFrequency = RocksDB.open(options, indexDir + File.separator + "snpgIndex");
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
    maxBatchEntries = 100000;
  }

  @Override
  public void put(long key, long value) {
    if (entriesInBatch == null) {
      entriesInBatch = new HashMap<>();
    }
    boolean isNew = entriesInBatch.put(key, value) == null;
    if (isNew && (entriesInBatch.size() == maxBatchEntries)) {
      internalFlush();
    }
  }

  @Override
  public long get(long key) {
    if (entriesInBatch == null) {
      entriesInBatch = new HashMap<>();
    }
    Long value = entriesInBatch.get(key);
    if (value != null) {
      return value.longValue();
    }
    try {
      byte[] result = pFrequency.get(NumberConversion.long2bytes(key));
      if (result == null) {
        throw new NoSuchElementException(key + " could not be found.");
      }
      return NumberConversion.bytes2long(result);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    internalFlush();
    try {
      pFrequency.compactRange();
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private void internalFlush() {
    if (entriesInBatch != null) {
      WriteOptions writeOpts = new WriteOptions();
      try {
        WriteBatch batch = new WriteBatch();
        for (Entry<Long, Long> elem : entriesInBatch.entrySet()) {
          batch.put(NumberConversion.long2bytes(elem.getKey()),
                  NumberConversion.long2bytes(elem.getValue()));
        }
        pFrequency.write(writeOpts, batch);
        entriesInBatch.clear();
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      } finally {
        writeOpts.dispose();
      }
    }
  }

  @Override
  public void close() {
    internalFlush();
    if (pFrequency != null) {
      pFrequency.close();
    }
  }

}
