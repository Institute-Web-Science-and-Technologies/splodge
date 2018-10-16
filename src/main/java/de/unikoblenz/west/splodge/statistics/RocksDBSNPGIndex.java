package de.unikoblenz.west.splodge.statistics;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Stores the incidence list of each vertex sorted by its frequency. RocksDB is
 * used for the implementation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBSNPGIndex implements SNPGIndex {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final RocksDB snpgIndex;

  private Map<LongArrayWrapper, ArrayWrapper> entriesInBatch;

  private WriteBatch deletionBatch;

  private final int maxBatchEntries;

  private final File numberOfTriplesFile;

  private long numberOfTriples;

  public RocksDBSNPGIndex(String databaseFile) {
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
      snpgIndex = RocksDB.open(options, indexDir + File.separator + "snpgIndex");
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
    maxBatchEntries = 100000;

    numberOfTriplesFile = new File(databaseFile + File.separator + "snpgIndex.numberOfTriples");
    if (numberOfTriplesFile.exists()) {
      loadNumberOfTriples();
    }
  }

  private void loadNumberOfTriples() {
    try (DataInputStream in = new DataInputStream(new FileInputStream(numberOfTriplesFile))) {
      numberOfTriples = in.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void saveNumberOfTriples() {
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(numberOfTriplesFile))) {
      out.writeLong(numberOfTriples);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void add(long subject, long predicate, long object, long graph) {
    LongArrayWrapper batchKey = new LongArrayWrapper(subject, predicate, graph);
    ArrayWrapper wrap = null;
    if (entriesInBatch != null) {
      wrap = entriesInBatch.get(batchKey);
    }
    byte[] element = null;
    if (wrap == null) {
      // this predicate has not been seen for this subject in the current batch
      long frequency = 0;
      long[] toDelete = null;
      Iterable<long[]> npgIterable = npgIterable(subject);
      for (long[] entry : npgIterable) {
        if ((entry[2] == predicate) && (entry[3] == graph)) {
          frequency = entry[1];
          toDelete = entry;
          break;
        }
      }
      ((RocksIteratorKeyWrapper) ((ByteArray2LongArrayIterator) npgIterable).getWrappedIterator())
              .close();
      element = new byte[4 * Long.BYTES];
      NumberConversion.long2bytes(subject, element, 0);
      NumberConversion.long2bytes(predicate, element, 2 * Long.BYTES);
      NumberConversion.long2bytes(graph, element, 3 * Long.BYTES);
      if (toDelete != null) {
        NumberConversion.long2bytes(toDelete[1], element, Long.BYTES);
        if (deletionBatch == null) {
          deletionBatch = new WriteBatch();
        }
        deletionBatch.remove(element);
      }
      frequency++;
      element = Arrays.copyOf(element, element.length);
      NumberConversion.long2bytes(frequency, element, Long.BYTES);
    } else {
      element = wrap.getArray();
      NumberConversion.long2bytes(NumberConversion.bytes2long(element, Long.BYTES) + 1, element,
              Long.BYTES);
    }
    add(subject, predicate, graph, element);
    numberOfTriples++;
  }

  private void add(long subject, long predicate, long graph, byte[] key) {
    if (entriesInBatch == null) {
      entriesInBatch = new HashMap<>();
    }
    boolean isNew = entriesInBatch.put(new LongArrayWrapper(subject, predicate, graph),
            new ArrayWrapper(key)) == null;
    if (isNew && (entriesInBatch.size() == maxBatchEntries)) {
      internalFlush();
    }
  }

  @Override
  public void flush() {
    internalFlush();
    try {
      snpgIndex.compactRange();
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private void internalFlush() {
    WriteOptions writeOpts = new WriteOptions();
    try {
      if (deletionBatch == null) {
        deletionBatch = new WriteBatch();
      }
      if (entriesInBatch != null) {
        for (ArrayWrapper elem : entriesInBatch.values()) {
          deletionBatch.put(elem.getArray(), RocksDBSNPGIndex.EMPTY_BYTE_ARRAY);
        }
      }
      snpgIndex.write(writeOpts, deletionBatch);
      deletionBatch = null;
      if (entriesInBatch != null) {
        entriesInBatch.clear();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      writeOpts.dispose();
    }
  }

  @Override
  public long getNumberOfTriples() {
    return numberOfTriples;
  }

  @Override
  public Iterator<long[]> iterator() {
    RocksIterator iterator = snpgIndex.newIterator();
    return new ByteArray2LongArrayIterator(
            new RocksIteratorKeyWrapper(iterator, RocksDBSNPGIndex.EMPTY_BYTE_ARRAY).iterator());
  }

  @Override
  public Iterable<long[]> npgIterable(long subject) {
    byte[] prefix = NumberConversion.long2bytes(subject);
    RocksIterator iterator = snpgIndex.newIterator();
    return new ByteArray2LongArrayIterator(
            new RocksIteratorKeyWrapper(iterator, prefix).iterator());
  }

  @Override
  public Iterator<long[]> npgIterator(long subject) {
    return npgIterable(subject).iterator();
  }

  @Override
  public long getMaxFrequency(long subject) {
    long maxFrequency = 0;
    for (long[] snpg : npgIterable(subject)) {
      if (maxFrequency < snpg[1]) {
        maxFrequency = snpg[1];
      }
    }
    return maxFrequency;
  }

  @Override
  public void close() {
    saveNumberOfTriples();
    internalFlush();
    if (snpgIndex != null) {
      snpgIndex.close();
    }
  }

}
