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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Stores all relevant statistical informations in RocksDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBSGPOIndex implements SGPOIndex {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final RocksDB sgpoIndex;

  private WriteBatch batch;

  private Set<ArrayWrapper> entriesInBatch;

  private final int maxBatchEntries;

  private final File numberOfTriplesFile;

  private long numberOfTriples;

  public RocksDBSGPOIndex(String databaseFile) {
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
      sgpoIndex = RocksDB.open(options, indexDir + File.separator + "sgpoIndex");
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
    maxBatchEntries = 100000;

    numberOfTriplesFile = new File(databaseFile + File.separator + "sgpoIndex.numberOfTriples");
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
    byte[] sgpoEntry = new byte[Long.BYTES * 4];
    NumberConversion.long2bytes(subject, sgpoEntry, 0 * Long.BYTES);
    NumberConversion.long2bytes(graph, sgpoEntry, 1 * Long.BYTES);
    NumberConversion.long2bytes(predicate, sgpoEntry, 2 * Long.BYTES);
    NumberConversion.long2bytes(object, sgpoEntry, 3 * Long.BYTES);
    add(sgpoEntry);
    numberOfTriples++;
  }

  private void add(byte[] key) {
    if (entriesInBatch == null) {
      entriesInBatch = new HashSet<>();
    }
    boolean isNew = entriesInBatch.add(new ArrayWrapper(key));
    if (isNew) {
      if (batch == null) {
        batch = new WriteBatch();
      }
      batch.put(key, RocksDBSGPOIndex.EMPTY_BYTE_ARRAY);
      if (entriesInBatch.size() == maxBatchEntries) {
        internalFlush();
      }
    }
  }

  @Override
  public void flush() {
    internalFlush();
    try {
      sgpoIndex.compactRange();
    } catch (RocksDBException e) {
      close();
      throw new RuntimeException(e);
    }
  }

  private void internalFlush() {
    try {
      WriteOptions writeOpts = new WriteOptions();
      if (batch != null) {
        sgpoIndex.write(writeOpts, batch);
        batch = null;
      }
      if (entriesInBatch != null) {
        entriesInBatch.clear();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getNumberOfTriples() {
    return numberOfTriples;
  }

  @Override
  public Iterable<long[]> getObject(long subject, long predicate, long graph) {
    byte[] sgpoEntry = new byte[Long.BYTES * 3];
    NumberConversion.long2bytes(subject, sgpoEntry, 0 * Long.BYTES);
    NumberConversion.long2bytes(graph, sgpoEntry, 1 * Long.BYTES);
    NumberConversion.long2bytes(predicate, sgpoEntry, 2 * Long.BYTES);
    RocksIterator iterator = sgpoIndex.newIterator();
    return new ByteArray2LongArrayIterator(
            new RocksIteratorKeyWrapper(iterator, sgpoEntry).iterator());
  }

  @Override
  public Iterator<long[]> iterator() {
    RocksIterator iterator = sgpoIndex.newIterator();
    return new ByteArray2LongArrayIterator(
            new RocksIteratorKeyWrapper(iterator, RocksDBSGPOIndex.EMPTY_BYTE_ARRAY).iterator());
  }

  @Override
  public Iterable<long[]> pgoIterable(long subject) {
    byte[] prefix = NumberConversion.long2bytes(subject);
    RocksIterator iterator = sgpoIndex.newIterator();
    return new ByteArray2LongArrayIterator(
            new RocksIteratorKeyWrapper(iterator, prefix).iterator());
  }

  @Override
  public Iterator<long[]> pgoIterator(long subject) {
    return pgoIterable(subject).iterator();
  }

  @Override
  public void close() {
    saveNumberOfTriples();
    internalFlush();
    if (sgpoIndex != null) {
      sgpoIndex.close();
    }
  }

}
