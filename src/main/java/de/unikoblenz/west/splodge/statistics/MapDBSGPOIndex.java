package de.unikoblenz.west.splodge.statistics;

import de.unikoblenz.west.splodge.mapdb.MapDBCacheOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBStorageOptions;
import de.unikoblenz.west.splodge.mapdb.MultiMap;
import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Stores all relevant statistical informations in MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBSGPOIndex implements SGPOIndex {

  private long numberOfTriples;

  private final MultiMap sgpoIndex;

  public MapDBSGPOIndex(String databaseFile) {
    sgpoIndex = new MultiMap(MapDBStorageOptions.MEMORY_MAPPED_FILE, databaseFile, false, true,
            MapDBCacheOptions.HASH_TABLE, "sgpoIndex");
    numberOfTriples = sgpoIndex.size();
  }

  @Override
  public void add(long subject, long predicate, long object, long graph) {
    numberOfTriples++;

    byte[] sgpoEntry = new byte[Long.BYTES * 4];
    NumberConversion.long2bytes(subject, sgpoEntry, 0 * Long.BYTES);
    NumberConversion.long2bytes(graph, sgpoEntry, 1 * Long.BYTES);
    NumberConversion.long2bytes(predicate, sgpoEntry, 2 * Long.BYTES);
    NumberConversion.long2bytes(object, sgpoEntry, 3 * Long.BYTES);
    sgpoIndex.put(sgpoEntry);
  }

  @Override
  public void flush() {
  }

  @Override
  public long getNumberOfTriples() {
    return numberOfTriples;
  }

  @Override
  public Iterable<long[]> getObject(long subject, long predicate, long graph) {
    byte[] prefix = new byte[3 * Long.BYTES];
    NumberConversion.long2bytes(subject, prefix, 0 * Long.BYTES);
    NumberConversion.long2bytes(graph, prefix, 1 * Long.BYTES);
    NumberConversion.long2bytes(predicate, prefix, 2 * Long.BYTES);

    NavigableSet<byte[]> matches = sgpoIndex.get(prefix);
    assert !matches.isEmpty() : "Found " + matches.size() + " matches but expected at least 1.";
    return new ByteArray2LongArrayIterator(matches);
  }

  @Override
  public Iterator<long[]> iterator() {
    return new ByteArray2LongArrayIterator(sgpoIndex);
  }

  @Override
  public Iterable<long[]> pgoIterable(long subject) {
    NavigableSet<byte[]> iterable = sgpoIndex.get(NumberConversion.long2bytes(subject));
    return new ByteArray2LongArrayIterator(iterable);
  }

  @Override
  public Iterator<long[]> pgoIterator(long subject) {
    return pgoIterable(subject).iterator();
  }

  @Override
  public void close() {
    sgpoIndex.close();
  }

}
