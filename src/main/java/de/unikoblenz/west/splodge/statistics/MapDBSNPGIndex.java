package de.unikoblenz.west.splodge.statistics;

import de.unikoblenz.west.splodge.mapdb.MapDBCacheOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBStorageOptions;
import de.unikoblenz.west.splodge.mapdb.MultiMap;
import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Stores the incidence list of each vertex sorted by its frequency. MapDB is
 * used for the implementation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBSNPGIndex implements SNPGIndex {

  private long numberOfTriples;

  private final MultiMap snpgIndex;

  public MapDBSNPGIndex(String databaseFile) {
    snpgIndex = new MultiMap(MapDBStorageOptions.MEMORY_MAPPED_FILE, databaseFile, false, true,
            MapDBCacheOptions.HASH_TABLE, "snpgIndex");
    numberOfTriples = snpgIndex.size();
  }

  @Override
  public void add(long subject, long predicate, long object, long graph) {
    numberOfTriples++;

    byte[] prefix = NumberConversion.long2bytes(subject);

    long frequency = 1;
    byte[] toDelete = null;
    NavigableSet<byte[]> matches = snpgIndex.get(prefix);
    if (!matches.isEmpty()) {
      for (byte[] snpg : matches) {
        if ((graph == NumberConversion.bytes2long(snpg, 3 * Long.BYTES))
                && (predicate == NumberConversion.bytes2long(snpg, 2 * Long.BYTES))) {
          frequency = NumberConversion.bytes2long(snpg, 1 * Long.BYTES) + 1;
          toDelete = snpg;
          break;
        }
      }
      if (toDelete != null) {
        snpgIndex.remove(toDelete);
      }
    }

    byte[] snpgEntry = new byte[4 * Long.BYTES];
    NumberConversion.long2bytes(subject, snpgEntry, 0 * Long.BYTES);
    NumberConversion.long2bytes(frequency, snpgEntry, 1 * Long.BYTES);
    NumberConversion.long2bytes(predicate, snpgEntry, 2 * Long.BYTES);
    NumberConversion.long2bytes(graph, snpgEntry, 3 * Long.BYTES);

    snpgIndex.put(snpgEntry);
  }

  @Override
  public void flush() {
  }

  @Override
  public long getNumberOfTriples() {
    return numberOfTriples;
  }

  @Override
  public Iterator<long[]> iterator() {
    return new ByteArray2LongArrayIterator(snpgIndex);
  }

  @Override
  public Iterable<long[]> npgIterable(long subject) {
    NavigableSet<byte[]> iterable = snpgIndex.get(NumberConversion.long2bytes(subject));
    return new ByteArray2LongArrayIterator(iterable.descendingIterator());
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
    snpgIndex.close();
  }

}
