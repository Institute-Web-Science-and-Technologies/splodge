package de.unikoblenz.west.splodge.statistics;

import de.unikoblenz.west.splodge.mapdb.MapDBCacheOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBStorageOptions;
import de.unikoblenz.west.splodge.mapdb.MultiMap;
import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Collects all occurring pairs of p1 g1 p2 g2, i.e.,
 * &lt;?v0,p1,?v1&gt;.&lt;?v1,p2,?v2&gt;
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CoOccurrenceIndex implements Closeable, Iterable<long[]> {

  private final MultiMap pg1pg2;

  public CoOccurrenceIndex(String databaseFile) {
    pg1pg2 = new MultiMap(MapDBStorageOptions.MEMORY_MAPPED_FILE, databaseFile + "_pg1pg2", false,
            true, MapDBCacheOptions.HASH_TABLE, "pg1pg2");
  }

  public void add(long p1, long g1, long p2, long g2) {
    byte[] prefix = new byte[Long.BYTES * 4];
    NumberConversion.long2bytes(p1, prefix, 0 * Long.BYTES);
    NumberConversion.long2bytes(g1, prefix, 1 * Long.BYTES);
    NumberConversion.long2bytes(p2, prefix, 2 * Long.BYTES);
    NumberConversion.long2bytes(g2, prefix, 3 * Long.BYTES);
    pg1pg2.put(prefix);
  }

  @Override
  public Iterator<long[]> iterator() {
    return new ByteArray2LongArrayIterator(pg1pg2);
  }

  public Iterable<long[]> getCoOccurences(long predicate, long graph) {
    byte[] prefix = getPrefix(predicate, graph);
    return new ByteArray2LongArrayIterator(pg1pg2.get(prefix));
  }

  protected byte[] getPrefix(long predicate, long graph) {
    byte[] prefix = new byte[2 * Long.BYTES];
    NumberConversion.long2bytes(predicate, prefix, 0);
    NumberConversion.long2bytes(graph, prefix, Long.BYTES);
    return prefix;
  }

  @Override
  public void close() {
    pg1pg2.close();
  }

}
