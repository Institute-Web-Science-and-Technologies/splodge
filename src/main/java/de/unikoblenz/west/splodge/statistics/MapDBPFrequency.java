package de.unikoblenz.west.splodge.statistics;

import org.mapdb.DB;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import de.unikoblenz.west.splodge.mapdb.MapDBCacheOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBStorageOptions;

import java.util.NoSuchElementException;

/**
 * A MapDB implementation of {@link PFrequency}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 */
public class MapDBPFrequency implements PFrequency {

  private final DB database;

  private final HTreeMap<Long, Long> map;

  public MapDBPFrequency(String databaseFile) {
    DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE.getDBMaker(databaseFile)
            .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
    dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
    database = dbmaker.make();
    HTreeMapMaker treeMaker = database.createHashMap("pFrequency").keySerializer(Serializer.LONG)
            .valueSerializer(Serializer.LONG);
    map = treeMaker.makeOrGet();
  }

  @Override
  public void put(long key, long value) {
    map.put(key, value);
  }

  @Override
  public long get(long key) {
    Long value = map.get(key);
    if (value == null) {
      throw new NoSuchElementException(key + " could not be found.");
    }
    return value;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    if (!database.isClosed()) {
      map.close();
    }
    if (!database.isClosed()) {
      database.close();
    }
  }

}
