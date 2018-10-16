package de.unikoblenz.west.splodge.dictionary;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.Serializer;

import de.unikoblenz.west.splodge.mapdb.BTreeMapWrapper;
import de.unikoblenz.west.splodge.mapdb.HashTreeMapWrapper;
import de.unikoblenz.west.splodge.mapdb.MapDBCacheOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBDataStructureOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBMapWrapper;
import de.unikoblenz.west.splodge.mapdb.MapDBStorageOptions;

import java.io.File;

/**
 * Implements {@link Dictionary} with MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBDictionary implements Dictionary {

  private final MapDBMapWrapper<String, Long> encoder;

  private final MapDBMapWrapper<Long, String> decoder;

  /**
   * id 0 indicates that a string in a query has not been encoded yet
   */
  private long nextID = 1;

  private final long maxID = 0x7fffffffffffffffl;

  public MapDBDictionary(String storageDir) {
    this(MapDBStorageOptions.MEMORY_MAPPED_FILE, MapDBDataStructureOptions.HASH_TREE_MAP,
            storageDir, false, true, MapDBCacheOptions.HASH_TABLE);
  }

  @SuppressWarnings("unchecked")
  public MapDBDictionary(MapDBStorageOptions storageType, MapDBDataStructureOptions dataStructure,
          String storageDir, boolean useTransactions, boolean writeAsynchronously,
          MapDBCacheOptions cacheType) {
    File dictionaryDir = new File(storageDir);
    if (!dictionaryDir.exists()) {
      dictionaryDir.mkdirs();
    }
    try {
      switch (dataStructure) {
        case B_TREE_MAP:
          encoder = new BTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "encoder.db",
                  useTransactions, writeAsynchronously, cacheType, "encoder",
                  BTreeKeySerializer.STRING, Serializer.LONG, false);
          decoder = new BTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "decoder.db",
                  useTransactions, writeAsynchronously, cacheType, "decoder",
                  BTreeKeySerializer.BASIC, Serializer.STRING, true);
          break;
        case HASH_TREE_MAP:
        default:
          encoder = new HashTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "encoder.db",
                  useTransactions, writeAsynchronously, cacheType, "encoder",
                  new Serializer.CompressionWrapper<>(Serializer.STRING), Serializer.LONG);
          decoder = new HashTreeMapWrapper<>(storageType,
                  dictionaryDir.getAbsolutePath() + File.separatorChar + "decoder.db",
                  useTransactions, writeAsynchronously, cacheType, "decoder", Serializer.LONG,
                  new Serializer.CompressionWrapper<>(Serializer.STRING));
      }
    } catch (Throwable e) {
      close();
      throw e;
    }
    resetNextId();
  }

  private void resetNextId() {
    try {
      for (Long usedIds : decoder.keySet()) {
        if (usedIds != null) {
          long id = usedIds.longValue();
          // delete ownership
          id = id << 16;
          id = id >>> 16;
          if (id >= nextID) {
            nextID = id + 1;
          }
        }
      }
    } catch (Throwable e) {
      close();
      throw e;
    }
  }

  @Override
  public long encode(String value, boolean createNewEncodingForUnknownNodes) {
    Long id = null;
    try {
      id = encoder.get(value);
    } catch (Throwable e) {
      close();
      throw e;
    }
    if (id == null) {
      if (nextID > maxID) {
        throw new RuntimeException("The maximum number of Strings have been encoded.");
      } else if (!createNewEncodingForUnknownNodes) {
        return 0;
      } else {
        try {
          id = nextID;
          encoder.put(value, id);
          decoder.put(id, value);
          nextID++;
        } catch (Throwable e) {
          close();
          throw e;
        }
      }
    }
    return id.longValue();
  }

  @Override
  public String decode(long id) {
    try {
      return decoder.get(id);
    } catch (Throwable e) {
      close();
      throw e;
    }
  }

  @Override
  public boolean isEmpty() {
    return nextID == 1;
  }

  @Override
  public void clear() {
    if (encoder != null) {
      encoder.clear();
    }
    if (decoder != null) {
      decoder.clear();
    }
    nextID = 1;
  }

  @Override
  public void close() {
    if (encoder != null) {
      encoder.close();
    }
    if (decoder != null) {
      decoder.close();
    }
  }

  @Override
  public void flush() {
  }

}
