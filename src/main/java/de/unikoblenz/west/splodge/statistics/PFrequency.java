package de.unikoblenz.west.splodge.statistics;

import java.io.Closeable;
import java.util.NoSuchElementException;

/**
 * Persistent Map for measuring the frequency of properties.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface PFrequency extends Closeable, AutoCloseable {

  public void put(long key, long value);

  /**
   * @param key
   * @return
   * @throws NoSuchElementException
   *           <code>key</code> is not contained
   */
  public long get(long key);

  public void flush();

  @Override
  public void close();

}
