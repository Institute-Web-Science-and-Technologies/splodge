package de.unikoblenz.west.splodge.statistics;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Stores the incidence list of each vertex sorted by its frequency.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface SNPGIndex extends Closeable, Iterable<long[]> {

  public void add(long subject, long predicate, long object, long graph);

  public void flush();

  public long getNumberOfTriples();

  @Override
  public Iterator<long[]> iterator();

  public Iterable<long[]> npgIterable(long subject);

  public Iterator<long[]> npgIterator(long subject);

  public long getMaxFrequency(long subject);

  @Override
  public void close();

}
