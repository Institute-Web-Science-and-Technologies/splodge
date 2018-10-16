package de.unikoblenz.west.splodge.statistics;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Stores all relevant statistical informations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface SGPOIndex extends Closeable, Iterable<long[]> {

  public void add(long subject, long predicate, long object, long graph);

  public void flush();

  public long getNumberOfTriples();

  public Iterable<long[]> getObject(long subject, long predicate, long graph);

  @Override
  public Iterator<long[]> iterator();

  public Iterable<long[]> pgoIterable(long subject);

  public Iterator<long[]> pgoIterator(long subject);

  @Override
  public void close();

}
