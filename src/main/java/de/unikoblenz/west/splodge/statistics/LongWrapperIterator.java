package de.unikoblenz.west.splodge.statistics;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Wraps an iterator over two Long values to a long[] of length 2.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LongWrapperIterator implements Iterable<long[]>, Iterator<long[]> {

  private final Iterator<Entry<Long, Long>> iter;

  public LongWrapperIterator(Iterable<Entry<Long, Long>> iterable) {
    iter = iterable.iterator();
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public long[] next() {
    Entry<Long, Long> next = iter.next();
    return new long[] { next.getKey(), next.getValue() };
  }

  @Override
  public Iterator<long[]> iterator() {
    return this;
  }

}
