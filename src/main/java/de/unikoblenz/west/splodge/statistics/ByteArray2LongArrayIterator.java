package de.unikoblenz.west.splodge.statistics;

import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.util.Iterator;

/**
 * Wrappes byte[] to long[]
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ByteArray2LongArrayIterator implements Iterable<long[]>, Iterator<long[]> {

  private final Iterator<byte[]> iter;

  public ByteArray2LongArrayIterator(Iterable<byte[]> iterable) {
    this(iterable.iterator());
  }

  public ByteArray2LongArrayIterator(Iterator<byte[]> iterator) {
    iter = iterator;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public long[] next() {
    byte[] next = iter.next();
    long[] result = new long[next.length / Long.BYTES];
    for (int i = 0; i < result.length; i++) {
      result[i] = NumberConversion.bytes2long(next, i * Long.BYTES);
    }
    return result;
  }

  @Override
  public Iterator<long[]> iterator() {
    return this;
  }

  public Iterator<byte[]> getWrappedIterator() {
    return iter;
  }

}
