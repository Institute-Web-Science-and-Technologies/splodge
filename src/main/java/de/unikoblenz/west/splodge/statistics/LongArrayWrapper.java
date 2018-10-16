package de.unikoblenz.west.splodge.statistics;

import java.util.Arrays;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class LongArrayWrapper implements Comparable<LongArrayWrapper> {

  private final long[] array;

  public LongArrayWrapper(long... array) {
    this.array = array;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(array);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    LongArrayWrapper other = (LongArrayWrapper) obj;
    if (!Arrays.equals(array, other.array)) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(LongArrayWrapper o) {
    for (int i = 0; i < array.length; i++) {
      long diff = array[i] - o.array[i];
      if (diff != 0) {
        return diff < 0 ? -1 : 1;
      }
    }
    return 0;
  }
}
