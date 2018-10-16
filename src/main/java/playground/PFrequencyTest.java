package playground;

import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;

import de.unikoblenz.west.splodge.dictionary.Dictionary;
import de.unikoblenz.west.splodge.dictionary.RocksDBDictionary;
import de.unikoblenz.west.splodge.inputGraphProcessor.RDFFileIterator;
import de.unikoblenz.west.splodge.statistics.MapDBPFrequency;
import de.unikoblenz.west.splodge.statistics.PFrequency;
import de.unikoblenz.west.splodge.statistics.RocksDBPFrequency;

import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Tests different implementations of {@link PFrequency}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class PFrequencyTest {

  private static long numberOfPredicates;

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Missing input file");
      return;
    }
    File workingDir = new File(
            System.getProperty("java.io.tmpdir") + File.separator + "splodgeTest");
    if (!workingDir.exists()) {
      workingDir.mkdir();
    }

    File inputFile = new File(args[0]);

    if ((args.length < 2) || (Integer.parseInt(args[1]) != 1)) {
      Dictionary dictionary = new RocksDBDictionary(
              workingDir.getAbsolutePath() + File.separator + "rocksDBDictionary");

      System.out.println("RocksDB implementation");
      PFrequency pFrequency = new RocksDBPFrequency(
              workingDir.getAbsolutePath() + File.separator + "rocksDBsgpo");
      PFrequencyTest.measureTimes(inputFile, dictionary, pFrequency);

      dictionary.close();
      pFrequency.close();
    }

    System.out.println();

    if ((args.length < 2) || (Integer.parseInt(args[1]) != 2)) {
      Dictionary dictionary = new RocksDBDictionary(
              workingDir.getAbsolutePath() + File.separator + "mapDBDictionary");

      System.out.println("MapDB implementation");

      PFrequency pFrequency = new MapDBPFrequency(
              workingDir.getAbsolutePath() + File.separator + "mapDBsgpo");
      PFrequencyTest.measureTimes(inputFile, dictionary, pFrequency);

      dictionary.close();
      pFrequency.close();
    }

    PFrequencyTest.delete(workingDir);
  }

  private static void measureTimes(File inputFile, Dictionary dictionary, PFrequency pFrequency) {
    DateFormat format = DateFormat.getDateTimeInstance();

    System.out.println("\twriting");
    long start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    PFrequencyTest.collectStatistics(inputFile, dictionary, pFrequency);
    long end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    long duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));

    System.out.println("\treading");
    start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    PFrequencyTest.readStatistics(dictionary, pFrequency);
    end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));
  }

  private static void collectStatistics(File input, Dictionary dictionary, PFrequency pFrequency) {
    PFrequencyTest.numberOfPredicates = 0;
    try (RDFFileIterator iterator = new RDFFileIterator(input, false, null);) {
      for (Node[] statement : iterator) {
        String predicate = PFrequencyTest.serializeNode(statement[1]);
        long predEnc = dictionary.encode(predicate, true);

        long frequency = 1;
        try {
          frequency = pFrequency.get(predEnc) + 1;
        } catch (NoSuchElementException e) {
          PFrequencyTest.numberOfPredicates++;
          frequency = 1;
        }

        pFrequency.put(predEnc, frequency);
      }
    }
    dictionary.flush();
    pFrequency.flush();
  }

  private static String serializeNode(Node node) {
    return NodeFmtLib.str(node);
  }

  private static void readStatistics(Dictionary dictionary, PFrequency pFrequency) {
    int numberOfReads = PFrequencyTest.numberOfPredicates > Integer.MAX_VALUE ? Integer.MAX_VALUE
            : (int) PFrequencyTest.numberOfPredicates;
    System.out.println("\tNumber of reads: " + numberOfReads);
    Random rand = new Random(System.currentTimeMillis());
    int bound = PFrequencyTest.numberOfPredicates > Integer.MAX_VALUE ? Integer.MAX_VALUE
            : (int) PFrequencyTest.numberOfPredicates;
    for (int i = 0; i < numberOfReads; i++) {
      int nextInt = rand.nextInt(bound);
      try {
        pFrequency.get(nextInt);
      } catch (NoSuchElementException e) {
      }
    }
  }

  private static void delete(File dir) {
    for (File file : dir.listFiles()) {
      if (file.isDirectory()) {
        PFrequencyTest.delete(file);
      } else {
        file.delete();
      }
    }
    dir.delete();
  }

}
