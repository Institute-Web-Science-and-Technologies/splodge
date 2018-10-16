package playground;

import org.apache.jena.graph.Node;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.system.IRIResolver;

import de.unikoblenz.west.splodge.dictionary.Dictionary;
import de.unikoblenz.west.splodge.dictionary.RocksDBDictionary;
import de.unikoblenz.west.splodge.inputGraphProcessor.RDFFileIterator;
import de.unikoblenz.west.splodge.statistics.MapDBSNPGIndex;
import de.unikoblenz.west.splodge.statistics.RocksDBSNPGIndex;
import de.unikoblenz.west.splodge.statistics.SNPGIndex;

import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Tests different implementations of {@link SNPGIndex}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SNPGTest {

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
      SNPGIndex snpgIndex = new RocksDBSNPGIndex(
              workingDir.getAbsolutePath() + File.separator + "rocksDBsgpo");
      SNPGTest.measureTimes(inputFile, dictionary, snpgIndex);

      dictionary.close();
      snpgIndex.close();
    }

    System.out.println();

    if ((args.length < 2) || (Integer.parseInt(args[1]) != 2)) {
      Dictionary dictionary = new RocksDBDictionary(
              workingDir.getAbsolutePath() + File.separator + "mapDBDictionary");

      System.out.println("MapDB implementation");

      SNPGIndex snpgIndex = new MapDBSNPGIndex(
              workingDir.getAbsolutePath() + File.separator + "mapDBsgpo");
      SNPGTest.measureTimes(inputFile, dictionary, snpgIndex);

      dictionary.close();
      snpgIndex.close();
    }

    SNPGTest.delete(workingDir);
  }

  private static void measureTimes(File inputFile, Dictionary dictionary, SNPGIndex snpgIndex) {
    DateFormat format = DateFormat.getDateTimeInstance();

    System.out.println("\twriting");
    long start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    SNPGTest.collectStatistics(inputFile, dictionary, snpgIndex);
    long end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    long duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));

    System.out.println("\treading");
    start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    SNPGTest.readStatistics(dictionary, snpgIndex);
    end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));
  }

  private static void collectStatistics(File input, Dictionary dictionary, SNPGIndex snpgIndex) {
    try (RDFFileIterator iterator = new RDFFileIterator(input, false, null);) {
      long defaultGraph = dictionary.encode("<urn:uri:defaultGraph>", true);
      for (Node[] statement : iterator) {
        String subject = SNPGTest.serializeNode(statement[0]);
        long subEnc = dictionary.encode(subject, true);

        String predicate = SNPGTest.serializeNode(statement[1]);
        long predEnc = dictionary.encode(predicate, true);

        String object = SNPGTest.serializeNode(statement[2]);
        long objEnc = dictionary.encode(object, true);

        long graphEnc = defaultGraph;
        if ((statement.length > 3) && statement[3].isURI()) {
          String graph = SNPGTest.serializeNode(statement[3]);
          graph = SNPGTest.getTopLevelDomain(graph);
          graphEnc = dictionary.encode(graph, true);
        }
        snpgIndex.add(subEnc, predEnc, objEnc, graphEnc);
      }
    }
    dictionary.flush();
    snpgIndex.flush();
  }

  private static String serializeNode(Node node) {
    return NodeFmtLib.str(node);
  }

  private static String getTopLevelDomain(String iriStr) {
    if (iriStr.startsWith("<")) {
      iriStr = iriStr.substring(1);
      if (iriStr.endsWith(">")) {
        iriStr = iriStr.substring(0, iriStr.length() - 1);
      }
    }
    IRI iri = IRIResolver.parseIRI(iriStr);
    String host = iri.getRawHost();
    String[] hostParts = null;
    if (host != null) {
      hostParts = host.split(Pattern.quote("."));
    } else {
      return iriStr;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("http://");
    int startIndex = 0;
    for (startIndex = hostParts.length - 1; startIndex > 0; startIndex--) {
      if (hostParts[startIndex].length() > 3) {
        break;
      }
    }
    String delim = "";
    for (int i = startIndex; i < hostParts.length; i++) {
      sb.append(delim).append(hostParts[i]);
      delim = ".";
    }
    sb.append("/");
    return sb.toString();
  }

  private static void readStatistics(Dictionary dictionary, SNPGIndex snpgIndex) {
    long numberOfTriples = snpgIndex.getNumberOfTriples();
    int numberOfReads = (int) (numberOfTriples * 0.5);
    System.out.println("\tNumber of reads: " + numberOfReads);
    Random rand = new Random(System.currentTimeMillis());
    int bound = (numberOfTriples * 3) > Integer.MAX_VALUE ? Integer.MAX_VALUE
            : (int) numberOfTriples * 3;
    for (int i = 0; i < numberOfReads;) {
      int nextInt = rand.nextInt(bound);
      for (@SuppressWarnings("unused")
      long[] triple : snpgIndex.npgIterable(nextInt)) {
        i++;
        if (i == numberOfReads) {
          break;
        }
      }
    }
  }

  private static void delete(File dir) {
    for (File file : dir.listFiles()) {
      if (file.isDirectory()) {
        SNPGTest.delete(file);
      } else {
        file.delete();
      }
    }
    dir.delete();
  }

}
