package playground;

import org.apache.jena.graph.Node;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.system.IRIResolver;

import de.unikoblenz.west.splodge.dictionary.Dictionary;
import de.unikoblenz.west.splodge.dictionary.RocksDBDictionary;
import de.unikoblenz.west.splodge.inputGraphProcessor.RDFFileIterator;
import de.unikoblenz.west.splodge.statistics.MapDBSGPOIndex;
import de.unikoblenz.west.splodge.statistics.RocksDBSGPOIndex;
import de.unikoblenz.west.splodge.statistics.SGPOIndex;

import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Tests different implementations of {@link SGPOIndex}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SGPOTest {

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

    if ((args.length < 2) || (Integer.parseInt(args[1]) != 2)) {
      Dictionary dictionary = new RocksDBDictionary(
              workingDir.getAbsolutePath() + File.separator + "mapDBDictionary");

      System.out.println("MapDB implementation");

      SGPOIndex sgpoIndex = new MapDBSGPOIndex(
              workingDir.getAbsolutePath() + File.separator + "mapDBsgpo");
      SGPOTest.measureTimes(inputFile, dictionary, sgpoIndex);

      dictionary.close();
      sgpoIndex.close();
    }

    System.out.println();

    if ((args.length < 2) || (Integer.parseInt(args[1]) != 1)) {
      Dictionary dictionary = new RocksDBDictionary(
              workingDir.getAbsolutePath() + File.separator + "rocksDBDictionary");

      System.out.println("RocksDB implementation");
      SGPOIndex sgpoIndex = new RocksDBSGPOIndex(
              workingDir.getAbsolutePath() + File.separator + "rocksDBsgpo");
      SGPOTest.measureTimes(inputFile, dictionary, sgpoIndex);

      dictionary.close();
      sgpoIndex.close();
    }

    SGPOTest.delete(workingDir);
  }

  private static void measureTimes(File inputFile, Dictionary dictionary, SGPOIndex sgpoIndex) {
    DateFormat format = DateFormat.getDateTimeInstance();

    System.out.println("\twriting");
    long start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    SGPOTest.collectStatistics(inputFile, dictionary, sgpoIndex);
    long end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    long duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));

    System.out.println("\treading");
    start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    SGPOTest.readStatistics(dictionary, sgpoIndex);
    end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));
  }

  private static void collectStatistics(File input, Dictionary dictionary, SGPOIndex sgpoIndex) {
    try (RDFFileIterator iterator = new RDFFileIterator(input, false, null);) {
      long defaultGraph = dictionary.encode("<urn:uri:defaultGraph>", true);
      for (Node[] statement : iterator) {
        String subject = SGPOTest.serializeNode(statement[0]);
        long subEnc = dictionary.encode(subject, true);

        String predicate = SGPOTest.serializeNode(statement[1]);
        long predEnc = dictionary.encode(predicate, true);

        String object = SGPOTest.serializeNode(statement[2]);
        long objEnc = dictionary.encode(object, true);

        long graphEnc = defaultGraph;
        if ((statement.length > 3) && statement[3].isURI()) {
          String graph = SGPOTest.serializeNode(statement[3]);
          graph = SGPOTest.getTopLevelDomain(graph);
          graphEnc = dictionary.encode(graph, true);
        }
        sgpoIndex.add(subEnc, predEnc, objEnc, graphEnc);
      }
    }
    dictionary.flush();
    sgpoIndex.flush();
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

  private static void readStatistics(Dictionary dictionary, SGPOIndex sgpoIndex) {
    long numberOfTriples = 1_000_000_000;// sgpoIndex.getNumberOfTriples();
    int numberOfReads = (int) (numberOfTriples * 0.5);
    System.out.println("\tNumber of reads: " + numberOfReads);
    Random rand = new Random(System.currentTimeMillis());
    int bound = (numberOfTriples * 3) > Integer.MAX_VALUE ? Integer.MAX_VALUE
            : (int) numberOfTriples * 3;
    for (int i = 0; i < numberOfReads;) {
      int nextInt = rand.nextInt(bound);
      for (@SuppressWarnings("unused")
      long[] triple : sgpoIndex.pgoIterable(nextInt)) {
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
        SGPOTest.delete(file);
      } else {
        file.delete();
      }
    }
    dir.delete();
  }

}
