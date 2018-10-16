/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.unikoblenz.west.splodge;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jena.graph.Node;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.system.IRIResolver;

import de.unikoblenz.west.splodge.dictionary.Dictionary;
import de.unikoblenz.west.splodge.dictionary.RocksDBDictionary;
import de.unikoblenz.west.splodge.inputGraphProcessor.RDFFileIterator;
import de.unikoblenz.west.splodge.mapdb.MapDBCacheOptions;
import de.unikoblenz.west.splodge.mapdb.MapDBStorageOptions;
import de.unikoblenz.west.splodge.mapdb.MultiMap;
import de.unikoblenz.west.splodge.queryGenerator.QueryGenerator;
import de.unikoblenz.west.splodge.statistics.ByteArray2LongArrayIterator;
import de.unikoblenz.west.splodge.statistics.MapDBPFrequency;
import de.unikoblenz.west.splodge.statistics.PFrequency;
import de.unikoblenz.west.splodge.statistics.RocksDBSGPOIndex;
import de.unikoblenz.west.splodge.statistics.RocksDBSNPGIndex;
import de.unikoblenz.west.splodge.statistics.SGPOIndex;
import de.unikoblenz.west.splodge.statistics.SNPGIndex;
import de.unikoblenz.west.splodge.utils.NumberConversion;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 
 * The base class to call SPLODGE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Splodge implements Closeable {

  private final boolean deleteIntermediateData;

  private final File splodgeWorkingDir;

  private final Dictionary dictionary;

  private final SGPOIndex sgpoIndex;

  private final SNPGIndex snpgIndex;

  private final PFrequency pFrequency;

  private final MultiMap sortedSubjectSet;

  private final QueryGenerator queryGenerator;

  /**
   * @param workingDir
   *          location where temporary files are stored
   * @param input
   *          the graph file or the directory with the graph files
   * @param outputDir
   *          the directory where the generated queries are written to
   * @param deleteIntermediateData
   *          if set to true, the intermediate data is deleted
   */
  public Splodge(File workingDir, File input, File outputDir, boolean deleteIntermediateData) {
    this.deleteIntermediateData = deleteIntermediateData;
    try {
      splodgeWorkingDir = new File(workingDir.getAbsolutePath() + File.separator + "splodge");
      boolean loadOldData = splodgeWorkingDir.exists();
      if (!splodgeWorkingDir.exists()) {
        splodgeWorkingDir.mkdirs();
      }
      dictionary = new RocksDBDictionary(
              splodgeWorkingDir.getAbsolutePath() + File.separator + "dictionary");
      sgpoIndex = new RocksDBSGPOIndex(
              splodgeWorkingDir.getAbsolutePath() + File.separator + "sgpo");
      snpgIndex = new RocksDBSNPGIndex(
              splodgeWorkingDir.getAbsolutePath() + File.separator + "snpg");
      pFrequency = new MapDBPFrequency(
              splodgeWorkingDir.getAbsolutePath() + File.separator + "pFrequency");

      sortedSubjectSet = new MultiMap(MapDBStorageOptions.MEMORY_MAPPED_FILE,
              splodgeWorkingDir.getAbsolutePath() + File.separator + "sSet", false, true,
              MapDBCacheOptions.HASH_TABLE, "sSet");

      if (!loadOldData) {
        collectStatistics(input);
        sortSubjects();
      }

      queryGenerator = new QueryGenerator(outputDir);
    } catch (Throwable t) {
      close();
      throw t;
    }
  }

  private void collectStatistics(File input) {
    System.out.println("Collecting statistics...");
    try (RDFFileIterator iterator = new RDFFileIterator(input, false, null);) {
      long defaultGraph = dictionary.encode("<urn:uri:defaultGraph>", true);
      long numberOfTriples = 0;
      for (Node[] statement : iterator) {
        String subject = serializeNode(statement[0]);
        long subEnc = dictionary.encode(subject, true);

        String predicate = serializeNode(statement[1]);
        long predEnc = dictionary.encode(predicate, true);

        String object = serializeNode(statement[2]);
        long objEnc = dictionary.encode(object, true);

        long graphEnc = defaultGraph;
        if ((statement.length > 3) && statement[3].isURI()) {
          String graph = serializeNode(statement[3]);
          graph = getTopLevelDomain(graph);
          graphEnc = dictionary.encode(graph, true);
        }
        sgpoIndex.add(subEnc, predEnc, objEnc, graphEnc);
        snpgIndex.add(subEnc, predEnc, objEnc, graphEnc);
        long frequency = 1;
        try {
          frequency = pFrequency.get(predEnc) + 1;
        } catch (NoSuchElementException e) {
          // this element is not found
          frequency = 1;
        }
        pFrequency.put(predEnc, frequency);
        numberOfTriples++;
        if ((numberOfTriples % 1_000_000) == 0) {
          System.out.println("\tprocessed " + (numberOfTriples / 1_000_000) + "M triples");
        }
      }
    }
    dictionary.flush();
    sgpoIndex.flush();
    snpgIndex.flush();
    pFrequency.flush();
  }

  private String serializeNode(Node node) {
    return NodeFmtLib.str(node);
  }

  private String getTopLevelDomain(String iriStr) {
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

  private void sortSubjects() {
    System.out.println("Sort subjects...");
    long[] previous = null;
    for (long[] triple : sgpoIndex) {
      if ((previous == null) || (previous[0] != triple[0])) {
        long frequency = snpgIndex.getMaxFrequency(triple[0]);
        byte[] nSubject = new byte[2 * Long.BYTES];
        NumberConversion.long2bytes(frequency, nSubject, 0);
        NumberConversion.long2bytes(triple[0], nSubject, Long.BYTES);
        sortedSubjectSet.put(nSubject);
        previous = triple;
      }
    }
  }

  /**
   * Generates the defined set of queries according to the parameters.
   *
   * @param joinPattern
   * @param numberOfJoins
   * @param numberOfDataSources
   * @param selectivity
   *          the percentage of triples that are touched during the query
   *          processing
   * @param timeout
   *          the number of milliseconds after which the search from one subject
   *          is aborted
   * @param limit
   *          the maximum number of returned results. If set to a value less or
   *          equal 0 all results are returned
   */
  public void generateQueries(JoinPattern joinPattern, int numberOfJoins, int numberOfDataSources,
          double selectivity, int timeout, int limit) {
    generateQueries(joinPattern, numberOfJoins, numberOfDataSources, selectivity, 1d, timeout,
            limit);
  }

  /**
   * Generates the defined set of queries according to the parameters.
   *
   * @param joinPattern
   * @param numberOfJoins
   * @param numberOfDataSources
   * @param selectivity
   *          the percentage of triples that are touched during the query
   *          processing
   * @param maximumSelectivity
   * @param timeout
   *          the number of milliseconds after which the search from one subject
   *          is aborted
   * @param limit
   *          the maximum number of returned results. If set to a value less or
   *          equal 0 all results are returned
   * @return -1 if no query could be found
   */
  public long generateQueries(JoinPattern joinPattern, int numberOfJoins, int numberOfDataSources,
          double selectivity, double maximumSelectivity, int timeout, int limit) {
    String queryFileName = getQueryFileName(joinPattern, numberOfJoins, numberOfDataSources,
            selectivity, maximumSelectivity);
    System.out.println("Generating query " + queryFileName);
    long[] predicatesEnc = new long[numberOfJoins + 1];
    long requiredNumberOfSelectedTriples = (long) Math
            .ceil(sgpoIndex.getNumberOfTriples() * selectivity);
    long maximumNumberOfSelectedTriples = (long) Math
            .ceil(sgpoIndex.getNumberOfTriples() * maximumSelectivity);
    System.out.println(requiredNumberOfSelectedTriples + " <= selectedNumberOfTriple <= "
            + maximumNumberOfSelectedTriples);
    long estimatedNumberOfTouchedTriples = -1;
    if ((numberOfJoins + 1) >= numberOfDataSources) {
      switch (joinPattern) {
        case SUBJECT_OBJECT_JOIN:
          estimatedNumberOfTouchedTriples = generatePathShapedQuery(predicatesEnc,
                  numberOfDataSources, requiredNumberOfSelectedTriples,
                  maximumNumberOfSelectedTriples, timeout);
          break;
        case SUBJECT_SUBJECT_JOIN:
          estimatedNumberOfTouchedTriples = generateStarShapedQuery(predicatesEnc,
                  numberOfDataSources, requiredNumberOfSelectedTriples,
                  maximumNumberOfSelectedTriples, timeout);
          break;
        default:
          throw new IllegalArgumentException(
                  "The join pattern " + joinPattern + " is currently not supported.");
      }
    }
    if (estimatedNumberOfTouchedTriples < requiredNumberOfSelectedTriples) {
      System.out.println("No query found for joinPattern=" + joinPattern + " numberOfJoins="
              + numberOfJoins + " numberOfDataSources=" + numberOfDataSources + " selectivity=["
              + selectivity + "," + maximumSelectivity + "].");
      return -1;
    }
    String[] predicates = new String[predicatesEnc.length];
    for (int i = 0; i < predicatesEnc.length; i++) {
      predicates[i] = dictionary.decode(predicatesEnc[i]);
      assert predicates[i] != null : "index " + i + " of " + Arrays.toString(predicatesEnc);
    }
    queryGenerator.createQuery(queryFileName,
            estimatedNumberOfTouchedTriples / (double) sgpoIndex.getNumberOfTriples(), joinPattern,
            limit, predicates);
    return estimatedNumberOfTouchedTriples;
  }

  private String getQueryFileName(JoinPattern joinPattern, int numberOfJoins,
          int numberOfDataSources, double selectivity, double maxSelectivity) {
    return "query-" + joinPattern + "-" + numberOfJoins + "-" + numberOfDataSources + "-"
            + selectivity + "-" + maxSelectivity + ".sparql";
  }

  private long generatePathShapedQuery(long[] predicates, int numberOfDataSources,
          long requiredNumberOfSelectedTriples, long maximumNumberOfSelectedTriples, int timeout) {
    long[] graphs = new long[predicates.length];
    long[] vertices = new long[predicates.length + 1];
    long number = 0;
    long numberOfSubjects = sortedSubjectSet.size();
    for (long[] nSubject : new ByteArray2LongArrayIterator(sortedSubjectSet.reverseIterator())) {
      System.out.println("checking " + (++number) + "/" + numberOfSubjects + " subjects");
      long abortionTime = System.currentTimeMillis() + timeout;
      for (long[] sgpo : sgpoIndex.pgoIterable(nSubject[1])) {
        graphs[0] = sgpo[1];
        predicates[0] = sgpo[2];
        vertices[0] = sgpo[0];
        vertices[1] = sgpo[3];
        long touchedTriples = findPath(1, predicates, graphs, vertices, numberOfDataSources,
                pFrequency.get(predicates[0]), requiredNumberOfSelectedTriples,
                maximumNumberOfSelectedTriples, abortionTime);
        if ((requiredNumberOfSelectedTriples <= touchedTriples)
                && (touchedTriples <= maximumNumberOfSelectedTriples)) {
          return touchedTriples;
        }
        if (System.currentTimeMillis() > abortionTime) {
          break;
        }
      }
    }
    return -1;
  }

  private long findPath(int currentIndex, long[] predicates, long[] graphs, long[] vertices,
          int numberOfDataSources, long numberOfTouchedTriplesSoFar,
          long requiredNumberOfSelectedTriples, long maximumNumberOfSelectedTriples,
          long abortionTime) {
    if (currentIndex == predicates.length) {
      if ((requiredNumberOfSelectedTriples <= numberOfTouchedTriplesSoFar)
              && (numberOfTouchedTriplesSoFar <= maximumNumberOfSelectedTriples)) {
        return numberOfTouchedTriplesSoFar;
      } else {
        return -1;
      }
    } else {
      Iterable<long[]> snpgIterable = snpgIndex.npgIterable(vertices[currentIndex]);
      if (snpgIterable.iterator().hasNext()) {
        for (long[] snpg : snpgIterable) {
          if (!isGraphAllowed(snpg[3], graphs, currentIndex, numberOfDataSources)) {
            continue;
          }
          graphs[currentIndex] = snpg[3];
          predicates[currentIndex] = snpg[2];
          long nextTouchedTriples = numberOfTouchedTriplesSoFar;
          if (!isContained(snpg[2], predicates, currentIndex)) {
            nextTouchedTriples += pFrequency.get(snpg[2]);
          }
          if (nextTouchedTriples > maximumNumberOfSelectedTriples) {
            continue;
          }
          for (long[] sgpo : sgpoIndex.getObject(snpg[0], snpg[2], snpg[3])) {
            if (isContained(sgpo[3], vertices, currentIndex + 1)) {
              continue;
            }
            vertices[currentIndex + 1] = sgpo[3];
            long touchedTriples = findPath(currentIndex + 1, predicates, graphs, vertices,
                    numberOfDataSources, nextTouchedTriples, requiredNumberOfSelectedTriples,
                    maximumNumberOfSelectedTriples, abortionTime);
            if ((requiredNumberOfSelectedTriples <= touchedTriples)
                    && (touchedTriples <= maximumNumberOfSelectedTriples)) {
              return touchedTriples;
            }
            if (System.currentTimeMillis() > abortionTime) {
              return -1;
            }
          }
        }
      }
      return -1;
    }
  }

  private long generateStarShapedQuery(long[] predicates, int numberOfDataSources,
          long requiredNumberOfSelectedTriples, long maximumNumberOfSelectedTriples, int timeout) {
    long number = 0;
    long numberOfSubjects = sortedSubjectSet.size();
    for (long[] nSubject : new ByteArray2LongArrayIterator(sortedSubjectSet.reverseIterator())) {
      System.out.println("checking " + (++number) + "/" + numberOfSubjects + " subjects");

      Set<long[]> incidenceList = new HashSet<>();
      for (long[] sgpo : sgpoIndex.pgoIterable(nSubject[1])) {
        incidenceList.add(new long[] { sgpo[2], sgpo[1] });
      }

      long abortionTime = System.currentTimeMillis() + timeout;

      long numberOfUsedTriples = findStar(0,
              incidenceList.toArray(new long[incidenceList.size()][]), 0, predicates,
              new long[predicates.length], numberOfDataSources, 0, requiredNumberOfSelectedTriples,
              maximumNumberOfSelectedTriples, abortionTime);
      if ((requiredNumberOfSelectedTriples <= numberOfUsedTriples)
              && (numberOfUsedTriples <= maximumNumberOfSelectedTriples)) {
        return numberOfUsedTriples;
      }

    }
    return -1;
  }

  /**
   * @param currentIndex
   * @param incidenceList
   *          list of (predicate,graph) tuples
   * @param firstIncidenceIndex
   * @param predicates
   * @param graphs
   * @param numberOfDataSources
   * @param numberOfUsedTriples
   * @param requiredNumberOfSelectedTriples
   * @param maximumNumberOfSelectedTriples
   * @param abortionTime
   * @return
   */
  private long findStar(int currentIndex, long[][] incidenceList, int firstIncidenceIndex,
          long[] predicates, long[] graphs, int numberOfDataSources, long numberOfUsedTriples,
          long requiredNumberOfSelectedTriples, long maximumNumberOfSelectedTriples,
          long abortionTime) {
    if (numberOfUsedTriples > maximumNumberOfSelectedTriples) {
      return -1;
    } else if (currentIndex == predicates.length) {
      if ((requiredNumberOfSelectedTriples <= numberOfUsedTriples)
              && (numberOfUsedTriples <= maximumNumberOfSelectedTriples)) {
        return numberOfUsedTriples;
      } else {
        return -1;
      }
    } else if ((((incidenceList.length - firstIncidenceIndex)
            + currentIndex) < predicates.length)) {
      // there are not enough remaining triples to get the number of required
      // triple patterns
      return -1;
    } else {
      for (int i = firstIncidenceIndex; i < incidenceList.length; i++) {
        long[] currentIncidence = incidenceList[i];
        if (isContained(currentIncidence[0], predicates, currentIndex)) {
          // predicates should occur only once
          continue;
        }
        if (!isGraphAllowed(currentIncidence[1], graphs, currentIndex, numberOfDataSources)) {
          continue;
        }
        predicates[currentIndex] = currentIncidence[0];
        graphs[currentIndex] = currentIncidence[1];
        long numberOfTouchedTriples = findStar(currentIndex + 1, incidenceList, i + 1, predicates,
                graphs, numberOfDataSources,
                numberOfUsedTriples + pFrequency.get(predicates[currentIndex]),
                requiredNumberOfSelectedTriples, maximumNumberOfSelectedTriples, abortionTime);
        if ((requiredNumberOfSelectedTriples <= numberOfTouchedTriples)
                && (numberOfTouchedTriples <= maximumNumberOfSelectedTriples)) {
          return numberOfTouchedTriples;
        }
        if (System.currentTimeMillis() > abortionTime) {
          return -1;
        }
      }
      return -1;
    }
  }

  private boolean isContained(long predicate, long[] predicates, int excludedUpperIndex) {
    for (int i = 0; i < excludedUpperIndex; i++) {
      long pred = predicates[i];
      if (pred == predicate) {
        return true;
      }
    }
    return false;
  }

  private boolean isGraphAllowed(long nextGraph, long[] graphs, int excludedUpperIndex,
          int numberOfDataSources) {
    boolean isGraphAlreadyKnown = isContained(nextGraph, graphs, excludedUpperIndex);
    int numberOfUsedGraphs = 0;
    for (int i = 0; i < excludedUpperIndex; i++) {
      boolean isGraphKnown = false;
      for (int j = 0; j < i; j++) {
        if (graphs[i] == graphs[j]) {
          isGraphKnown = true;
          break;
        }
      }
      if (!isGraphKnown) {
        numberOfUsedGraphs++;
      }
    }
    if (!isGraphAlreadyKnown) {
      // it is a new graph, check if a new graph is allowed
      return numberOfUsedGraphs < numberOfDataSources;
    } else {
      // is is an already seen graph, check if you can get the number of
      // required data sources if you reuse an existing graph
      int maxNumberOfPossibleGraphs = (numberOfUsedGraphs + graphs.length) - excludedUpperIndex;
      return maxNumberOfPossibleGraphs > numberOfDataSources;
    }
  }

  @Override
  public void close() {
    if (queryGenerator != null) {
      queryGenerator.close();
    }
    if (dictionary != null) {
      dictionary.close();
    }
    if (sgpoIndex != null) {
      sgpoIndex.close();
    }
    if (snpgIndex != null) {
      snpgIndex.close();
    }
    if (pFrequency != null) {
      pFrequency.close();
    }
    if (deleteIntermediateData) {
      deleteDirectory(splodgeWorkingDir);
    }
  }

  private void deleteDirectory(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File subFile : file.listFiles()) {
        deleteDirectory(subFile);
      }
    }
    file.delete();
  }

  public static void main(String[] args) throws ParseException {
    Options options = Splodge.createCommandLineOptions();
    if (args.length == 0) {
      Splodge.printUsage(options);
      return;
    }
    CommandLine line = Splodge.parseCommandLineArgs(options, args);
    if (line.hasOption("h")) {
      Splodge.printUsage(options);
      return;
    }

    File input = null;
    if (line.hasOption('i')) {
      input = new File(line.getOptionValue("i"));
    }

    File output = new File(line.getOptionValue("o"));

    File workingDir = null;
    if (line.hasOption("w")) {
      workingDir = new File(line.getOptionValue('w'));
    } else {
      workingDir = new File(System.getProperty("java.io.tmpdir"));
    }

    JoinPattern joinPattern = JoinPattern.valueOf(line.getOptionValue("j"));

    int numberOfJoins = Integer.parseInt(line.getOptionValue("n"));

    int numberOfDataSources = Integer.parseInt(line.getOptionValue("d"));

    double selectivity = Double.parseDouble(line.getOptionValue("s"));

    int timeout = Integer.MAX_VALUE;
    if (line.hasOption("t")) {
      timeout = Integer.parseInt(line.getOptionValue("t"));
    }

    int limit = -1;
    if (line.hasOption("l")) {
      limit = Integer.parseInt(line.getOptionValue("l"));
    }

    Splodge splodge = null;
    try {
      splodge = new Splodge(workingDir, input, output, line.hasOption('r'));
      if (line.hasOption('m')) {
        splodge.generateQueries(joinPattern, numberOfJoins, numberOfDataSources, selectivity,
                Double.parseDouble(line.getOptionValue('m')), timeout, limit);
      } else {
        splodge.generateQueries(joinPattern, numberOfJoins, numberOfDataSources, selectivity,
                timeout, limit);
      }
    } finally {
      if (splodge != null) {
        splodge.close();
      }
    }
  }

  private static Options createCommandLineOptions() {
    Option help = new Option("h", "help", false, "print this help message");
    help.setRequired(false);

    Option input = Option.builder("i").longOpt("input").hasArg().argName("fileOrDirctory")
            .desc("File or directory that contains the input graph. If not given, statistical information must be stored in workDir")
            .required(false).build();

    Option work = Option.builder("w").longOpt("workDir").hasArg().argName("dirctory")
            .desc("Directory where the statistics will be stored.").required(false).build();

    Option output = Option.builder("o").longOpt("output").hasArg().argName("outputDirctory")
            .desc("Directory where the generated queries are stored.").required(true).build();

    Option joinPattern = Option.builder("j").longOpt("joinPattern").hasArg()
            .argName("joinPattern").desc("The join pattern of the generated query."
                    + " Possible values are: " + Arrays.toString(JoinPattern.values()))
            .required(true).build();

    Option numberOfJoins = Option.builder("n").longOpt("numberOfJoins").hasArg().argName("int")
            .desc("The number of joins in the generated queries.").required(true).build();

    Option dataSources = Option.builder("d").longOpt("dataSources").hasArg().argName("int")
            .desc("The number of data sources like DBPedia or GeoNames that should be used by the query.")
            .required(true).build();

    Option selectivity = Option.builder("s").longOpt("selectivity").hasArg().argName("double")
            .desc("The minimal number of triples that should be used to create the query response, e.g. selectivity is in interval [s,1].")
            .required(true).build();

    Option selectivityUp = Option.builder("m").longOpt("maxSelectivity").hasArg().argName("double")
            .desc("The maximal number of triples that should be used to create the query response, e.g. selectivity is in interval [s,m].")
            .required(false).build();

    Option removeOldData = Option.builder("r").longOpt("removeIntermediateData")
            .desc("If set, the statistical information will be deleted.").required(false).build();

    Option timeOut = Option.builder("t").longOpt("timeout").hasArg().argName("sec")
            .desc("The timeout in milliseconds after which the sub-search for a query starting from one subject is aborted.")
            .required(false).build();

    Option limit = Option.builder("l").longOpt("limit").hasArg().argName("int")
            .desc("The maximim number of returned results.").required(false).build();

    Options options = new Options();
    options.addOption(help);
    options.addOption(input);
    options.addOption(work);
    options.addOption(output);
    options.addOption(joinPattern);
    options.addOption(numberOfJoins);
    options.addOption(dataSources);
    options.addOption(selectivity);
    options.addOption(selectivityUp);
    options.addOption(removeOldData);
    options.addOption(timeOut);
    options.addOption(limit);
    return options;
  }

  private static CommandLine parseCommandLineArgs(Options options, String[] args)
          throws ParseException {
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }

  protected static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
            "java " + Splodge.class.getName()
                    + " [-h] [-i <inputFileOrDirctory>] [-w <dirctory>] -o <outputDirectory> -j <joinPattern> -n <int> -d <int> -s <double> [-m <double>] [-r] [-t <timeout>] [-l <int>]",
            options);
  }

}
