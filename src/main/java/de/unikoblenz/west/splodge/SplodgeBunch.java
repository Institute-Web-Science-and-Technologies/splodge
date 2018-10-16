package de.unikoblenz.west.splodge;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Generates a bunch of queries for the given metrices.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SplodgeBunch {

  public static void main(String[] args) throws ParseException {
    Options options = SplodgeBunch.createCommandLineOptions();
    if (args.length == 0) {
      SplodgeBunch.printUsage(options);
      return;
    }
    CommandLine line = SplodgeBunch.parseCommandLineArgs(options, args);
    if (line.hasOption("h")) {
      SplodgeBunch.printUsage(options);
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

    String[] joinPatternStrings = SplodgeBunch.getList(line.getOptionValue("j"));
    JoinPattern[] joinPatterns = new JoinPattern[joinPatternStrings.length];
    for (int i = 0; i < joinPatternStrings.length; i++) {
      joinPatterns[i] = JoinPattern.valueOf(joinPatternStrings[i]);
    }

    String[] numberOfJoinsStrings = SplodgeBunch.getList(line.getOptionValue("n"));
    int[] numberOfJoins = new int[numberOfJoinsStrings.length];
    for (int i = 0; i < numberOfJoinsStrings.length; i++) {
      numberOfJoins[i] = Integer.parseInt(numberOfJoinsStrings[i]);
    }

    String[] numberOfDataSourcesStrings = SplodgeBunch.getList(line.getOptionValue("d"));
    int[] numberOfDataSources = new int[numberOfDataSourcesStrings.length];
    for (int i = 0; i < numberOfDataSourcesStrings.length; i++) {
      numberOfDataSources[i] = Integer.parseInt(numberOfDataSourcesStrings[i]);
    }

    String[] minSelectivityStrings = SplodgeBunch.getList(line.getOptionValue("s"));
    String[] maxSelectivityStrings = line.hasOption('m')
            ? SplodgeBunch.getList(line.getOptionValue("m")) : new String[0];
    double[][] selectivities = new double[minSelectivityStrings.length][2];
    for (int i = 0; i < minSelectivityStrings.length; i++) {
      selectivities[i][0] = Double.parseDouble(minSelectivityStrings[i]);
      selectivities[i][1] = i < maxSelectivityStrings.length
              ? Double.parseDouble(maxSelectivityStrings[i]) : 1D;
    }

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
      for (JoinPattern joinPattern : joinPatterns) {
        for (int numberOfDataSource : numberOfDataSources) {
          for (int numberOfJoin : numberOfJoins) {
            for (double[] selectivity : selectivities) {
              splodge.generateQueries(joinPattern, numberOfJoin, numberOfDataSource, selectivity[0],
                      selectivity[1], timeout, limit);
            }
          }
        }
      }
    } finally {
      if (splodge != null) {
        splodge.close();
      }
    }
  }

  private static String[] getList(String stringList) {
    return stringList.substring(1, stringList.length() - 1).split(Pattern.quote(","));
  }

  private static Options createCommandLineOptions() {
    Option help = new Option("h", "help", false, "print this help message");
    help.setRequired(false);

    Option input = Option.builder("i").longOpt("input").hasArg().argName("fileOrDirctory")
            .desc("File or directory that contains the input graph. If not given, statistical information must be stored in outputDirectory")
            .required(false).build();

    Option work = Option.builder("w").longOpt("workDir").hasArg().argName("dirctory")
            .desc("Directory where the statistics will be stored.").required(false).build();

    Option output = Option.builder("o").longOpt("output").hasArg().argName("outputDirctory")
            .desc("Directory where the generated queries are stored.").required(true).build();

    Option joinPattern = Option.builder("j").longOpt("joinPattern").hasArg()
            .argName("[joinPattern,joinPattern,...]")
            .desc("The join pattern of the generated query." + " Possible values are: "
                    + Arrays.toString(JoinPattern.values()))
            .required(true).build();

    Option numberOfJoins = Option.builder("n").longOpt("numberOfJoins").hasArg()
            .argName("[int,int,...]").desc("The number of joins in the generated queries.")
            .required(true).build();

    Option dataSources = Option.builder("d").longOpt("dataSources").hasArg()
            .argName("[int,int,...]")
            .desc("The number of data sources like DBPedia or GeoNames that should be used by the query.")
            .required(true).build();

    Option selectivity = Option.builder("s").longOpt("selectivity").hasArg()
            .argName("[double,double,...]")
            .desc("The minimal number of triples that should be used to create the query response, e.g. selectivity is in interval [s,1].")
            .required(true).build();

    Option selectivityUp = Option.builder("m").longOpt("maxSelectivity").hasArg()
            .argName("[double,double,...]")
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
            "java " + SplodgeBunch.class.getName()
                    + " [-h] [-i <inputFileOrDirctory>] [-w <directory>] -o <outputDirectory> -j <joinPattern> -n <int> -d <int> -s <double> [-m <double>] [-r] [-t <timeout>] [-l <int>]",
            options);
  }

}
