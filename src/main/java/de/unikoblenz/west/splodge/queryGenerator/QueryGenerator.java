package de.unikoblenz.west.splodge.queryGenerator;

import de.unikoblenz.west.splodge.JoinPattern;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

/**
 * Used to generate the SPARQL query.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryGenerator implements Closeable {

  private final File queryDir;

  private final Writer selectivityOutput;

  public QueryGenerator(File outputDir) {
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    queryDir = new File(outputDir.getAbsolutePath() + File.separator + "queries");
    if (!queryDir.exists()) {
      queryDir.mkdirs();
    }
    try {
      selectivityOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
              outputDir.getAbsolutePath() + File.separator + "querySelectivities.txt", true),
              "UTF-8"));
    } catch (UnsupportedEncodingException | FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public void createQuery(String queryFileName, double selectivity, JoinPattern joinPattern,
          int limit, String... predicates) {
    try (Writer queryWriter = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(queryDir.getAbsolutePath() + File.separator + queryFileName),
            "UTF-8"));) {
      selectivityOutput.write(queryFileName + "\t" + selectivity + "\n");

      int varId = 0;
      StringBuilder sb = new StringBuilder();
      for (String predicate : predicates) {
        if (joinPattern == JoinPattern.SUBJECT_OBJECT_JOIN) {
          sb.append("    ?v").append(varId).append(" ").append(predicate).append(" ?v")
                  .append(++varId).append(".\n");
        } else if (joinPattern == JoinPattern.SUBJECT_SUBJECT_JOIN) {
          sb.append("    ?v0 ").append(predicate).append(" ?v").append(++varId).append(".\n");
        } else {
          throw new IllegalArgumentException(
                  "The join pattern " + joinPattern + " is currently not supported.");
        }
      }

      queryWriter.write("SELECT ?v0 ?v" + varId + " WHERE {\n");
      queryWriter.write(sb.toString());
      queryWriter.write("}");
      if (limit > 0) {
        queryWriter.write(" LIMIT " + limit);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      selectivityOutput.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
