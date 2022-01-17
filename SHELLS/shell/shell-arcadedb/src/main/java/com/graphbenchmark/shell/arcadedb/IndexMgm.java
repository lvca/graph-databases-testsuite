package com.graphbenchmark.shell.arcadedb;

import com.graphbenchmark.common.GenericIndexMgm;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class IndexMgm extends GenericIndexMgm {
  private ArcadeDBShell shell;

  public IndexMgm(Graph g, ArcadeDBShell shell) {
    super(g);
    this.shell = shell;
  }

  // https://docs.arcadedb.com/#SQL-Create-Index

  @Override
  public void node(String label, String prop_name) {
    ArcadeGraph noTxOg = (ArcadeGraph) this.g;

    // "CREATE INDEX addresses ON Employee (address) NOTUNIQUE METADATA {ignoreNullValues: true}";
    // NOTUNIQUE = SB-Tree Algorithm; these indexes allow duplicate keys.
    String query = "CREATE INDEX %3$s ON `%1$s` (`%2$s`) NOTUNIQUE NULL_STRATEGY SKIP";

    String index_name = String.format("%1$s_%2$s_index", label, prop_name).replaceAll("[^A-Za-z0-9]", "_");

    // NOTE: the property must exist in the schema. We cannot create it here as we are missing the type.

    noTxOg.sql(String.format(query, label, prop_name, index_name));
  }
}
