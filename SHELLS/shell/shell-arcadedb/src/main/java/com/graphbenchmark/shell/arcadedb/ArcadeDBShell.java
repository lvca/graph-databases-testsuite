package com.graphbenchmark.shell.arcadedb;

import com.graphbenchmark.common.GdbLogger;
import com.graphbenchmark.common.GenericIndexMgm;
import com.graphbenchmark.common.GenericShell;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;

public class ArcadeDBShell extends GenericShell {
  static {
    shellClass = ArcadeDBShell.class;
  }

  final static         String      DB_URI   = "/data/thedb";
  private static final Boolean     lck      = false;
  private              ArcadeGraph database = null;

  private void initFactory() {
    if (database == null) {
      synchronized (lck) {
        if (database == null) {
          database = ArcadeGraph.open(DB_URI);
        }
      }
    }
  }

  @Override
  public GraphTraversalSource getConcurrentGTS() {
    initFactory();
    try {
      return database.traversal();
    } catch (Exception e) {
      e.printStackTrace();
      GdbLogger.getLogger().fatal("Error creating graph object");
    }
    return null;
  }

  @Override
  public GenericIndexMgm getIndexManager(Graph g) {
    return new IndexMgm(g, this);
  }

  @Override
  public Map<String, String> getQueryOverrideMap() {
    Map<String, String> h = new HashMap<>();
    h.put(com.graphbenchmark.queries.mgm.Load.class.getName(), Load.class.getName());
    h.put(com.graphbenchmark.queries.mgm.MapSample.class.getName(), MapSample.class.getName());
    h.put(com.graphbenchmark.queries.mgm.IndexUID.class.getName(), IndexUID.class.getName());
    return h;
  }
}
