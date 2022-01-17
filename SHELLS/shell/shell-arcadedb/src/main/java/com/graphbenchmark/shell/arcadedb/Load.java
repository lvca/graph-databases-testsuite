package com.graphbenchmark.shell.arcadedb;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.graphbenchmark.common.BaseQParam;
import com.graphbenchmark.common.GdbLogger;
import com.graphbenchmark.common.GenericIndexMgm;
import com.graphbenchmark.common.GenericShell;
import com.graphbenchmark.common.TimingCheckpoint;
import com.graphbenchmark.common.samples.Sample;
import com.graphbenchmark.common.schema.v2.Schema;
import com.graphbenchmark.settings.Dataset;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.javatuples.Pair;

import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.stream.*;

// Build schema before loading (otherwise will not be able to index)
@SuppressWarnings("unused")
public class Load extends com.graphbenchmark.queries.mgm.Load {
  private GdbLogger log = GdbLogger.getLogger();

  public static void createProperty(ArcadeGraph og, String label, String property, Class type, boolean isEdge) {
    Database db = og.getDatabase();
    com.arcadedb.schema.Schema schema = db.getSchema();
    VertexType cls = schema.getOrCreateVertexType(label);
    cls.createProperty(property, dbTypeMap(type));
  }

  @Override
  public Collection<TimingCheckpoint> query(GraphTraversalSource gts, BaseQParam params, Sample sample, Dataset ds, final long thread_id, GenericIndexMgm imgm,
      GenericShell shell) {
    return super.query(gts, params, sample, ds, thread_id, imgm, shell);
  }

  @Override
  public Collection<TimingCheckpoint> createSchema(GraphTraversalSource gts, Dataset ds, GenericShell shell) {
    ArcadeGraph og = (ArcadeGraph) gts.getGraph();
    List<TimingCheckpoint> tc = new ArrayList<>();

    // Disable minicluster
    og.getDatabase().getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 1);

    // Infer/Load schema
    long start = System.currentTimeMillis();
    Pair<Stream<String>, Stream<String>> sl = (new Schema(ds.path)).getLabelStreams();
    tc.add(new TimingCheckpoint("Schema", System.currentTimeMillis() - start, "infer", (Object) ds.path));

    // Create schema
    start = System.currentTimeMillis();
    sl.getValue0().forEach(og.getDatabase().getSchema()::createVertexType);
    sl.getValue0().close();
    tc.add(new TimingCheckpoint("Schema", System.currentTimeMillis() - start, "-vertex labels", (Object) ds.path));

    sl.getValue1().forEach(og.getDatabase().getSchema()::createEdgeType);
    sl.getValue1().close();
    tc.add(new TimingCheckpoint("Schema", System.currentTimeMillis() - start, "-edges labels", (Object) ds.path));

    // Property declaration happens only for sampled nodes/edges in MapSample

    tc.add(new TimingCheckpoint("Schema", System.currentTimeMillis() - start, "-commit", (Object) ds.path));

    return tc;
  }

  private static Type dbTypeMap(final Class c) {
    // See: SchemaInference.inferNodeProperty
    if (Date.class.equals(c))
      return Type.DATE;
    else if (Double.class.equals(c))
      return Type.DOUBLE;
    else if (Float.class.equals(c))
      return Type.FLOAT;
    else if (Integer.class.equals(c))
      return Type.INTEGER;
    else if (Long.class.equals(c))
      return Type.LONG;
    else if (Timestamp.class.equals(c))
      GdbLogger.getLogger().warning("Unsupported type g:Timestamp, treating as STRING");
    else if (UUID.class.equals(c))
      GdbLogger.getLogger().warning("Unsupported type g:UUID, treating as STRING");
    return Type.STRING;
  }
}
