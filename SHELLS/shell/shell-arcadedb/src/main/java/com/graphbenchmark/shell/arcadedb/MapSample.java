package com.graphbenchmark.shell.arcadedb;

import com.graphbenchmark.common.ExperimentSettings;
import com.graphbenchmark.common.GdbLogger;
import com.graphbenchmark.common.GenericIndexMgm;
import com.graphbenchmark.common.GenericShell;
import com.graphbenchmark.common.TimingCheckpoint;
import com.graphbenchmark.common.samples.Sample;
import com.graphbenchmark.common.schema.QSupportSchema;
import com.graphbenchmark.settings.Dataset;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.*;

public class MapSample extends com.graphbenchmark.queries.mgm.MapSample {

  @Override
  protected Collection<TimingCheckpoint> query(GraphTraversalSource gts, ExperimentSettings params, Sample sample, Dataset ds, long thread_id,
      GenericIndexMgm imgm, GenericShell shell) {
    return super.query(gts, params, sample, ds, thread_id, imgm, shell);
  }

  @Override
  public Collection<TimingCheckpoint> qSupportPatch(GraphTraversalSource gts, Sample sample, Dataset ds, GenericShell shell) {
    Collection<TimingCheckpoint> ts = new ArrayList<>();

    ArcadeGraph og = (ArcadeGraph) gts.getGraph();

    // Patch schema
    long start = System.currentTimeMillis();

    // Sampled properties (allow index creation)
    sample.raw.node_props.forEach(np -> {
      try {
        GdbLogger.getLogger().debug("Creating node %s", np);
        Load.createProperty(og, np.label, np.name, Class.forName(np.type), false);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (Exception e) {
        //} catch (com.orientechnologies.orient.core.exception.OSchemaException e) {
        // The database contains some schema-less data in the property '...' that is not compatible with the type ... .
        GdbLogger.getLogger().debug("Type consistency issue for property %s, creating as Object.", np);
        Load.createProperty(og, np.label, np.name, Object.class, false);
      }
    });

    sample.raw.edge_props.forEach(np -> {
      try {
        GdbLogger.getLogger().debug("Creating edge %s", np);
        Load.createProperty(og, np.label, np.name, Class.forName(np.type), true);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        //} catch (com.orientechnologies.orient.core.exception.OSchemaException e) {
      } catch (Exception e) {
        // The database contains some schema-less data in the property '...' that is not compatible with the type ... .
        GdbLogger.getLogger().debug("Type consistency issue for property %s, creating as Object.", np);
        Load.createProperty(og, np.label, np.name, Object.class, false);
      }
    });
    ts.add(new TimingCheckpoint("Schema", System.currentTimeMillis() - start, "-sampled props labels", (Object) ds.path));

    // Framework fixture data
    start = System.currentTimeMillis();
    og.getDatabase().getSchema().createVertexType(QSupportSchema.VLabel);
    og.getDatabase().getSchema().createEdgeType(QSupportSchema.ELabel);

    gts.V(sample.mapping.nodes).label().dedup().toList().forEach(lbl -> {
      // 'New property' support
      Load.createProperty(og, lbl, QSupportSchema.VProperty, QSupportSchema.VPropertyType, false);
      // 'common property' support
      Load.createProperty(og, lbl, QSupportSchema.VCommonProperty, String.class, false);
    });

    gts.E(sample.mapping.edges).label().dedup().toList().forEach(lbl -> {
      // 'New property' support
      Load.createProperty(og, lbl, QSupportSchema.EProperty, QSupportSchema.EPropertyType, true);
      // 'common property' support
      Load.createProperty(og, lbl, QSupportSchema.ECommonProperty, String.class, true);
    });
    ts.add(new TimingCheckpoint("qSupportPatch", System.currentTimeMillis() - start, "-commit", (Object) ds.path));

    // Poison data
    ts.addAll(super.qSupportPatch(gts, sample, ds, shell));
    return ts;
  }
}
