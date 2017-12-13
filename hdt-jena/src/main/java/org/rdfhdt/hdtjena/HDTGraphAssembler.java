/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-jena/src/org/rdfhdt/hdtjena/HDTGraphAssembler.java $
 * Revision: $Rev: 190 $
 * Last modified: $Date: 2013-03-03 11:30:03 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 */

package org.rdfhdt.hdtjena;

import org.apache.commons.io.FileUtils;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

public class HDTGraphAssembler extends AssemblerBase implements Assembler {
  private static final Logger log = LoggerFactory.getLogger(HDTGraphAssembler.class);

  private static boolean initialized;

  static {
    init();
  }

  public static void init() {
    if (initialized) {
      return;
    }
    initialized = true;
    Assembler.general.implementWith(HDTJenaConstants.tGraphHDT, new HDTGraphAssembler());
  }

  @Override
  public Model open(Assembler a, Resource root, Mode mode) {
    String filePath = GraphUtils.getStringValue(root, HDTJenaConstants.pFileName);
    boolean loadInMemory = Boolean.parseBoolean(GraphUtils.getStringValue(root, HDTJenaConstants.pKeepInMemory));

    File file = new File(filePath);
    if (file.isFile()) {
      log.info("HDT pointing to file {}", file);
      return ModelFactory.createModelForGraph(createHdtGraph(root, filePath, loadInMemory));
    }
    if (file.isDirectory()) {
      log.info("HDT pointing to folder {}", file);
      return createFromFolder(root, file, loadInMemory);
    }
    throw new AssemblerException(root, "Filename doesn't point to file or folder: " + file);
  }


  private HDTGraph createHdtGraph(Resource root, String file, boolean loadInMemory) {
    log.info("Creating HDT from file {}", file);
    try {
      // FIXME: Read more properties. Cache config?
      HDT hdt = loadInMemory ? HDTManager.loadIndexedHDT(file, null) : HDTManager.mapIndexedHDT(file, null);
      return new HDTGraph(hdt);
    }
    catch (IOException e) {
      log.error("Error reading HDT file: " + file, e);
      throw new AssemblerException(root, "Error reading HDT file: " + file, e);
    }
  }

  private Model createFromFolder(Resource root, File folder, boolean loadInMemory) {
    log.info("Creating HDT from folder {}", folder);

    GraphInvocationHandler invocationHandler = new GraphInvocationHandler();

    Thread thread = new Thread("HDTupdater") {
      @Override
      public void run() {
        Graph toCloseGraph = null;
        log.info("HDT check thread started");
        //noinspection InfiniteLoopStatement
        while (true) {
          if (null != toCloseGraph) {
            try {
              log.info("Going to close an old graph");
              toCloseGraph.close();
            }
            catch (Exception ignore) {
            }
            toCloseGraph = null;
          }
          try {
            File file = getCurrentHdtFile(root, folder);

            if (null != file && !file.equals(invocationHandler.currentFile)) {
              log.info("HDT trying to change from {} to {}", invocationHandler.currentFile, file);
              Graph oldGraph = invocationHandler.graph;
              invocationHandler.graph = createHdtGraph(root, file.getAbsolutePath(), loadInMemory);
              invocationHandler.currentFile = file;

              toCloseGraph = oldGraph; //we use intermediate oldGraph so we don't close the current graph if creating a new one fails

              log.info("HDT changed to {}", file);
            }

          }
          catch (Throwable e) {
            log.error("Failed to update HDT file", e);
          }
          try {
            Thread.sleep(TimeUnit.MINUTES.toMillis(5)); //5 minutes so we can also handle the graph close in the same thread
          }
          catch (InterruptedException ignore) {
          }
        }
      }
    };

    thread.setDaemon(true);
    thread.start();

    return ModelFactory.createModelForGraph(createWrapper(invocationHandler));

  }


  private File getCurrentHdtFile(Resource root, File folder) {
    try {
      String current = FileUtils.readFileToString(new File(folder, "current.txt")).trim(); //deprecated in commons-io 2.5, but fuseki 3.4.0 includes commons-io 2.2 !!!
      File currentFile = new File(folder, current);
      if (!currentFile.isFile()) {
        log.error("Current HDT file not found " + currentFile);
        return null;
      }
      return currentFile;
    }
    catch (IOException e) {
      throw new AssemblerException(root, "Failed to load current.txt file in " + folder, e);
    }
  }


  private Graph createWrapper(GraphInvocationHandler invocationHandler) {
    return (Graph) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Graph.class}, invocationHandler);
  }

  private static class GraphInvocationHandler implements InvocationHandler {
    private volatile Graph graph;
    private volatile File currentFile;

    public GraphInvocationHandler() {
      this.graph = GraphFactory.createGraphMem();
      this.currentFile = null;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return method.invoke(graph, args);
    }
  }


}
