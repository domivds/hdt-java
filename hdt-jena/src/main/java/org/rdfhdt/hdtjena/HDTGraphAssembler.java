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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

public class HDTGraphAssembler extends AssemblerBase implements Assembler {
  private static final Logger log = LoggerFactory.getLogger(HDTGraphAssembler.class);

  private static boolean initialized;

  static {
    System.err.println("HDTGraphAssembler patch");
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
      System.err.println("HDT pointing to file " + file);
      return ModelFactory.createModelForGraph(createHdtGraph(root, filePath, loadInMemory));
    }
    if (file.isDirectory()) {
      log.info("HDT pointing to folder {}", file);
      System.err.println("HDT pointing to folder " + file);
      return createFromFolder(root, file, loadInMemory);
    }
    throw new AssemblerException(root, "Filename doesn't point to file or folder: " + file);
  }


  private HDTGraph createHdtGraph(Resource root, String file, boolean loadInMemory) {
    log.info("Creating HDT from file {}", file);
    try {
      // FIXME: Read more properties. Cache config?
      HDT hdt = loadInMemory ? HDTManager.loadIndexedHDT(file, null) : HDTManager.mapIndexedHDT(file, null);
      HDTGraph hdtGraph = new HDTGraph(hdt);
      System.err.println("Created - " + hdtGraph.hashCode());
      return hdtGraph;
    }
    catch (IOException e) {
      log.error("Error reading HDT file: " + file, e);
      throw new AssemblerException(root, "Error reading HDT file: " + file, e);
    }
  }

  private Model createFromFolder(final Resource root, final File folder, final boolean loadInMemory) {
    log.info("Creating HDT from folder {}", folder);
    File file = getCurrentHdtFile(root, folder);

    HDTGraph startingGraph = createHdtGraph(root, file.getAbsolutePath(), loadInMemory);

    final GraphInvocationHandler invocationHandler = new GraphInvocationHandler(startingGraph, file);


    final AtomicReference<File> currentFile = new AtomicReference<File>(file);

    Thread thread = new Thread("HDTupdater") {
      @Override
      public void run() {
        while (true) {
          try {
            File file = getCurrentHdtFile(root, folder);
            if (!currentFile.get().equals(file)) {
              System.err.println("HDT trying to change from " + currentFile.get() + " to " + file);
              log.info("HDT trying to change from {} to {}", currentFile.get(), file);
              Graph oldGraph = invocationHandler.graph;
              invocationHandler.graph = createHdtGraph(root, file.getAbsolutePath(), loadInMemory);
              currentFile.set(file);
              try {
                System.err.println("Closing old: " + oldGraph.hashCode());
                oldGraph.close();
              }
              catch (Exception ignore) {
              }
              log.info("HDT changed to {}", file);
              System.err.println("HDT changed to " + file);
            }

          }
          catch (Exception e) {
            log.error("Failed to update HDT file", e);
          }

          try {
            Thread.sleep(60000L);
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
      String current = FileUtils.readFileToString(new File(folder, "current.txt"), StandardCharsets.UTF_8).trim();
      File currentFile = new File(folder, current);
      if (!currentFile.isFile()) throw new AssemblerException(root, "Current HDT file not found " + currentFile);
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

    public GraphInvocationHandler(Graph graph, File currentFile) {
      this.graph = graph;
      this.currentFile = currentFile;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      System.err.println(method + " on " + graph.hashCode() + " [" + graph.isClosed() + "]");
      return method.invoke(graph, args);
    }
  }


}
