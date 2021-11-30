/**
 *
 */
package org.theseed.clusters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.methods.ClusterMergeMethod;
import org.theseed.io.TabbedLineReader;

/**
 * This object manages a clustering group.  The clustering group contains a map of the
 * extant clusters and a queue of the similarities between all existing clusters.
 *
 * The clustering group is built by adding the similarity scores of object pairs.  The
 * objects are specified by ID only, so there is no complication with interfaces and
 * subclasses.  Each object is put in a cluster by itself and the similarities stored
 * in the appropriate places.
 *
 * The primary operation is merging the two closest clusters, which requires updating
 * all the appropriate similarities.  The constructor of this object must specify
 * the update method to use.
 *
 * @author Bruce Parrello
 *
 */
public class ClusterGroup {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(ClusterGroup.class);
    /** similarity queue */
    private NavigableSet<Similarity> simQueue;
    /** map of clusters */
    private Map<String, Cluster> clusterMap;
    /** method for computing merged similarities */
    private ClusterMergeMethod method;
    /** maximum permissible cluster size */
    private int maxSize;

    /**
     * Create a new cluster group.
     *
     * @param size		estimated number of data points expected
     * @param method	method for similarity merging
     */
    public ClusterGroup(int size, ClusterMergeMethod method) {
        this.method = method;
        this.simQueue = new TreeSet<Similarity>();
        // Note we give the hash map extra capacity to avoid clashes.
        this.clusterMap = new HashMap<String, Cluster>((size + 1) * 4 / 3);
        // Default to the maximum possible group size for the size limit.
        this.maxSize = Integer.MAX_VALUE;
    }

    /**
     * Load this group from the specified tab-delimited file.
     *
     * @param inFile	input file to read
     * @param col1		index (1-based) or name of column containing first data point ID
     * @param col2		index (1-based) or name of column containing second data point ID
     * @param score		index (1-based) or name of column containing score
     * @param sparse	if TRUE, the load is presumed to be sparse
     *
     * @throws IOException
     */
    public void load(File inFile, String col1, String col2, String score, boolean sparse) throws IOException {
        try (TabbedLineReader inStream = new TabbedLineReader(inFile)) {
            // We find the three columns of interest, and then load each line of the file
            // into the group as a similarity.
            int col1Idx = inStream.findField(col1);
            int col2Idx = inStream.findField(col2);
            int scoreIdx = inStream.findField(score);
            log.info("Reading cluster group from {}.", inFile);
            this.readSims(inStream, col1Idx, col2Idx, scoreIdx, sparse);
        }
    }

    /**
     * Read the similarities from the specified stream.
     *
     * @param inStream		input stream
     * @param col1Idx		index of column containing the first data point ID
     * @param col2Idx		index of column containing the second data point ID
     * @param scoreIdx		index of column containing the score
     * @param sparse		if TRUE, the load is presumed to be sparse
     */
    private void readSims(TabbedLineReader inStream, int col1Idx, int col2Idx, int scoreIdx, boolean sparse) {
        // This will count the number of lines read.
        int count = 0;
        for (TabbedLineReader.Line line : inStream) {
            // We need to insure non-finite similarities get turned into -infinity.
            double score = line.getDouble(scoreIdx);
            if (! Double.isFinite(score)) score = Double.NEGATIVE_INFINITY;
            // Add the similarity.
            this.addSim(line.get(col1Idx), line.get(col2Idx), score);
            // Indicate our progress.
            count++;
            if (count % 20000 == 0)
                log.info("{} records processed.", count);
        }
        int points = this.clusterMap.size();
        log.info("{} similarities read for {} data points.", count, points);
        int simCount = this.simQueue.size();
        if (simCount < count)
            log.warn("WARINING: {} redundant similarities read.", count - simCount);
        // If the dataset is expected to be complete, check that here.
        if (! sparse) {
            int expected = points * (points - 1) / 2;
            if (expected < simCount)
                log.warn("WARNING:  Expected {} similarities, but only found {}.", expected, simCount);
        }
    }

    /**
     * Create a group from a tab-delimited file.  The file must have headers, and the data
     * point IDs must be in the first two columns with the score in the third.
     *
     * @param inFile	input file to read
     * @param method	method to use for merging
     *
     * @return a cluster group initialized from the file
     *
     * @throws IOException
     */
    public static ClusterGroup load(File inFile, ClusterMergeMethod method) throws IOException {
        // Estimate the group size from the file size.  We constrain it to reasonable values;
        int size = estimateDataPoints(inFile);
        // Now build the cluster group.
        ClusterGroup retVal = new ClusterGroup(size, method);
        // Read from the file.
        log.info("Reading cluster group of type {} from {}.  {} data points estimated.",
                method, inFile, size);
        try (TabbedLineReader inStream = new TabbedLineReader(inFile)) {
            retVal.readSims(inStream, 0, 1, 2, false);
        }
        return retVal;
    }

    /**
     * Estimate the number of data points represented in a file.
     *
     * @param inFile	input file to test
     *
     * @return an estimated number of data points for the file, constrained to reasonable limits
     */
    public static int estimateDataPoints(File inFile) {
        long records = inFile.length() / 40;
        if (records <= 0)
            records = 100;
        else if (records > 100000)
            records = 100000;
        int retVal = ((int) Math.sqrt(records)) * 2 + 1;
        return retVal;
    }

    /**
     * Add a new similarity observation.
     *
     * @param id1		ID of the first data point
     * @param id2		ID of the second data point
     * @param score		similarity between the two data points
     */
    public void addSim(String id1, String id2, double score) {
        Cluster cl1 = this.clusterMap.computeIfAbsent(id1, k -> new Cluster(k));
        Cluster cl2 = this.clusterMap.computeIfAbsent(id2, k -> new Cluster(k));
        Similarity sim = new Similarity(cl1, cl2, score);
        cl1.addSim(sim);
        cl2.addSim(sim);
        this.simQueue.add(sim);
    }

    /**
     * @return the number of clusters
     */
    public int size() {
        return this.clusterMap.size();
    }

    /**
     * @return the sorted list of clusters
     */
    public List<Cluster> getClusters() {
        List<Cluster> retVal = new ArrayList<Cluster>(this.clusterMap.values());
        Collections.sort(retVal);
        return retVal;
    }

    /**
     * Merge the two closest clusters if the similarity is above a specified minimum.
     *
     * @param minSim		minimum permissible similarity score for a merge
     *
     * @return TRUE if successful, FALSE if there was nothing to merge
     */
    public boolean merge(double minSim) {
        boolean retVal = false;
        boolean done = false;
        while (! done && this.simQueue.size() >= 1) {
            // Get the closest similarity.  Note we remove it from the queue.  If we can't use
            // it now, we will not be able to unless it changes and is re-added.
            Similarity closest = this.simQueue.pollFirst();
            double simAB = closest.getScore();
            if (simAB < minSim) {
                // We have run out of permissible similarities.
                done = true;
            } else {
                // Now we need to check the clusters.  We only proceed if combining them
                // does not push us past the maximum size.
                Cluster clA = this.clusterMap.get(closest.getId1());
                Cluster clB = this.clusterMap.get(closest.getId2());
                if (clA.size() + clB.size() <= this.maxSize) {
                    // Here we are done looping, and we have a merge we can do.
                    retVal = true;
                    done = true;
                    log.debug("Merging {} and {} with similarity {}.", clA, clB, simAB);
                    // Remove all the similarities in A and B from the queue.
                    // The A similarities will be added back with new scores.
                    this.simQueue.removeAll(clB.getSims());
                    Collection<Similarity> clAsims = clA.getSims();
                    this.simQueue.removeAll(clAsims);
                    // Remove the AB similarity from A's sim list.  B is being merged into A.
                    // Note that clAsims is backed by the sim list in A, so it will update, too.
                    clA.removeSim(clB);
                    // Compute the score of the merged cluster.
                    double newScore = this.method.mergedScore(clA.getScore(), clB.getScore(), simAB,
                            clA.size(), clB.size());
                    // Now loop through clAsims, updating the scores.
                    clAsims.parallelStream().forEach(x ->
                            x.updateScore(this.method, simAB, clA, clB,
                                    this.clusterMap.get(x.getOtherId(clA))));
                    // Now remove B's scores from all the other clusters.
                    clB.getSims().parallelStream().map(x -> this.clusterMap.get(x.getOtherId(clB)))
                            .forEach(x -> x.removeSim(clB));
                    // Can we make this cluster any bigger?
                    if (clA.size() < this.maxSize) {
                        // Yes. Add the updated scores back to the queue.  Note that B's scores
                        // are never added back, as they are no longer relevant.
                        this.simQueue.addAll(clAsims);
                    }
                    // Finally, merge cluster B into cluster A and remove B from the map.
                    // We have to do all of this last, since the score updates rely on the
                    // old sizes.
                    clA.merge(clB);
                    this.clusterMap.remove(clB.getId());
                    clA.setScore(newScore);
                }
            }
        }
        return retVal;
    }

    /**
     * @return the cluster with specified ID, or NULL if no such cluster exists
     *
     * @param clID		ID of the desired cluster
     */
    public Cluster getCluster(String clID) {
        return this.clusterMap.get(clID);
    }

    /**
     * @return the maximum permissible group size
     */
    public int getMaxSize() {
        return this.maxSize;
    }

    /**
     * Specify a new maximum group size.
     *
     * @param maxSize 	the new maximum size
     */
    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

}
