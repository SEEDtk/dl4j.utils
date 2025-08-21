/**
 *
 */
package org.theseed.clusters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.methods.ClusterMergeMethod;

/**
 * @author Bruce Parrello
 *
 */
class TestClustering {

    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(TestClustering.class);


    @Test
    void testClusters() {
        Cluster cl1 = new Cluster("A");
        Cluster cl2 = new Cluster("B");
        Cluster cl3 = new Cluster("C");
        assertThat(cl1.size(), equalTo(1));
        assertThat(cl1.getMembers(), contains("A"));
        cl1.merge(cl2);
        assertThat(cl2.size(), equalTo(1));
        assertThat(cl1.size(), equalTo(2));
        assertThat(cl1.getMembers(), contains("A", "B"));
        assertThat(cl1.getHeight(), equalTo(2));
        cl3.merge(cl2);
        assertThat(cl3.size(), equalTo(2));
        assertThat(cl3.getMembers(), contains("B", "C"));
        assertThat(cl3.getHeight(), equalTo(2));
        cl1.merge(cl3);
        assertThat(cl1.size(), equalTo(3));
        assertThat(cl1.getMembers(), contains("A", "B", "C"));
        assertThat(cl1.getHeight(), equalTo(3));
        Cluster cl4 = new Cluster("D");
        Cluster cl5 = new Cluster("E");
        cl4.merge(cl5);
        assertThat(cl4.getMembers(), contains("D", "E"));
        assertThat(cl4.getHeight(), equalTo(2));
        cl4.merge(cl3);
        assertThat(cl4.getMembers(), contains("B", "C", "D", "E"));
        assertThat(cl4.getHeight(), equalTo(3));
    }

    @Test
    void testSimilarities() {
        Cluster clA = new Cluster("A");
        Cluster clB = new Cluster("B");
        Cluster clC = new Cluster("C");
        Cluster clD = new Cluster("D");
        Cluster clE = new Cluster("E");
        Similarity simAB = new Similarity(clA, clB, 0.5);
        // Create an invalid similarity to insure the pairing is unordered.
        Similarity simBA = new Similarity(clB, clA, 0.0);
        assertThat(simAB, equalTo(simBA));
        assertThat(simAB.hashCode(), equalTo(simBA.hashCode()));
        // Now we go back to the real sims.
        Similarity simAC = new Similarity(clA, clC, 0.6);
        Similarity simAD = new Similarity(clA, clD, 0.7);
        Similarity simAE = new Similarity(clA, clE, 0.8);
        clA.addSims(Arrays.asList(simBA, simAC, simAD, simAE));
        assertThat(clA.getScore("B"), equalTo(0.0));
        // Verify that the bad sim is overwritten by the good one.
        clA.addSim(simAB);
        assertThat(clA.getScore("B"), equalTo(0.5));
        // Get the backed collection.
        Collection<Similarity> clAsims = clA.getSims();
        assertThat(clAsims.size(), equalTo(4));
        // Check some of the other distances.
        assertThat(clA.getScore("F"), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(clA.getScore("C"), equalTo(0.6));
        clA.removeSim(clB);
        assertThat(clA.getScore("B"), equalTo(Double.NEGATIVE_INFINITY));
        // Insure the getSims() returned a collection backed by the map.
        assertThat(clAsims.size(), equalTo(3));
        for (Similarity sim : clAsims)
            assertThat(sim.getOtherId(clA), not(equalTo("B")));
    }

    @Test
    void testClusterGroups() throws IOException {
        log.info("Testing group algorithms.");
        File testFile = new File("data", "rnaseq.tbl");
        ClusterGroup group = ClusterGroup.load(testFile, ClusterMergeMethod.COMPLETE);
        assertThat(group.size(), equalTo(27));
        Cluster thrA = group.getCluster("thrA");
        assertThat(thrA, not(nullValue()));
        assertThat(thrA.getId(), equalTo("thrA"));
        assertThat(thrA.getMembers(), contains("thrA"));
        assertThat(thrA.getSims().size(), equalTo(26));
        assertThat(thrA.getScore("thrB"), closeTo(0.7685, 0.0001));
        // Get the clusters we are about to merge.
        Cluster yaaJ = group.getCluster("yaaJ");
        Cluster yaaW = group.getCluster("yaaW");
        // Create a map of the current similarities for yaaJ and yaaW.
        Map<String, Double> yaaJMap = new HashMap<>(30);
        yaaJ.getSims().stream().forEach(x -> yaaJMap.put(x.getOtherId(yaaJ), x.getScore()));
        Map<String, Double> yaaWMap = new HashMap<>(30);
        yaaW.getSims().stream().forEach(x -> yaaWMap.put(x.getOtherId(yaaW), x.getScore()));
        // Perform the merge.
        boolean merged = group.merge(0.64);
        assertThat(merged, equalTo(true));
        assertThat(yaaJ.size(), equalTo(2));
        // Verify that the similarity of the merged cluster to each other cluster is <= the
        // old similarities.
        for (Similarity sim : yaaJ.getSims()) {
            // Get the target cluster and the score.
            String otherId = sim.getOtherId(yaaJ);
            Cluster other = group.getCluster(otherId);
            double score = sim.getScore();
            // Verify that yaaJ is in the other cluster's sims but yaaW isn't.
            assertThat(other.getScore(yaaJ), equalTo(score));
            assertThat(other.getScore(yaaW), equalTo(Double.NEGATIVE_INFINITY));
            assertThat(other.getSims().size(), equalTo(25));
            // Verify the new score.
            assertThat(score, lessThanOrEqualTo(yaaJMap.get(otherId)));
            assertThat(score, lessThanOrEqualTo(yaaWMap.get(otherId)));
        }
        // Finish merging.
        int count = 0;
        while (group.merge(0.64))
            count++;
        log.info("{} merges performed.  {} groups created.", count, group.size());
        // Get the group list and verify the sort.
        List<Cluster> groupList = group.getClusters();
        Iterator<Cluster> iter = groupList.iterator();
        Cluster prev = iter.next();
        while (iter.hasNext()) {
            Cluster curr = iter.next();
            assertThat(prev.size(), greaterThanOrEqualTo(curr.size()));
            prev = curr;
        }
        // Now we need to verify the scores.  We reload the group to get the original
        // similarities back.
        ClusterGroup oldGroup = ClusterGroup.load(testFile, ClusterMergeMethod.SINGLE);
        // Process the scores in the new groups.
        for (Cluster cluster : groupList) {
            if (cluster.size() > 1) {
                log.info("Cluster {} (height = {}, score = {}) contains {}.", cluster,
                        cluster.getHeight(), cluster.getScore(),
                        StringUtils.join(cluster.getMembers(), ','));
            }
            // Verify that the similarities of all the members of this group are not less than
            // the minimum 0.64.
            List<String> members = new ArrayList<>(cluster.getMembers());
            for (int i = 0; i < members.size(); i++) {
                Cluster mem1 = oldGroup.getCluster(members.get(i));
                for (int j = i+1; j < members.size(); j++) {
                    Cluster mem2 = oldGroup.getCluster(members.get(j));
                    double score = mem1.getScore(mem2);
                    assertThat(mem1.toString() + "/" + mem2.toString(), score, greaterThanOrEqualTo(0.64));
                }
            }
        }
        // The next step is to try with AVERAGE and verify the scores again.
        group = ClusterGroup.load(testFile, ClusterMergeMethod.AVERAGE);
        count = 0;
        while (group.merge(0.64))
            count++;
        log.info("{} merges performed.  {} groups created.", count, group.size());
        this.testAverageClusters(group, oldGroup, 0.64);
        // Finally, do it with SINGLE.
        group = ClusterGroup.load(testFile, ClusterMergeMethod.SINGLE);
        count = 0;
        while (group.merge(0.64))
            count++;
        log.info("{} merges performed.  {} groups created.", count, group.size());
        // Get the group list.
        groupList = group.getClusters();
        // Process the scores in the new groups.
        for (Cluster cluster : groupList) {
            if (cluster.size() > 1) {
                log.info("Cluster {} (height = {}, score = {}) contains {}.", cluster,
                        cluster.getHeight(), cluster.getScore(),
                        StringUtils.join(cluster.getMembers(), ','));
            }
            // We want to compute the maximum similarity for this group.
            List<String> members = new ArrayList<>(cluster.getMembers());
            // Only proceed if there are at least two members; otherwise, there
            // is no score to measure.
            if (members.size() > 1) {
                double max = Double.NEGATIVE_INFINITY;
                for (int i = 0; i < members.size(); i++) {
                    Cluster mem1 = oldGroup.getCluster(members.get(i));
                    for (int j = i+1; j < members.size(); j++) {
                        Cluster mem2 = oldGroup.getCluster(members.get(j));
                        double score = mem1.getScore(mem2);
                        if (score > max) max = score;
                    }
                }
                assertThat(cluster.toString(), max, greaterThanOrEqualTo(0.64));
                assertThat(cluster.toString(), max, equalTo(cluster.getScore()));
            }
        }

    }

    /**
     * Verify that all the clusters have an average internal distance equal to the score.
     *
     * @param group		cluster group to test
     * @param oldGroup	cluster group containing all the similarities
     * @param limit		score limit for the clustering
     */
    protected void testAverageClusters(ClusterGroup group, ClusterGroup oldGroup, double limit) {
        // Get the group list.
        List<Cluster> groupList = group.getClusters();
        // Process the scores in the new groups.
        for (Cluster cluster : groupList) {
            if (cluster.size() > 1) {
                log.info("Cluster {} (height = {}, score = {}) contains {}.", cluster,
                        cluster.getHeight(), cluster.getScore(),
                        StringUtils.join(cluster.getMembers(), ','));
            }
            // Compute the average similarity for this group.
            List<String> members = new ArrayList<>(cluster.getMembers());
            double total = 0.0;
            int n = 0;
            for (int i = 0; i < members.size(); i++) {
                Cluster mem1 = oldGroup.getCluster(members.get(i));
                for (int j = i+1; j < members.size(); j++) {
                    Cluster mem2 = oldGroup.getCluster(members.get(j));
                    double score = mem1.getScore(mem2);
                    total += score;
                    n++;
                }
            }
            if (n > 0) {
                double average = total / n;
                assertThat(cluster.toString(), average, greaterThanOrEqualTo(limit));
                assertThat(cluster.toString(), average, closeTo(cluster.getScore(), 0.0001));
            }
        }
    }


    @Test
    public void testLimitedClustering() throws IOException {
        log.info("Testing limited groups.");
        File testFile = new File("data", "rnaseq.tbl");
        ClusterGroup group = ClusterGroup.load(testFile, ClusterMergeMethod.AVERAGE);
        assertThat(group.size(), equalTo(27));
        // Limit the clusters to size 3.
        group.setMaxSize(3);
        int count = 0;
        while (group.merge(0.64))
            count++;
        log.info("{} merges performed.  {} groups created.", count, group.size());
        // We need to verify all the clusters built.  Reload the group to get the old
        // scores back.
        ClusterGroup oldGroup = ClusterGroup.load(testFile, ClusterMergeMethod.SINGLE);
        this.testAverageClusters(group, oldGroup, 0.64);
        // Now verify all the clusters are 3 or less.
        for (Cluster cluster : group.getClusters())
            assertThat(cluster.getId(), cluster.size(), lessThanOrEqualTo(3));
    }

}
