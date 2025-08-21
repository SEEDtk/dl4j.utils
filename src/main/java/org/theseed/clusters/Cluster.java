/**
 *
 */
package org.theseed.clusters;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.theseed.reports.NaturalSort;

/**
 * A cluster represents a group of data points.  At any time, the cluster will contain an ID
 * (which is the ID of its primary element), a list of the member IDs, and a list of all
 * the similarities to the other extant clusters.
 *
 * Each data point starts in its own cluster.  The clustering algorithm groups them
 * together until each is sufficiently large that adding more data points dilutes the
 * cluster to a non-meaningful level.
 *
 * Clusters are sorted by size (largest first), then score (highest first), and then by
 * the natural ordering of the names.
 *
 * @author Bruce Parrello
 *
 */
public class Cluster implements Comparable<Cluster> {

    // FIELDS
    /** cluster ID */
    private final String id;
    /** set of member IDs */
    private final Set<String> members;
    /** map of other cluster IDs to similarities */
    private final Map<String, Similarity> simMap;
    /** height of this cluster */
    private int height;
    /** score of this cluster */
    private double score;
    /** sorter for cluster IDs */
    private static final Comparator<String> NATURAL_SORT = new NaturalSort();

    /**
     * Construct a singleton cluster.
     *
     * @param dataId	ID of this cluster's sole data point
     */
    public Cluster(String dataId) {
        this.id = dataId;
        this.members = new TreeSet<>(NATURAL_SORT);
        this.members.add(dataId);
        this.simMap = new TreeMap<>();
        this.height = 1;
        this.score = Double.POSITIVE_INFINITY;
    }

    /**
     * Add similarities to this cluster's similarity list.
     *
     * @param sims	collection of similarities to add
     */
    public void addSims(Collection<Similarity> sims) {
        for (Similarity sim : sims)
            this.addSim(sim);
    }

    /**
     * Add a single similarity to this cluster's similarity list.
     *
     * @param sim	similarity to add
     */
    public void addSim(Similarity sim) {
        this.simMap.put(sim.getOtherId(this), sim);
    }

    /**
     * Remove the similarity for a specified other cluster.
     *
     * @param clB	ID of other cluster whose similarity is to be removed
     */
    public void removeSim(Cluster clB) {
        this.simMap.remove(clB.getId());
    }

    /**
     * Get the similarity score for a specified other cluster (by ID).
     *
     * @param otherId	ID of other cluster whose similarity is desired
     *
     * @return the similarity score, or -INFINITY if the record is not found
     */
    public double getScore(String otherId) {
        Similarity sim = this.simMap.get(otherId);
        double retVal = Double.NEGATIVE_INFINITY;
        if (sim != null)
            retVal = sim.getScore();
        return retVal;
    }

    /**
     * Get the similarity score for a specified other cluster.
     *
     * @param other		other cluster whose similarity is desired
     *
     * @return the similarity score, or -INFINITY if the record is not found
     */
    public double getScore(Cluster other) {
        return this.getScore(other.getId());
    }

    /**
     * @return the collection of similarities in this object
     */
    public Collection<Similarity> getSims() {
        return this.simMap.values();
    }

    /**
     * @return all the members of this cluster
     */
    public Set<String> getMembers() {
        return this.members;
    }

    /**
     * Merge another cluster's membership with this one.
     *
     * @param otherCl	other cluster to merge
     * @param score		score associated with the merge
     */
    protected void merge(Cluster otherCl) {
        this.members.addAll(otherCl.members);
        this.height = Math.max(this.height, otherCl.height) + 1;
    }

    /**
     * @return the ID of this cluster
     */
    public String getId() {
        return this.id;
    }

    /**
     * @return the number of members in this cluster
     */
    public int size() {
        return this.members.size();
    }

    @Override
    public int hashCode() {
        int result = ((this.id == null) ? 0 : this.id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Cluster))
            return false;
        Cluster other = (Cluster) obj;
        if (this.id == null) {
            if (other.id != null)
                return false;
        } else if (!this.id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public int compareTo(Cluster o) {
        int retVal = o.size() - this.size();
        if (retVal == 0) {
            retVal = Double.compare(o.score, this.score);
            if (retVal == 0)
                retVal = NATURAL_SORT.compare(this.id, o.id);
        }
        return retVal;
    }

    @Override
    public String toString() {
        return this.id;
    }

    /**
     * @return the height of this cluster
     */
    public int getHeight() {
        return this.height;
    }

    /**
     * @return the score for this cluster
     */
    public double getScore() {
        return score;
    }

    /**
     * Update the score for this cluster.
     *
     * @param newScore		proposed new score
     */
    protected void setScore(double newScore) {
        this.score = newScore;
    }


}
