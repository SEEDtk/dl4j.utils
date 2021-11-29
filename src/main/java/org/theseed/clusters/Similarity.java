/**
 *
 */
package org.theseed.clusters;

import org.theseed.clusters.methods.ClusterMergeMethod;

/**
 * A similarity contains a score indicating the similarity between two clusters.  It
 * also contains the IDs of the two clusters being compared.  Two similarities with
 * swapped IDs are considered equal (the IDs are an unordered pair).  In addition,
 * the similarities sort with the highest similarity score first.
 *
 * In a similarity, the first cluster's ID must always be lexically less than the second
 * cluster's ID.  When we merge clusters A and B, we insist that the ID of A be lexically
 * before the ID of B.  The similarities for A are recomputed.  The scores will change,
 * but the lexical ordering of the cluster IDs will remain the same, since the new cluster
 * keeps the name A.
 *
 * @author Bruce Parrello
 *
 */
public class Similarity implements Comparable<Similarity> {

    // FIELDS
    /** first cluster ID */
    private String cluster1;
    /** second cluster ID */
    private String cluster2;
    /** similarity score */
    private double score;

    /**
     * Create a similarity.
     *
     * @param cl1		first cluster
     * @param cl2		second cluster
     * @param score		similarity score
     */
    public Similarity(Cluster cl1, Cluster cl2, double score) {
        if (cl1.getId().compareTo(cl2.getId()) < 0) {
            this.cluster1 = cl1.getId();
            this.cluster2 = cl2.getId();
        } else {
            this.cluster1 = cl2.getId();
            this.cluster2 = cl1.getId();
        }
        this.score = score;
    }

    /**
     * @return the ID of the cluster not indicated
     *
     * @param curr	cluster whose ID is not wanted
     */
    public String getOtherId(Cluster curr) {
        String retVal = this.cluster1;
        if (curr.getId().contentEquals(this.cluster1))
            retVal = this.cluster2;
        return retVal;
    }

    /**
     * @return the score for this similarity
     */
    public double getScore() {
        return this.score;
    }

    /**
     * @return the ID of the first cluster
     */
    public String getId1() {
        return this.cluster1;
    }

    /**
     * @return the ID of the second cluster
     */
    public String getId2() {
        return this.cluster2;
    }

    @Override
    public int compareTo(Similarity o) {
        int retVal = Double.compare(o.score, this.score);
        if (retVal == 0) {
            retVal = this.cluster1.compareTo(o.cluster1);
            if (retVal == 0)
                retVal = this.cluster2.compareTo(o.cluster2);
        }
        return retVal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.cluster1 == null) ? 0 : this.cluster1.hashCode());
        result = prime * result + ((this.cluster2 == null) ? 0 : this.cluster2.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Similarity))
            return false;
        Similarity other = (Similarity) obj;
        if (this.cluster1 == null) {
            if (other.cluster1 != null)
                return false;
        } else if (!this.cluster1.equals(other.cluster1))
            return false;
        if (this.cluster2 == null) {
            if (other.cluster2 != null)
                return false;
        } else if (!this.cluster2.equals(other.cluster2))
            return false;
        return true;
    }

    /**
     * This method updates the score in this similarity.  The score currently
     * indicates the distance from A to X, but we want it to show the distance
     * from the merged cluster AB to X.
     *
     * @param method	update method for similarity scores
     * @param simAB		similarity between A and B
     * @param clA		cluster A
     * @param clB		cluster B
     * @param clX		cluster X
     */
    protected void updateScore(ClusterMergeMethod method, double simAB,
            Cluster clA, Cluster clB, Cluster clX) {
        this.score = method.mergedSim(simAB, clA.getScore(clX), clB.getScore(clX),
                clA.size(), clB.size(), clX.size());
    }

}
