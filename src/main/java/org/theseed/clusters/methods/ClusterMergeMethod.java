/**
 *
 */
package org.theseed.clusters.methods;

/**
 * This is the enumeration for a method to merge two clusters.  The only thing
 * that matters is how we compute the similarity between clusters after merging.
 *
 * To convert the similarity of A to X to the similarity of the merged AB to X,
 * we need the similarity from A to X, the similarity of B to X, and the
 * similarity of A to B, and the size of each cluster A, B, and X.
 *
 * COMPLETE 	produces small, tight clusters
 * AVERAGE 		produces slightly larger clusters and is useful when the data is fuzzy
 * SINGLE		is the traditional method, but it is prone to long chains
 *
 * @author Bruce Parrello
 *
 */
public enum ClusterMergeMethod {

    /** The similarity is equal to the similarity of the least similar elements of the two sets */
    COMPLETE {
        @Override
        public double mergedSim(double ab, double ax, double bx, int asz, int bsz, int xsz) {
            // AX and BX are already the lowest similarity of X to A, and the lowest of X to B.
            // The desired similarity is the minimum of these two.
            return Math.min(ax, bx);
        }

        @Override
        public double mergedScore(double a, double b, double ab, int asz, int bsz) {
            double retVal = Math.min(a, b);
            return Math.min(retVal, ab);
        }
    },

    /** The similarity is equal to the similarity of the most similar elements of the two sets */
    SINGLE {
        @Override
        public double mergedSim(double ab, double ax, double bx, int asz, int bsz, int xsz) {
            // AX and BX are already the highest similarity of X to A, and the highest of X to B.
            // The desired similarity is the maximum of these two.
            return Math.max(ax, bx);
        }

        @Override
        public double mergedScore(double a, double b, double ab, int asz, int bsz) {
            // An internal similarity is only valid if the set size is greater than 1.
            // Get the maximum valid similarity.
            double retVal = ab;
            if (asz > 1)
                retVal = Math.max(retVal, a);
            if (bsz > 1)
                retVal = Math.max(retVal, b);
            return retVal;
        }
    },

    /** The similarity is the average similarity between the data points in the clusters. */
    AVERAGE {
        @Override
        public double mergedSim(double ab, double ax, double bx, int asz, int bsz, int xsz) {
            // AX is the average of all elements of A to all elements of X.  BX is the average
            // of all elements of B to all elements of X.  We use the set sizes to compute
            // the new mean from the old means.
            return (asz*ax + bsz*bx)/(asz+bsz);
        }

        @Override
        public double mergedScore(double a, double b, double ab, int asz, int bsz) {
            double retVal = ab;
            int n = asz * bsz;
            if (asz > 1) {
                // Note we must multiple the A mean by the number of connections in A,
                // which is Asz*(Asz-1)/2.
                int triangle = (asz * (asz - 1))/2;
                retVal = (retVal * n + a * triangle) / (n + triangle);
                n += triangle;
            }
            if (bsz > 1) {
                int triangle = (bsz * (bsz - 1))/2;
                retVal = (retVal * n + b * triangle) / (n + triangle);
            }
            return retVal;
        }
    };

    /**
     * Compute the similarity between the merged cluster AB and the external
     * cluster X.
     *
     * @param ab	similarity of A to B
     * @param ax	similarity of A to X
     * @param bx	similarity of B to X
     * @param asz	size of A
     * @param bsz	size of B
     * @param xsz	size of X
     *
     * @return the similarity of the merged cluster AB to X
     */
    public abstract double mergedSim(double ab, double ax, double bx, int asz, int bsz, int xsz);

    /**
     * Compute the internal similarity of a merged cluster.
     *
     * @param a		internal similarity of cluster A
     * @param b		internal similarity of cluster B
     * @param ab	similarity between A and B
     * @param asz	size of A
     * @param bsz	size of B
     */
    public abstract double mergedScore(double a, double b, double ab, int asz, int bsz);

}
