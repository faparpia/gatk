package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.metrics.MetricsArgs;
import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * Base class for standalone Spark metrics collector tools.ß
 *
 */
public abstract class MetricsCollectorToolSpark<T extends MetricsArgs>
        extends GATKSparkTool
        implements MetricsCollectorSpark<T> {

    private static final long serialVersionUID = 1l;

    @Override
    public final boolean requiresReads(){ return true; }

    @Override
    public final ReadFilter makeReadFilter() {
        return getCollectorReadFilter(getHeaderForReads());
    }

    /**
     * The runTool method used when the derived metrics collector tools are run
     * "standalone". The filtered RDD is passed to the collectMetrics
     * method which does the bulk of the analysis.
     *
     * @param ctx our Spark context
     */
    @Override
    protected void runTool( JavaSparkContext ctx ) {
        validateInputState();
        validateExpectedSortOrder(SAMFileHeader.SortOrder.coordinate, false);

        //  Execute the collector lifecycle
        T args = gatherInputArguments();
        if (null == args) {
            // Null args indicates an incorrectly written collector. Metrics collector arg objects
            // are always derived from MetricsArgs, so they always have at least an output argument
            // and should not be null.
            throw new GATKException.ShouldNeverReachHereException
                    ("A Spark metrics collector must return a non-a null argument object");
        }

        initializeCollector(args, getDefaultHeaders());
        final JavaRDD<GATKRead> filteredReads = getReads();
        collectMetrics(
                filteredReads,
                getHeaderForReads(),
                getReadSourceName(),
                getAuthHolder()
        );
        finishCollection();
    }

    private void validateInputState() {
        //TODO: temporary references are implemented
        if (requiresReference()) {
            throw new UnsupportedOperationException("Requires reference for collector not yet implemented");
        }
    }

    /**
     * Validate the input sort order
     */
    protected void validateExpectedSortOrder(
            final SAMFileHeader.SortOrder expectedSortOrder,
            final boolean assumeSorted)
    {
        final SAMFileHeader.SortOrder sortOrder = getHeaderForReads().getSortOrder();
        if (sortOrder != expectedSortOrder) {
            final String message = String.format("File %s has sort order (%s) but (%s) is required.",
                    getReadSourceName(),
                    sortOrder.name(),
                    expectedSortOrder.name());
            if (assumeSorted) {
                logger.warn(message + " Assuming it's coordinate sorted anyway.");
            }
            else {
                throw new UserException(message + "If you believe the file to be sorted correctly, use ASSUME_SORTED=true");
            }
        }
    }

}

