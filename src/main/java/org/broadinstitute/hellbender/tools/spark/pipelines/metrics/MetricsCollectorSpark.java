package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import htsjdk.samtools.SAMFileHeader;

import htsjdk.samtools.metrics.Header;
import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.engine.AuthHolder;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.metrics.MetricsArgs;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.Serializable;
import java.util.List;

/**
 * Interface implemented by Spark metrics collectors to allow them
 * to run as either a standalone Spark tool, or under the control of
 * CollectMultipleMetricsSpark, which reuses the same input RDD to run
 * multiple collectors.
 *
 * Spark collectors should be factored into the following implementation
 * classes:
 *
 * <li>
 * A class derived from MetricsArgs that defines the input arguments for
 * the collector in a form that is suitable as a command line argument
 * collection.
 * A class that implements this interface, parameterized with the
 * MetricsArgs-derived class.
 * </li>
 *
 * These two classes can then be used either in the standalone collector tool (see
 * CollectInsertSizeMetricsSpark as an example), or from CollectMultipleMetricsSpark.
 *
 * The general lifecyle of a Spark collector looks like this:
 *
 *     CollectorType collector = new CollectorType<CollectorArgType>()
 *     CollectorArgType args = // may call collector.gatherInputArguments();
 *
 *     // pass the input arguments back to the collector for initialization
 *     collector.initializeCollector(args);
 *
 *     ReadFilter filter == collector.getReadFilter();
 *     collector.collectMetrics(
 *         getReads().filter(filter),
 *         getHeaderForReads(),
 *         getReadSourceName(),
 *         getAuthHolder()
 *     );
 *     collector.finishCollection();
 *
 */
public interface MetricsCollectorSpark<T extends MetricsArgs> extends Serializable
{
    static final long serialVersionUID = 1l;

    /**
     * Advertise whether or not this collector requires a reference.
     * @return true if this collector requires that a reference be provided.
     */
    default boolean requiresReference() { return false;}

    /**
     * Give the collector a chance to gather and return any input argument values
     * specific to this collector type, i.e, the argument values specified
     * at the command line by the user of a standalone metric collector tool.
     *
     * This method is primarily for use in a standalone collector tool in order
     * transfer the command line argument values for that tool. However, it may
     * never be called if the collector is being used by a pipeline driver such
     * CollectMultipleMetrics, since the arguments will be provided directly to the
     * collector's initializeCollector method by the driver.
     * @return object of type T
     */
    default T gatherInputArguments() { return null; };

    /**
     * Transfer the collector's input arguments to the collector (if the collector
     * is being driven by a standalone tool, these arguments will have been returned
     * by gatherInputArguments; in the case of a pipeline driver they will be provided
     * directly by the driver. This method will always be called before either
     * getReadFilter (so the collector can use the arguments to initialize the
     * readFilter) or collectMetrics.
     * @param inputArgs an object that contains the argument values to be used for
     *                  the collector
     * @param defaultHeaders default metrics headers from the containing tool
     */
    void initializeCollector(T inputArgs, List<Header> defaultHeaders);

    /**
     * Return the read filter required for this collector.
     * @param samHeader the SAMFileHeader for the input BAM
     * @return the read filter required for this collector
     */
    default ReadFilter getCollectorReadFilter(SAMFileHeader samHeader) {
        return ReadFilterLibrary.ALLOW_ALL_READS;
    }

    /**
     * Do the actual metrics collection on the provided RDD.
     * @param filteredReads The reads to be analyzed for this collector. The reads will have already
     *                      been filtered by this collector's read filter.
     * @param samHeader The SAMFileHeader associated with the reads in the input RDD.
     * @param inputBaseName base name of the input file
     * @param authHolder authHolder
     */
    void collectMetrics(
            JavaRDD<GATKRead> filteredReads,
            SAMFileHeader samHeader,
            String inputBaseName,
            AuthHolder authHolder
    );

    default void finishCollection() { }
}
