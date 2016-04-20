package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.Header;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.broadinstitute.hellbender.metrics.QualityYieldMetricsArgs;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;

/**
 * Tool that instantiates and executes multiple metrics programs using a single RDD.
 */
@CommandLineProgramProperties(
        summary = "Takes an input SAM/BAM/CRAM file and reference sequence and runs one or more " +
                "metrics modules at the same time to cut down on I/O. Currently all programs are run with " +
                "default options and fixed output extensions, but this may become more flexible in future.",
        oneLineSummary = "A \"meta-metrics\" calculating program that produces multiple metrics for the provided SAM/BAM/CRAM file",
        programGroup = SparkProgramGroup.class
)
public final class CollectMultipleMetricsSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    @Argument(fullName=StandardArgumentDefinitions.ASSUME_SORTED_LONG_NAME,
                shortName = StandardArgumentDefinitions.ASSUME_SORTED_SHORT_NAME,
                doc = "If true (default), then the sort order in the header file will be ignored.")
    public boolean ASSUME_SORTED = true;

    //TODO: do we need this for Spark ?
    //@Argument(doc = "Stop after processing N reads, mainly for debugging.")
    //public int STOP_AFTER = 0;

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
                shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
                doc = "Base name of output files.")
    public String outputBaseName;

    @Argument(shortName="LEVEL", doc="The level(s) at which to accumulate metrics.  ")
    public Set<MetricAccumulationLevel> metricAccumulationLevel = EnumSet.of(MetricAccumulationLevel.ALL_READS);

    @Argument(doc = "List of metrics collectors to apply during the pass through the SAM file.")
    public List<Collectors> collectors = new ArrayList<>();

    private interface CollectorPrimer {
        /**
         * For each collector type supported by this tool, we need to provide a
         * type-safe, collector-specific primer that creates and populates an
         * instance of the collector's input arguments class; creates an instance
         * of the collector; initializes it with the arguments object; and then
         * forwards the initialized collector to the executeLifeCycle function
         */
        void primeCollector(
                final String outputBaseName,
                final Set<MetricAccumulationLevel> metricAccumulationLevel,
                final List<Header> defaultHeaders,
                final Consumer<MetricsCollectorSpark<?>> executeCollectorLifecyle
        );
    }

    // Enum of Collector types supported by this tool.
    public static enum Collectors implements CollectorPrimer {
        CollectInsertSizeMetrics {
            @Override
            public void primeCollector(
                    final String outputBaseName,
                    final Set<MetricAccumulationLevel> metricAccumulationLevel,
                    final List<Header> defaultHeaders,
                    final Consumer<MetricsCollectorSpark<?>> executeCollectorLifecyle)
            {
                // disambiguate this collector's output files from the other collectors
                final String localBaseName = outputBaseName + ".insertMetrics";

                final InsertSizeMetricsCollectorSparkArgs isArgs = new InsertSizeMetricsCollectorSparkArgs();
                isArgs.output = new File(localBaseName + ".txt");
                isArgs.histogramPlotFile = localBaseName + ".pdf";
                isArgs.useEnd = InsertSizeMetricsCollectorSparkArgs.EndToUse.SECOND;
                isArgs.metricAccumulationLevel = metricAccumulationLevel;

                final InsertSizeMetricsCollectorSpark collector = new InsertSizeMetricsCollectorSpark();
                collector.initializeCollector(isArgs, defaultHeaders);

                executeCollectorLifecyle.accept(collector);
            }
        },
        CollectQualityYieldMetrics {
            @Override
            public void primeCollector(
                    final String outputBaseName,
                    final Set<MetricAccumulationLevel> metricAccumulationLevel,
                    final List<Header> defaultHeaders,
                    final Consumer<MetricsCollectorSpark<?>> executeCollectorLifecyle)
            {
                // disambiguate this collector's output files from the other collectors
                final String localBaseName = outputBaseName + ".qualityYieldMetrics";

                final QualityYieldMetricsArgs isArgs = new QualityYieldMetricsArgs();
                isArgs.output = new File(localBaseName + ".txt");

                final QualityYieldMetricsCollectorSpark collector = new QualityYieldMetricsCollectorSpark();
                collector.initializeCollector(isArgs, defaultHeaders);

                executeCollectorLifecyle.accept(collector);
            }
        }
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        // Check on the sort order of the input
        validateExpectedSortOrder(SAMFileHeader.SortOrder.coordinate, ASSUME_SORTED);

        final JavaRDD<GATKRead> unFilteredReads = getUnfilteredReads();

        if (collectors.size() == 0) { // run all collectors
            collectors.addAll(Arrays.asList(Collectors.values()));
        }
        if (collectors.size() > 1) {
            // if there is more than one collector to run, cache the
            // unfiltered RDD so we don't recompute it
            unFilteredReads.cache();
        }

        for (final CollectorPrimer primer : collectors) {
            primer.primeCollector(
                    outputBaseName,
                    metricAccumulationLevel,
                    getDefaultHeaders(),
                    metricsCollector -> {
                        if (metricsCollector.requiresReference()) {
                            throw new UnsupportedOperationException
                                    ("Requires reference for collector not yet implemented");
                        }
                        // execute the lifecycle of a collector that has already been
                            // initialized with it's input arguments
                            ReadFilter readFilter =
                                    metricsCollector.getCollectorReadFilter(getHeaderForReads());
                            metricsCollector.collectMetrics(
                                        unFilteredReads.filter(r -> readFilter.test(r)),
                                        getHeaderForReads(),
                                        getReadSourceName(),
                                        getAuthHolder()
                                        );
                            metricsCollector.finishCollection();
                    }
            );
        }
    }

    // Validate the input sort order
    private void validateExpectedSortOrder(
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
