package org.broadinstitute.hellbender.tools.spark.pipelines.metrics;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.Header;
import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.AuthHolder;
import org.broadinstitute.hellbender.metrics.QualityYieldMetricsArgs;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.List;

/**
 * Command line program to calibrate quality yield metrics
 *
 * This is the Spark version.
 */
@CommandLineProgramProperties(
        summary = "Collects quality yield metrics, a set of metrics that quantify the quality and yield of sequence data from a " +
                "SAM/BAM/CRAM input file.",
        oneLineSummary = "CollectQualityYieldMetrics on Spark",
        programGroup = SparkProgramGroup.class
)
public final class CollectQualityYieldMetricsSpark extends MetricsCollectorToolSpark<QualityYieldMetricsArgs> {

    private static final long serialVersionUID = 1L;

    @ArgumentCollection
    private QualityYieldMetricsArgs qualityYieldArgs = new QualityYieldMetricsArgs();

    private QualityYieldMetricsCollectorSpark qualityYieldCollector = new QualityYieldMetricsCollectorSpark();

    @Override
    public QualityYieldMetricsArgs gatherInputArguments() {
        return qualityYieldArgs;
    }

    public void initializeCollector(final QualityYieldMetricsArgs inputArgs, final List<Header> defaultHeaders) {
        qualityYieldCollector.initializeCollector(inputArgs, defaultHeaders);
    }

    @Override
    public void collectMetrics(
            final JavaRDD<GATKRead> filteredReads,
            final SAMFileHeader samHeader,
            final String inputBaseName,
            final AuthHolder authHolder)
    {
        qualityYieldCollector.collectMetrics(
                filteredReads,
                samHeader,
                inputBaseName,
                authHolder
        );
    }

}
