package org.broadinstitute.hellbender.tools.spark.sv;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump FASTQs for local assembly of putative genomic breakpoints.",
        programGroup = SparkProgramGroup.class)
public final class KmerHistogrammer extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() { return true; }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        final List<Tuple2<Integer, Integer>> histogram =
            getUnfilteredReads()
                .filter(read ->
                    !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                    !read.isDuplicate() && !read.failsVendorQualityCheck())
                .mapPartitionsToPair(readItr ->
                    new MapPartitioner<GATKRead, Tuple2<SVKmer, Integer>>(readItr, read ->
                        SVKmerizer.stream(read.getBases(),SVConstants.KMER_SIZE)
                            .map(kmer -> kmer.canonical(SVConstants.KMER_SIZE))
                            .map(kmer ->
                                new Tuple2<>(kmer,1)).iterator()),false)
                .reduceByKey(Integer::sum)
                .mapToPair(t2 -> new Tuple2<>(t2._2, 1))
                .reduceByKey(Integer::sum)
                .collect();
        Collections.sort(histogram, (t1, t2) -> t1._1.compareTo(t2._1));
        for ( final Tuple2<Integer, Integer> t2 : histogram )
            System.out.println(t2._1+"\t"+t2._2);
    }
}
