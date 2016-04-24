package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.*;
import htsjdk.samtools.fastq.BasicFastqWriter;
import htsjdk.samtools.fastq.FastqRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKRegistrator;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.utils.HopscotchHashSet;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump FASTQs for local assembly of putative genomic breakpoints.",
        programGroup = SparkProgramGroup.class)
public final class FindBreakpointEvidenceSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private static final int MIN_MAPQ = 20;
    private static final int MIN_MATCH_LEN = 45; // minimum length of matched portion of an interesting alignment
    //private static final int MAX_FRAGMENT_LEN = 2000;
    private static final int MAX_COVERAGE = 1000;

    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");

    @Argument(doc = "directory for fastq output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    private String outputDir;

    @Argument(doc = "directory for evidence output", fullName = "breakpointEvidenceDir", optional = true)
    private String evidenceDir;

    @Argument(doc = "file for breakpoint intervals output", fullName = "breakpointIntervals", optional = true)
    private String intervalFile;

    @Argument(doc = "file for qname intervals output", fullName = "qnameIntervals", optional = true)
    private String qNameFile;

    @Argument(doc = "file for kmer intervals output", fullName = "kmerIntervals", optional = true)
    private String kmerFile;

    /**
     * This is a path to a file of kmers that appear too frequently in the reference to be usable as probes to localize
     * reads.  We don't calculate it here, because it depends only on the reference.
     * The program FindBadGenomicKmersSpark can produce such a list for you.
     */
    @Argument(doc = "file containing ubiquitous kmer list", fullName = "kmersToIgnore", optional = false)
    private String kmersToIgnoreFilename;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        if ( getHeaderForReads().getSortOrder() != SAMFileHeader.SortOrder.coordinate ) {
            throw new GATKException("The reads must be coordinate sorted.");
        }

        // Process the input again, this time pulling all reads that contain kmers associated with a given breakpoint,
        // and writing those reads into a separate FASTQ for each breakpoint.
        final Broadcast<HopscotchHashSet<KmerCountAndInterval>> broadcastKmerAndIntervalsSet =
                ctx.broadcast(new HopscotchHashSet<>(getKmerIntervals(ctx)));
        getUnfilteredReads()
            .filter(read ->
                    !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                            !read.isDuplicate() && !read.failsVendorQualityCheck())
            .mapPartitionsToPair(readItr ->
                    new MapPartitioner<>(readItr, new ReadsForIntervalFinder(broadcastKmerAndIntervalsSet.value())), false)
            .groupByKey()
            .foreach(intervalReads -> writeFastq(intervalReads, outputDir));

        log("Wrote assembly FASTQs.");
    }

    private void writeFastq( final Tuple2<Integer, Iterable<FastqRecord>> intervalReads, final String outputDir ) {
        final String fileName = outputDir + "/assembly" + intervalReads._1 + ".fastq";
        final PipelineOptions popts = null;
        try ( final BasicFastqWriter writer =
                      new BasicFastqWriter(
                              new PrintStream(
                                      new BufferedOutputStream(
                                              BucketUtils.createFile(fileName, popts)))) ) {
            intervalReads._2.forEach(writer::write);
        }
    }

    private List<KmerCountAndInterval> getKmerIntervals(final JavaSparkContext ctx ) {
        final PipelineOptions pipelineOptions = getAuthenticatedGCSOptions();
        final Broadcast<Set<SVKmer>> broadcastKmerKillSet =
                ctx.broadcast(SVKmer.readKmersFile(kmersToIgnoreFilename, pipelineOptions));
        final Broadcast<HopscotchHashSet<QNameAndInterval>> broadcastQNameAndIntervalsSet =
                ctx.broadcast(new HopscotchHashSet<>(getQNames(ctx)));

        final JavaRDD<GATKRead> unfilteredReads = getUnfilteredReads();
        final List<KmerCountAndInterval> kmerIntervals =
                unfilteredReads
                        .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() &&
                                !read.isSecondaryAlignment() && !read.isSupplementaryAlignment())
                        .mapPartitions(readItr ->
                                new QNameKmerizer(broadcastQNameAndIntervalsSet.value(),
                                                    broadcastKmerKillSet.value()).call(readItr), false)
                        .distinct()
                        .mapPartitions(kmerItr -> new KmerCleaner().call(kmerItr))
                        .collect();

        broadcastQNameAndIntervalsSet.destroy();
        broadcastKmerKillSet.destroy();

        // record the kmers
        if ( kmerFile != null ) {
            try (final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                    BucketUtils.createFile(kmerFile, pipelineOptions)))) {
                for (final KmerCountAndInterval kmerCountAndInterval : kmerIntervals) {
                    writer.write(kmerCountAndInterval.toString() + "\n");
                }
            } catch (final IOException ioe) {
                throw new GATKException("Can't write kmer intervals file " + kmerFile, ioe);
            }
        }

        log("Discovered "+ kmerIntervals.size() +" kmers.");
        return kmerIntervals;
    }

    private List<QNameAndInterval> getQNames( final JavaSparkContext ctx ) {
        final ReadMetadata readMetadata = getMetadata();
        final Broadcast<ReadMetadata> broadcastMetadata = ctx.broadcast(readMetadata);
        final List<Interval> intervals = getIntervals(broadcastMetadata);
        final Broadcast<List<Interval>> broadcastIntervals = ctx.broadcast(intervals);

        final int meanBasesPerTemplate = readMetadata.getMeanBasesPerTemplate();
        final int maxIntervalLength = intervals.stream().mapToInt(Interval::getLength).max().orElse(0);
        final int maxQnamesPerInterval = MAX_COVERAGE*maxIntervalLength/meanBasesPerTemplate;
        final List<QNameAndInterval> qNames =
                getUnfilteredReads()
                        .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped())
                        .mapPartitions(readItr ->
                                new MapPartitioner<>(readItr,
                                        new QNameFinder(maxQnamesPerInterval,
                                                        broadcastMetadata.value(),
                                                        broadcastIntervals.value())), false)
                        .distinct()
                        .collect();

        broadcastIntervals.destroy();
        broadcastMetadata.destroy();

        cleanQNames(qNames, intervals, meanBasesPerTemplate);

        // record the qnames
        if ( qNameFile != null ) {
            final PipelineOptions pipelineOptions = getAuthenticatedGCSOptions();
            try (final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                    BucketUtils.createFile(qNameFile, pipelineOptions)))) {
                for (final QNameAndInterval qnameAndInterval : qNames) {
                    writer.write(qnameAndInterval.toString() + "\n");
                }
            } catch (final IOException ioe) {
                throw new GATKException("Can't write qname intervals file " + qNameFile, ioe);
            }
        }

        log("Discovered "+qNames.size()+" template names.");
        return qNames;
    }

    private void cleanQNames( final List<QNameAndInterval> qNames,
                              final List<Interval> intervals,
                              final int meanBasesPerTemplate ) {
        final int nIntervals = intervals.size();
        final int[] qNamesPerInterval = new int[nIntervals];
        for ( final QNameAndInterval qNameAndInterval : qNames ) {
            qNamesPerInterval[qNameAndInterval.getIntervalId()] += 1;
        }

        int nKilled = 0;
        for ( int idx = 0; idx != nIntervals; ++idx ) {
            final int nNamesForExcessiveCoverage = MAX_COVERAGE*intervals.get(idx).getLength()/meanBasesPerTemplate;
            if ( qNamesPerInterval[idx] > nNamesForExcessiveCoverage ) {
                qNamesPerInterval[idx] = -1;
                nKilled += 1;
            }
        }

        final int nNames = qNames.size();
        int writeIdx = 0;
        for ( int readIdx = 0; readIdx != nNames; ++readIdx ) {
            final QNameAndInterval qNameAndInterval = qNames.get(readIdx);
            if ( qNamesPerInterval[qNameAndInterval.getIntervalId()] != -1 ) {
                if ( readIdx != writeIdx ) qNames.set(writeIdx, qNameAndInterval);
                writeIdx += 1;
            }
        }
        for ( int deleteIdx = nNames-1; deleteIdx >= writeIdx; --deleteIdx ) {
            qNames.remove(deleteIdx);
        }

        log("Killed "+nKilled+" intervals that had >"+MAX_COVERAGE+"x coverage.");
    }

    private List<Interval> getIntervals( final Broadcast<ReadMetadata> broadcastMetadata ) {
        // find all breakpoint evidence, then filter for pile-ups
        final int maxFragmentSize = broadcastMetadata.value().getMaxMedianFragmentSize();
        final List<SAMSequenceRecord> contigs = getHeaderForReads().getSequenceDictionary().getSequences();
        final int nContigs = contigs.size();
        final JavaRDD<BreakpointEvidence> evidenceRDD =
                getUnfilteredReads()
                        .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped() &&
                                read.getMappingQuality() >= MIN_MAPQ &&
                                read.getCigar().getCigarElements()
                                        .stream()
                                        .filter(ele -> ele.getOperator()==CigarOperator.MATCH_OR_MISMATCH)
                                        .mapToInt(CigarElement::getLength)
                                        .sum() >= MIN_MATCH_LEN)
                        .mapPartitions(readItr ->
                                new MapPartitioner<>(readItr, new ReadClassifier(broadcastMetadata.value())), true)
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr, new BreakpointClusterer(2*maxFragmentSize)), true)
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr,
                                        new WindowSorter(3*maxFragmentSize), new BreakpointEvidence(nContigs)), true);

        // record the evidence
        if ( evidenceDir != null ) {
            evidenceRDD.cache();
            evidenceRDD.saveAsTextFile(evidenceDir);
        }

        // find discrete intervals that contain the breakpoint evidence
        final Iterator<Interval> intervalItr =
                evidenceRDD
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr,
                                        new IntervalMapper(maxFragmentSize), new BreakpointEvidence(nContigs)), true)
                        .collect()
                        .iterator();

        // coalesce overlapping intervals (can happen at partition boundaries)
        final List<Interval> intervals = new ArrayList<>();
        if ( intervalItr.hasNext() ) {
            Interval prev = intervalItr.next();
            while ( intervalItr.hasNext() ) {
                final Interval next = intervalItr.next();
                if ( prev.isDisjointFrom(next) ) {
                    intervals.add(prev);
                    prev = next;
                } else {
                    prev = prev.join(next);
                }
            }
            intervals.add(prev);
        }

        if ( evidenceDir != null ) evidenceRDD.unpersist();

        // record the intervals
        if ( intervalFile != null ) {
            final PipelineOptions pipelineOptions = getAuthenticatedGCSOptions();
            try (final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                    BucketUtils.createFile(intervalFile, pipelineOptions)))) {
                for (final Interval interval : intervals) {
                    final String seqName = contigs.get(interval.getContig()).getSequenceName();
                    writer.write(seqName + " " + interval.getStart() + " " + interval.getEnd() + "\n");
                }
            } catch (final IOException ioe) {
                throw new GATKException("Can't write intervals file " + intervalFile, ioe);
            }
        }

        log("Discovered "+intervals.size()+" intervals.");
        return intervals;
    }

    private ReadMetadata getMetadata() {
        final SAMFileHeader header = getHeaderForReads();
        final List<SAMReadGroupRecord> groups = header.getReadGroups();
        final int nGroups = groups.size();
        final ReadMetadata.ReadGroupFragmentStatistics groupStats =
                new ReadMetadata.ReadGroupFragmentStatistics(400.f, 75.f);
        final List<ReadMetadata.ReadGroupFragmentStatistics> stats = new ArrayList<>(nGroups);
        for ( int idx = 0; idx != nGroups; ++idx ) {
            stats.add(groupStats);
        }
        final ReadMetadata readMetadata = new ReadMetadata(header, stats, groupStats, getMeanBasesPerTemplate());
        log("Metadata retrieved.");
        return readMetadata;
    }

    private int getMeanBasesPerTemplate() {
        return (int)getUnfilteredReads()
                .aggregate(new ReadCountAndLength(), ReadCountAndLength::new, ReadCountAndLength::new)
                .getMeanLength();
    }

    private void log( final String message ) {
        System.out.println(dateFormatter.format(System.currentTimeMillis())+message);
    }

    private static final class ReadCountAndLength implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long count;
        private final long length;

        public ReadCountAndLength() { count = 0; length = 0; }

        public ReadCountAndLength( final ReadCountAndLength countNLength, final GATKRead read ) {
            this.count = countNLength.count + 1;
            this.length = countNLength.length + (read.isPaired()?2:1)*read.getLength(); }

        public ReadCountAndLength( final ReadCountAndLength countNLength1, final ReadCountAndLength countNLength2 ) {
            this.count = countNLength1.count + countNLength2.count;
            this.length = countNLength1.length + countNLength2.length;
        }

        private ReadCountAndLength( final Kryo kryo, final Input input ) {
            count = input.readLong();
            length = input.readLong();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeLong(count);
            output.writeLong(length);
        }

        public long getMeanLength() { return count != 0 ? length/count : 0; }

        private static final class Serializer extends com.esotericsoftware.kryo.Serializer<ReadCountAndLength> {
            @Override
            public void write( final Kryo kryo, final Output output, final ReadCountAndLength readCountAndLength ) {
                readCountAndLength.serialize(kryo, output);
            }

            @Override
            public ReadCountAndLength read( final Kryo kryo, final Input input, final Class<ReadCountAndLength> klass ) {
                return new ReadCountAndLength(kryo, input);
            }
        }
    }

    /**
     * A class that acts as a filter for breakpoint evidence.
     * It passes only that evidence that is part of a putative cluster.
     */
    private static final class BreakpointClusterer
            implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final int staleEventDistance;
        private final SortedMap<BreakpointEvidence, Boolean> locMap = new TreeMap<>();
        private final List<Map.Entry<BreakpointEvidence, Boolean>> reportableEntries = new ArrayList<>(2*MIN_EVIDENCE);
        private final Iterator<BreakpointEvidence> noEvidence = Collections.emptyIterator();
        private int currentContig = -1;

        private static final int MIN_EVIDENCE = 15; // minimum evidence count in a cluster

        BreakpointClusterer( final int staleEventDistance ) {
            this.staleEventDistance = staleEventDistance;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            if ( evidence.getContigIndex() != currentContig ) {
                currentContig = evidence.getContigIndex();
                locMap.clear();
            }

            locMap.put(evidence, true);

            final int locusStart = evidence.getContigStart();
            final int locusEnd = evidence.getContigEnd();
            final int staleEnd = locusStart - staleEventDistance;
            int evidenceCount = 0;
            reportableEntries.clear();
            final Iterator<Map.Entry<BreakpointEvidence, Boolean>> itr = locMap.entrySet().iterator();
            while ( itr.hasNext() ) {
                final Map.Entry<BreakpointEvidence, Boolean> entry = itr.next();
                final BreakpointEvidence evidence2 = entry.getKey();
                final int contigEnd = evidence2.getContigEnd();
                if ( contigEnd <= staleEnd ) itr.remove();
                else if ( evidence2.getContigStart() >= locusEnd ) break;
                else if ( contigEnd > locusStart ) {
                    evidenceCount += 1;
                    if ( entry.getValue() ) reportableEntries.add(entry);
                }
            }

            if ( evidenceCount >= MIN_EVIDENCE ) {
                return reportableEntries.stream()
                        .map(entry -> { entry.setValue(false); return entry.getKey(); })
                        .iterator();
            }
            return noEvidence;
        }
    }

    /**
     * Class to fully sort a stream of nearly sorted BreakpointEvidences.
     */
    private static final class WindowSorter
            implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final SortedSet<BreakpointEvidence> recordSet = new TreeSet<>();
        private final List<BreakpointEvidence> reportableEvidence = new ArrayList<>();
        private final int windowSize;
        private int currentContig = -1;

        WindowSorter( final int windowSize ) {
            this.windowSize = windowSize;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            reportableEvidence.clear();
            if ( evidence.getContigIndex() != currentContig ) {
                reportableEvidence.addAll(recordSet);
                recordSet.clear();
                currentContig = evidence.getContigIndex();
            } else {
                final int reportableEnd = evidence.getContigStart() - windowSize;
                final Iterator<BreakpointEvidence> itr = recordSet.iterator();
                while ( itr.hasNext() ) {
                    final BreakpointEvidence evidence2 = itr.next();
                    if ( evidence2.getContigStart() >= reportableEnd ) break;
                    reportableEvidence.add(evidence2);
                    itr.remove();
                }
            }
            recordSet.add(evidence);
            return reportableEvidence.iterator();
        }
    }

    /**
     * Minimalistic simple interval.
     */
    private static final class Interval {
        private final int contig;
        private final int start;
        private final int end;

        public Interval( final int contig, final int start, final int end ) {
            this.contig = contig;
            this.start = start;
            this.end = end;
        }

        private Interval( final Kryo kryo, final Input input ) {
            contig = input.readInt();
            start = input.readInt();
            end = input.readInt();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeInt(contig);
            output.writeInt(start);
            output.writeInt(end);
        }

        public int getContig() { return contig; }
        public int getStart() { return start; }
        public int getEnd() { return end; }
        public int getLength() { return end-start; }

        public boolean isDisjointFrom( final Interval that ) {
            return this.contig != that.contig || this.end < that.start || that.end < this.start;
        }

        public Interval join( final Interval that ) {
            if ( this.contig != that.contig ) throw new GATKException("Joining across contigs.");
            return new Interval(contig, Math.min(this.start, that.start), Math.max(this.end, that.end));
        }

        private static final class Serializer extends com.esotericsoftware.kryo.Serializer<Interval> {
            @Override
            public void write( final Kryo kryo, final Output output, final Interval interval ) {
                interval.serialize(kryo, output);
            }

            @Override
            public Interval read( final Kryo kryo, final Input input, final Class<Interval> klass ) {
                return new Interval(kryo, input);
            }
        }
    }

    /**
     * A class to examine a stream of BreakpointEvidence, and group it into Intervals.
     */
    private static final class IntervalMapper implements Function<BreakpointEvidence, Iterator<Interval>> {
        private final Iterator<Interval> noInterval = Collections.emptyIterator();
        private final int gapSize;
        private int contig = -1;
        private int start;
        private int end;

        IntervalMapper( final int gapSize ) {
            this.gapSize = gapSize;
        }

        public Iterator<Interval> apply( final BreakpointEvidence evidence ) {
            Iterator<Interval> result = noInterval;
            if ( evidence.getContigIndex() != contig ) {
                if ( contig != -1 ) {
                    result = new SVUtils.SingletonIterator<>(new Interval(contig, start, end));
                }
                contig = evidence.getContigIndex();
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else if ( evidence.getContigStart() >= end+gapSize ) {
                result = new SVUtils.SingletonIterator<>(new Interval(contig, start, end));
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else {
                end = Math.max(end, evidence.getContigEnd());
            }
            return result;
        }
    }

    /**
     * A template name and an intervalId.
     * Note:  hashCode does not depend on intervalId, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode is on K only.
     */
    private static final class QNameAndInterval implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] qName;
        private final int hashVal;
        private final int intervalId;

        QNameAndInterval( final String qName, final int intervalId ) {
            this.qName = qName.getBytes();
            this.hashVal = qName.hashCode();
            this.intervalId = intervalId;
        }

        private QNameAndInterval( final Kryo kryo, final Input input ) {
            final int nameLen = input.readInt();
            qName = input.readBytes(nameLen);
            hashVal = input.readInt();
            intervalId = input.readInt();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeInt(qName.length);
            output.writeBytes(qName);
            output.writeInt(hashVal);
            output.writeInt(intervalId);
        }

        public String getQName() { return new String(qName); }
        public int getIntervalId() { return intervalId; }

        @Override
        public int hashCode() { return hashVal; }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof QNameAndInterval && equals((QNameAndInterval) obj);
        }

        public boolean equals( final QNameAndInterval that ) {
            return Arrays.equals(this.qName, that.qName) && this.intervalId == that.intervalId;
        }

        public boolean sameName( final byte[] name ) { return Arrays.equals(qName, name); }

        public String toString() { return new String(qName)+" "+intervalId; }

        private static final class Serializer extends com.esotericsoftware.kryo.Serializer<QNameAndInterval> {
            @Override
            public void write( final Kryo kryo, final Output output, final QNameAndInterval qNameAndInterval ) {
                qNameAndInterval.serialize(kryo, output);
            }

            @Override
            public QNameAndInterval read( final Kryo kryo, final Input input, final Class<QNameAndInterval> klass ) {
                return new QNameAndInterval(kryo, input);
            }
        }
    }

    /**
     * Class to find the template names associated with reads in specified intervals.
     */
    private static final class QNameFinder implements Function<GATKRead, Iterator<QNameAndInterval>> {
        private final int maxQnamesPerInterval;
        private final ReadMetadata metadata;
        private final List<Interval> intervals;
        private final int[] qNamesPerInterval;
        private final Iterator<QNameAndInterval> noName = Collections.emptyIterator();
        private int intervalsIndex = 0;

        QNameFinder( final int maxQnamesPerInterval, final ReadMetadata metadata, final List<Interval> intervals ) {
            this.maxQnamesPerInterval = maxQnamesPerInterval;
            this.metadata = metadata;
            this.intervals = intervals;
            qNamesPerInterval = new int[intervals.size()];
        }

        @Override
        public Iterator<QNameAndInterval> apply( final GATKRead read ) {
            final int readContigId = metadata.getContigID(read.getContig());
            final int readStart = read.getUnclippedStart();
            final int intervalsSize = intervals.size();
            while ( intervalsIndex < intervalsSize ) {
                final Interval interval = intervals.get(intervalsIndex);
                if ( interval.getContig() > readContigId ) break;
                if ( interval.getContig() == readContigId && interval.getEnd() > read.getStart() ) break;
                intervalsIndex += 1;
            }
            if ( intervalsIndex >= intervalsSize ) return noName;
            final Interval indexedInterval = intervals.get(intervalsIndex);
            final Interval readInterval = new Interval(readContigId, readStart, read.getUnclippedEnd());
            if ( indexedInterval.isDisjointFrom(readInterval) ) return noName;
            if ( qNamesPerInterval[intervalsIndex]++ > maxQnamesPerInterval ) return noName;
            return new SVUtils.SingletonIterator<>(new QNameAndInterval(read.getName(), intervalsIndex));
        }
    }

    /**
     * A <SVKmer,IntervalId> pair.
     * Note:  hashCode is not overridden, and thus does not depend on intervalId, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode is on K only.
     */
    private final static class KmerCountAndInterval extends SVKmer {
        private final int intervalId;
        private int count;

        KmerCountAndInterval(final SVKmer kmer, final int intervalId ) {
            super(kmer);
            this.intervalId = intervalId;
            this.count = 0;
        }

        private KmerCountAndInterval(final Kryo kryo, final Input input ) {
            super(kryo, input);
            intervalId = input.readInt();
            count = input.readInt();
        }

        protected void serialize( final Kryo kryo, final Output output ) {
            super.serialize(kryo, output);
            output.writeInt(intervalId);
            output.writeInt(count);
        }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof KmerCountAndInterval && equals((KmerCountAndInterval)obj);
        }

        public boolean equals( final KmerCountAndInterval that ) {
            return equals((SVKmer)that) && this.intervalId == that.intervalId;
        }

        public int compareTo( final KmerCountAndInterval that ) {
            int result = compareTo((SVKmer)that);
            if ( result == 0 ) result = Integer.compare(this.intervalId, that.intervalId);
            return result;
        }

        public int getIntervalId() { return intervalId; }
        public int getCount() { return count; }
        public void incrementCount() { count += 1; }
        public void addCount( final int countIncrement ) { count += countIncrement; }

        public String toString() { return toString(SVConstants.KMER_SIZE)+" "+intervalId+" "+count; }

        private static final class Serializer extends com.esotericsoftware.kryo.Serializer<KmerCountAndInterval> {
            @Override
            public void write( final Kryo kryo, final Output output, final KmerCountAndInterval kmerCountAndInterval) {
                kmerCountAndInterval.serialize(kryo, output);
            }

            @Override
            public KmerCountAndInterval read(final Kryo kryo, final Input input, final Class<KmerCountAndInterval> klass ) {
                return new KmerCountAndInterval(kryo, input);
            }
        }
    }

    /**
     * Class that acts as a mapper from a stream of reads to a stream of KmerAndIntervals.
     * The template names of reads to kmerize, along with a set of kmers to ignore are passed in (by broadcast).
     */
    private static final class QNameKmerizer {
        private final HopscotchHashSet<QNameAndInterval> qNameAndIntervalSet;
        private final Set<SVKmer> kmersToIgnore;
        private final HopscotchHashSet<KmerCountAndInterval> kmerSet;

        QNameKmerizer( final HopscotchHashSet<QNameAndInterval> qNameAndIntervalSet,
                       final Set<SVKmer> kmersToIgnore ) {
            this.qNameAndIntervalSet = qNameAndIntervalSet;
            this.kmersToIgnore = kmersToIgnore;
            this.kmerSet = new HopscotchHashSet<>(10000000);
        }

        public Iterable<KmerCountAndInterval> call(final Iterator<GATKRead> readItr ) {
            while ( readItr.hasNext() ) {
                final GATKRead read = readItr.next();
                final String qName = read.getName();
                final int qNameHash = qName.hashCode();
                final byte[] qNameBytes = qName.getBytes();
                final Iterator<QNameAndInterval> names = qNameAndIntervalSet.bucketIterator(qNameHash);
                while ( names.hasNext() ) {
                    final QNameAndInterval qNameAndInterval = names.next();
                    if ( qNameAndInterval.hashCode() == qNameHash && qNameAndInterval.sameName(qNameBytes) ) {
                        SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                                .map(kmer -> kmer.canonical(SVConstants.KMER_SIZE))
                                .filter(kmer -> !kmersToIgnore.contains(kmer))
                                .map(kmer -> new KmerCountAndInterval(kmer, qNameAndInterval.getIntervalId()))
                                .forEach(kmerCountAndInterval -> kmerSet.put(kmerCountAndInterval).incrementCount());
                        break;
                    }
                }
            }
            return kmerSet;
        }
    }

    /**
     * Eliminates dups, and removes over-represented kmers.
     */
    private static final class KmerCleaner {
        private static final int MAX_INTERVALS = 4;
        private final HopscotchHashSet<KmerCountAndInterval> kmerSet = new HopscotchHashSet<>(600000);

        public Iterable<KmerCountAndInterval> call(final Iterator<KmerCountAndInterval> kmerItr ) {

            while ( kmerItr.hasNext() ) {
                final KmerCountAndInterval kmerCountAndInterval = kmerItr.next();
                final KmerCountAndInterval kmerCountAndInterval2 = kmerSet.put(kmerCountAndInterval);
                if ( kmerCountAndInterval != kmerCountAndInterval2 ) {
                    kmerCountAndInterval2.addCount(kmerCountAndInterval.getCount());
                }
            }
            final Iterator<KmerCountAndInterval> itr = kmerSet.iterator();
            while ( itr.hasNext() ) {
                if ( itr.next().getCount() == 1 ) itr.remove();
            }
            final int kmerSetCapacity = kmerSet.capacity();
            for ( int idx = 0; idx != kmerSetCapacity; ++idx ) {
                cleanBucket(idx);
            }
            return kmerSet;
        }

        private void cleanBucket( final int bucketIndex ) {
            final int nKmers = getBucketSize(bucketIndex);
            if ( nKmers <= MAX_INTERVALS ) return;
            final KmerCountAndInterval[] kmers = new KmerCountAndInterval[nKmers];
            final Iterator<KmerCountAndInterval> itr = kmerSet.bucketIterator(bucketIndex);
            int idx = 0;
            while ( itr.hasNext() ) {
                kmers[idx++] = itr.next();
            }
            Arrays.sort(kmers);
            int readIdx = 0;
            int testIdx = 1;
            while ( testIdx < nKmers ) {
                final SVKmer test = kmers[readIdx];
                while ( testIdx < nKmers && kmers[testIdx].equals(test) ) {
                    ++testIdx;
                }
                if ( testIdx-readIdx > MAX_INTERVALS ) {
                    while ( readIdx != testIdx ) {
                        kmerSet.remove(kmers[readIdx++]);
                    }
                }
                readIdx = testIdx;
                testIdx += 1;
            }
        }

        private int getBucketSize( final int bucketIndex ) {
            final Iterator<KmerCountAndInterval> itr = kmerSet.bucketIterator(bucketIndex);
            int result = 0;
            while ( itr.hasNext() ) {
                result += 1;
                itr.next();
            }
            return result;
        }
    }

    /**
     * Class that acts as a mapper from a stream of reads to a stream of <intervalId,read> pairs.
     * It knows which breakpoint(s) a read belongs to (if any) by kmerizing the read, and looking up each SVKmer in
     * a multi-map of SVKmers onto intervalIds.
     */
    private static final class ReadsForIntervalFinder
            implements Function<GATKRead, Iterator<Tuple2<Integer, FastqRecord>>> {
        private final HopscotchHashSet<KmerCountAndInterval> kmerCountAndIntervalSet;
        private final Set<Integer> intervalIds = new HashSet<>();
        private final List<Tuple2<Integer, FastqRecord>> readTuples = new ArrayList<>();
        private final Iterator<Tuple2<Integer, FastqRecord>> noReads = Collections.emptyIterator();

        ReadsForIntervalFinder(final HopscotchHashSet<KmerCountAndInterval> kmerCountAndIntervalSet) {
            this.kmerCountAndIntervalSet = kmerCountAndIntervalSet;
        }

        public Iterator<Tuple2<Integer, FastqRecord>> apply(final GATKRead read) {
            intervalIds.clear();
            SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                    .map( kmer -> kmer.canonical(SVConstants.KMER_SIZE) )
                    .forEach( kmer -> {
                        final Iterator<KmerCountAndInterval> itr = kmerCountAndIntervalSet.bucketIterator(kmer.hashCode());
                        while ( itr.hasNext() ) {
                            final KmerCountAndInterval kmerCountAndInterval = itr.next();
                            if (kmer.equals(kmerCountAndInterval)) intervalIds.add(kmerCountAndInterval.getIntervalId());
                        }
                    });
            if (intervalIds.isEmpty()) return noReads;
            readTuples.clear();
            String readName = read.getName();
            if ( read.isPaired() ) readName += read.isFirstOfPair() ? "/1" : "/2";
            final FastqRecord fastqRecord =
                    new FastqRecord(readName, read.getBasesString(), null, ReadUtils.getBaseQualityString(read));
            intervalIds.stream().forEach(intervalId -> readTuples.add(new Tuple2<>(intervalId, fastqRecord)));
            return readTuples.iterator();
        }
    }

    public static final class Registrator implements KryoRegistrator {
        @Override
        public void registerClasses( final Kryo kryo ) {
            new GATKRegistrator().registerClasses(kryo);
            new BreakpointEvidence.Registrator().registerClasses(kryo);
            new SVKmer.Registrator().registerClasses(kryo);
            new ReadMetadata.Registrator().registerClasses(kryo);
            new HopscotchHashSet.Registrator().registerClasses(kryo);
            kryo.register(Object[].class);
            kryo.register(java.util.ArrayList.class);
            kryo.register(htsjdk.samtools.fastq.FastqRecord.class);
            kryo.register(ReadCountAndLength.class, new ReadCountAndLength.Serializer());
            kryo.register(Interval.class, new Interval.Serializer());
            kryo.register(QNameAndInterval.class, new QNameAndInterval.Serializer());
            kryo.register(KmerCountAndInterval.class, new KmerCountAndInterval.Serializer());
        }
    }
}
