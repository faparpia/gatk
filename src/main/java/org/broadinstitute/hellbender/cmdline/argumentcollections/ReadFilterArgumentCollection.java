package org.broadinstitute.hellbender.cmdline.argumentcollections;


import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.cmdline.*;
import org.broadinstitute.hellbender.engine.filters.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.*;

import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

// TODO: how should this interact with --disable_all_read_filters ?
// TODO: should specified filters be destructive or additive ?
//

/**
 * Intended to be used as an {@link org.broadinstitute.hellbender.cmdline.ArgumentCollection} for specifying
 * read filters at the command line.
 *
 * Subclasses must override getReadFilters and addToReadFilters().
 */
public class ReadFilterArgumentCollection implements ArgumentCollectionDefinition {
    private static final Logger logger = LogManager.getLogger(ReadFilterArgumentCollection.class);
    private static final long serialVersionUID = 1L;

    @Argument(fullName = StandardArgumentDefinitions.READ_READ_FILTER_LONG_NAME,
            shortName = StandardArgumentDefinitions.READ_READ_FILTER_SHORT_NAME,
            doc = "Filters to apply to reads before analysis",
            common = true,
            optional = true)
    public List<String> readFilterNames = new ArrayList<>();

    //@ArgumentCollection
    //public WellformedReadFilter wfReadFilter;

    // read filter command line arguments with parameters
    //@ArgumentCollection
    //public ReadNameReadFilter rnReadFilter = new ReadNameReadFilter();

//    /**
//     *
//     * @return strings gathered from the command line -RF argument to be parsed into intervals to include
//     */
//    private List<String> getReadFilterStrings() {
//        return readFiltersNames;
//    }
//
//    /**
//     * Add an extra read filter to the list of filters to use.
//     * ONLY for testing -- will throw if called after interval parsing has been performed.
//     */
//    @VisibleForTesting
//    public void addToReadFilterStrings(String newReadFilter) {
//    }

    /**
     * Get a ReadFilter that represents the aggregate (AND) of all read filters specified on the command line.
     * @param wrapper function used to wrap each filter; used to aggregate CountingReadFilters when needed
     * @return an aggregate ReadFilter to be used for the containing tool
     */
    //TODO: this can't be CoountingReadFilter for Spark tools
    public CountingReadFilter getAggregateReadFilter(BiFunction<String, ReadFilter, CountingReadFilter> filterWrapper) {
        List<CountingReadFilter> specifiedFilters = getNamedReadFilters(readFilterNames);
        return specifiedFilters.stream().reduce(
                filterWrapper.apply("ALLOW_ALL_READS", ReadFilterLibrary.ALLOW_ALL_READS),
                (f1, f2) -> {return f1.and(filterWrapper.apply("generated", f2));}
                //TODO: need to use dynamic names above...
        );
    }

    private List<CountingReadFilter> getNamedReadFilters(List<String> filterNames) {
        final ClassFinder classFinder = new ClassFinder();
        classFinder.find("org.broadinstitute.hellbender.engine.filters", Predicate.class);
        //final Map<String, Class<?>> simpleNameToClass = new HashMap<>();

        ReadFilterLibrary rfl = new ReadFilterLibrary();

        //TODO: this is a list of objects not ReadFilter
        ArrayList<ReadFilter> readFilters = new ArrayList<>();
        for (String name: filterNames) {
            ReadFilter rf = rfl.getFilter(name);
            readFilters.add(rf);
        }
//        for (final Class<?> clazz : classFinder.getClasses()) {
//            // No interfaces, synthetic, primitive, local, or abstract classes.
//            if (ClassUtils.canMakeInstances(clazz) && clazz.isAnnotationPresent(ReadFilterArgument.class)) {
//                ReadFilterArgument rfa = clazz.getAnnotation(ReadFilterArgument.class);
//                for (String name: filterNames) {
//                    if (name.equals(rfa.fullName()) ||
//                            name.equals(rfa.shortName())) {
//                        try {
//                            readFilters.add(clazz.newInstance());
//                        }
//                        catch (IllegalAccessException | InstantiationException e) {
//                            throw new GATKException.CommandLineParserInternalException("");
//                        }
//                    }
//                }
//            }
//        }

        return new ArrayList<CountingReadFilter>();
    }

//    private void parseIntervals(final GenomeLocParser genomeLocParser) {
//        // return if no interval arguments at all
//        if (!intervalsSpecified()) {
//            throw new GATKException("Cannot call parseIntervals() without specifying either intervals to include or exclude.");
//        }
//n
//        GenomeLocSortedSet includeSortedSet;
//        if (getIntervalStrings().isEmpty()){
//            // the -L argument isn't specified, which means that -XL was, since we checked intervalsSpecified()
//            // therefore we set the include set to be the entire reference territory
//            includeSortedSet = GenomeLocSortedSet.createSetFromSequenceDictionary(genomeLocParser.getSequenceDictionary());
//        } else {
//            try {
//                includeSortedSet = IntervalUtils.loadIntervals(getIntervalStrings(), intervalSetRule, intervalMerging, intervalPadding, genomeLocParser);
//            } catch( UserException.EmptyIntersection e) {
//                throw new UserException.BadArgumentValue("-L, --interval_set_rule", getIntervalStrings()+","+intervalSetRule, "The specified intervals had an empty intersection");
//            }
//        }
//
//        final GenomeLocSortedSet excludeSortedSet = IntervalUtils.loadIntervals(excludeIntervalStrings, IntervalSetRule.UNION, intervalMerging, 0, genomeLocParser);
//        if ( excludeSortedSet.contains(GenomeLoc.UNMAPPED) ) {
//            throw new UserException("-XL unmapped is not currently supported");
//        }
//
//        GenomeLocSortedSet intervals;
//        // if no exclude arguments, can return the included set directly
//        if ( excludeSortedSet.isEmpty() ) {
//            intervals = includeSortedSet;
//        }// otherwise there are exclude arguments => must merge include and exclude GenomeLocSortedSets
//        else {
//            intervals = includeSortedSet.subtractRegions(excludeSortedSet);
//
//            if( intervals.isEmpty()){
//                throw new UserException.BadArgumentValue("-L,-XL",getIntervalStrings().toString() + ", "+excludeIntervalStrings.toString(),"The intervals specified for exclusion with -XL removed all territory specified by -L.");
//            }
//            // logging messages only printed when exclude (-XL) arguments are given
//            final long toPruneSize = includeSortedSet.coveredSize();
//            final long toExcludeSize = excludeSortedSet.coveredSize();
//            final long intervalSize = intervals.coveredSize();
//            logger.info(String.format("Initial include intervals span %d loci; exclude intervals span %d loci", toPruneSize, toExcludeSize));
//            logger.info(String.format("Excluding %d loci from original intervals (%.2f%% reduction)",
//                    toPruneSize - intervalSize, (toPruneSize - intervalSize) / (0.01 * toPruneSize)));
//        }
//
//        logger.info(String.format("Processing %d bp from intervals", intervals.coveredSize()));
//
//        // Separate out requests for unmapped records from the rest of the intervals.
//        boolean traverseUnmapped = false;
//        if ( intervals.contains(GenomeLoc.UNMAPPED) ) {
//            traverseUnmapped = true;
//            intervals.remove(GenomeLoc.UNMAPPED);
//        }
//
//        traversalParameters = new TraversalParameters(IntervalUtils.convertGenomeLocsToSimpleIntervals(intervals.toList()), traverseUnmapped);
//    }
//
//
//    /**
//     * Have any intervals been specified for inclusion or exclusion
//     */
//    public boolean intervalsSpecified() {
//        return !( getIntervalStrings().isEmpty() && excludeIntervalStrings.isEmpty());
//    }
}
