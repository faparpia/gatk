package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

// TODO: choose which parameters allowed to be tunable
// TODO: if throws, would temp files be cleaned up automatically?
// TODO: most robust way to check OS in method runSGAModulesInSerial (Mac doesn't support multi-thread mechanism in SGA)?
@CommandLineProgramProperties(
        summary        = "Program to call SGA to perform local assembly and return assembled contigs.",
        oneLineSummary = "Perform SGA-based local assembly on fasta files on Spark",
        programGroup   = StructuralVariationSparkProgramGroup.class)
public final class RunSGAViaProcessBuilderOnSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc       = "Absolute path to SGA installation.",
              shortName = "sgaPath",
              fullName  = "fullPathToSGA",
              optional  = false)
    public String pathToSGA = null;

    @Argument(doc       = "An URI to header-less file where each line contains a breakpoint ID and " +
                            "the absolute path to one raw FASTQ file delimited by a tab",
              shortName = "fl",
              fullName  = "fastqList",
              optional  = false)
    public String pathToFASTQListFile = null;

    @Argument(doc       = "A path to a directory to write results to.",
              shortName = "outDir",
              fullName  = "outputDirectory",
              optional  = false)
    public String outputDir = null;

    @Argument(doc       = "Number of threads to use when running sga.",
              shortName = "t",
              fullName  = "threads",
              optional  = true)
    public int threads = 1;

    @Argument(doc       = "To run k-mer based read correction, filter and duplication removal in SGA or not, with default parameters.",
              shortName = "correct",
              fullName  = "correctNFilter",
              optional  = true)
    public boolean runCorrectionSteps = false;

    @Override
    public boolean requiresReads(){
        return false;
    }

    @Override
    public void runTool(final JavaSparkContext ctx){

        final Path sgaPath = Paths.get(pathToSGA);

        // IO (files) preparation
        FileSystem proxy = null;
        try{
            proxy = FileSystem.get( ctx.hadoopConfiguration() );
        }catch (final IOException ex){
            throw new UserException("Cannot get configuration."); // Any better exception?
        }
        final FileSystem fs = proxy;

        // first load RDD of pair that has breakpoint ID as its first and URI to FASTQ file as its second
        final JavaRDD<String> rawFASTQFiles = ctx.textFile(Paths.get(pathToFASTQListFile).toAbsolutePath().toString());
        final JavaPairRDD<Long, URI> seqsArrangedByBreakpoints = rawFASTQFiles.mapToPair(RunSGAViaProcessBuilderOnSpark::assignFASTQToBreakpoints);
        seqsArrangedByBreakpoints.filter(pair -> (pair._2()!=null));

        // distribute/copy FASTA file to local disks and perform assembly (temp files live in temp dir that cleans self up automatically)
        final JavaPairRDD<Long, PipeLineResult> assembledContigs = seqsArrangedByBreakpoints.mapToPair(entry -> performAssembly(fs, entry, sgaPath, threads, runCorrectionSteps));

        // validate the results returned: save FASTA file for breakpoints that successfully assembled, or save error messages it somewhere along the process things went wrong.
        validateAndSaveResults(assembledContigs, outputDir);
    }

    /**
     * Validates the returned result from running the local assembly pipeline:
     *   if all steps executed successfully, the contig file is nonnull so we save the contig file and discard the runtime information
     *   if any sga step returns non-zero code, the contig file is null so we save the runtime information for that break point
     *   if any non-SGA steps erred, save the error message logged during the step.
     * @param results       the local assembly result and its associated breakpoint ID
     * @param outputDir     output directory to save the contigs (if assembly succeeded) or runtime info (if erred)
     */
    private static void validateAndSaveResults(final JavaPairRDD<Long, PipeLineResult> results, final String outputDir){

        results.cache(); // cache because Spark doesn't have an efficient RDD.split(predicate) yet

        final JavaPairRDD<Long, PipeLineResult> withoutNonSGAErrors = results.filter(entry -> entry._2().nonSGAStepsErrorMessages==null);

        // everything went fine
        final JavaPairRDD<Long, PipeLineResult> success = withoutNonSGAErrors.filter(entry -> entry._2().sgaStepsResult.assembledContigs!=null);
        success.mapToPair(entry -> new Tuple2<>(entry._1(), entry._2().sgaStepsResult.assembledContigs))
                .saveAsObjectFile(outputDir);

        // sga parts failed
        final JavaPairRDD<Long, PipeLineResult> sgaFailure = withoutNonSGAErrors.filter(entry -> entry._2().sgaStepsResult.assembledContigs==null);
        sgaFailure.mapToPair(entry -> new Tuple2<>(entry._1(), entry._2().sgaStepsResult.collectiveRuntimeInfo))
                .saveAsObjectFile(outputDir);

        // nonSGA parts failed
        final JavaPairRDD<Long, PipeLineResult> withNonSGAErrors = results.filter(entry -> entry._2().nonSGAStepsErrorMessages!=null);
        withNonSGAErrors.mapToPair(entry -> new Tuple2<>(entry._1(), entry._2().nonSGAStepsErrorMessages))
                .saveAsObjectFile(outputDir);
    }

    /**
     * Converts from a line of text, delimited with a tab, where the first entry is the breakpoint ID and the second entry
     *   is URI to its associated FASTQ file on HDFS.
     * @param recordLine    a line of text containing the breakpoint ID and URI to associated FASTQ file
     * @return              a pair constructed by splitting the string into the ID part and the URI part
     *                      the URI part will be null if cannot be parsed
     */
    @VisibleForTesting
    static Tuple2<Long, URI> assignFASTQToBreakpoints(final String recordLine){
        final String[] recordForOneBreakPoint = recordLine.split("\t");
        final Long breakpointID = Long.valueOf(recordForOneBreakPoint[0]);
        try{
            final URI fastqURI = new URI(recordForOneBreakPoint[1]);
            return new Tuple2<>(breakpointID, fastqURI);
        }catch (final URISyntaxException e){
            return new Tuple2<>(breakpointID, null);
        }
    }

    /**
     * Performs assembly on the FASTA files pointed to by the URI that is associated with the breakpoint identified by the long ID.
     * Actual assembly work is delegated to other functions.
     * @param fastqOfABreakpoint    the breakpoint ID and URI to the FASTQ file
     * @param sgaPath               full path to SGA
     * @param threads               number of threads to use in various modules, if the module supports parallelism
     * @param runCorrections        user's decision to run SGA's corrections (with default parameter values) or not
     * @return                      contig file (if process succeed) and runtime information, associated with the breakpoint ID
     */
    @VisibleForTesting
    static Tuple2<Long, PipeLineResult> performAssembly(final FileSystem fs,
                                                        final Tuple2<Long, URI> fastqOfABreakpoint,
                                                        final Path sgaPath,
                                                        final int threads,
                                                        final boolean runCorrections){

        final Long breakpointID = fastqOfABreakpoint._1();
        final URI  uriToFASTQ   = fastqOfABreakpoint._2();
        if(null==uriToFASTQ){
            return new Tuple2<>(breakpointID, new PipeLineResult("Failed to parse URI for breakpoint: " + breakpointID.toString()));
        }

        final Tuple2<File, String> copySuccess = makeTempDirAndCopyFASTQToLocal(fs, uriToFASTQ, breakpointID);
        final File workingDir = copySuccess._1();
        if(workingDir!=null){
            final PipeLineResult assembledContigsFileAndRuntimeInfo = runSGAModulesInSerial(sgaPath, workingDir, copySuccess._2(), threads, runCorrections);
            return new Tuple2<>(breakpointID, assembledContigsFileAndRuntimeInfo);
        }else{
            return new Tuple2<>(breakpointID, new PipeLineResult(copySuccess._2()));
        }
    }

    /**
     * Copy from hdfs to temp local dir
     * @param fs            filesystem
     * @param uriToFASTQ    uri to source
     * @param breakpointID  breakpoint ID
     * @return              (temp directory, FASTQ file name) if successfully copied, or (null, error message) if failed
     */
    @VisibleForTesting
    static Tuple2<File, String> makeTempDirAndCopyFASTQToLocal(final FileSystem fs, final URI uriToFASTQ, final Long breakpointID){

        try{
            final File workingDir = Files.createTempDirectory( "assembly" + breakpointID.toString() ).toAbsolutePath().toFile();
            workingDir.deleteOnExit();

            final String rawFASTQFileName = FilenameUtils.getName(uriToFASTQ.getPath());
            final org.apache.hadoop.fs.Path from = new org.apache.hadoop.fs.Path(uriToFASTQ);
            final org.apache.hadoop.fs.Path to   = new org.apache.hadoop.fs.Path(workingDir.toPath().toAbsolutePath().toString()+"/"+rawFASTQFileName);
            fs.copyToLocalFile(from, to);
            return new Tuple2<>(workingDir, rawFASTQFileName);
        } catch (final IOException ex){ // either failed to create temp dir or failed to copy
            return new Tuple2<>(null, ex.getMessage());
        }
    }

    /**
     * Linear pipeline for running the SGA local assembly process on a particular FASTQ file for its associated putative breakpoint.
     *
     * @param sgaPath           full path on the executors to the SGA program
     * @param workingDir        directory for SGA to work in (since many files are produced along the process)
     * @param rawFASTQFileName  file name of the FASTQ file in workingDir
     * @param threads           threads to be used by SGA modules, if the module supports multithreading
     * @param runCorrections    to run SGA correction steps--correct, filter, rmdup, merge--or not
     * @return                  the result accumulated through running the pipeline, where the contigs could be null if the process erred.
     */
    @VisibleForTesting
    static PipeLineResult runSGAModulesInSerial(final Path sgaPath,
                                                final File workingDir,
                                                final String rawFASTQFileName,
                                                final int threads,
                                                final boolean runCorrections){

        int threadsToUse = threads;
        if( System.getProperty("os.name").toLowerCase().contains("mac") && threads>1){
            System.err.println("Running on Mac OS X, which doesn't provide unnamed semaphores used by SGA for multithreading. " +
                               "Resetting threads argument to 1. ");
            threadsToUse = 1;
        }

        final File rawFASTQ = new File(workingDir, rawFASTQFileName);

        // the index module is used frequently, so make single instance and pass around
        final SGAModule indexer = new SGAModule("index");
        final List<String> indexerArgs = new ArrayList<>();
        indexerArgs.add("--algorithm"); indexerArgs.add("ropebwt");
        indexerArgs.add("--threads");   indexerArgs.add(Integer.toString(threadsToUse));
        indexerArgs.add("--check");
        indexerArgs.add("");

        // collect runtime information along the way
        final List<SGAModule.RuntimeInfo> runtimeInfo = new ArrayList<>();

        String preppedFileName = runAndStopEarly("preprocess", rawFASTQ, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if( null == preppedFileName ){
            final SGAAssemblyResult sgaErred = new SGAAssemblyResult(null, runtimeInfo);
            return new PipeLineResult(sgaErred);
        }

        if(runCorrections){// correction, filter, and remove duplicates stringed together
            final File preprocessedFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("correct", preprocessedFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                final SGAAssemblyResult sgaErred = new SGAAssemblyResult(null, runtimeInfo);
                return new PipeLineResult(sgaErred);
            }
            final File correctedFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("filter", correctedFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                final SGAAssemblyResult sgaErred = new SGAAssemblyResult(null, runtimeInfo);
                return new PipeLineResult(sgaErred);
            }
            final File filterPassingFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("rmdup", filterPassingFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                final SGAAssemblyResult sgaErred = new SGAAssemblyResult(null, runtimeInfo);
                return new PipeLineResult(sgaErred);
            }
        }

        final File fileToMerge      = new File(workingDir, preppedFileName);
        final String fileNameToAssemble = runAndStopEarly("fm-merge", fileToMerge, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if(null == fileNameToAssemble){
            final SGAAssemblyResult sgaErred = new SGAAssemblyResult(null, runtimeInfo);
            return new PipeLineResult(sgaErred);
        }

        final File fileToAssemble   = new File(workingDir, fileNameToAssemble);
        final String contigsFileName = runAndStopEarly("assemble", fileToAssemble, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if(null == contigsFileName){
            final SGAAssemblyResult sgaErred = new SGAAssemblyResult(null, runtimeInfo);
            return new PipeLineResult(sgaErred);
        }

        // if code reaches here, all steps in the SGA pipeline went smoothly,
        // but the following conversion from File to ContigsCollection may still err
        final File assembledContigsFile = new File(workingDir, contigsFileName);
        try{
            final List<String> contigsFASTAContents = (null==assembledContigsFile) ? null : Files.readAllLines(Paths.get(assembledContigsFile.getAbsolutePath()));
            final SGAAssemblyResult sgaResults =  new SGAAssemblyResult(contigsFASTAContents, runtimeInfo);
            return new PipeLineResult(sgaResults);
        }catch(final IOException ex){ // failed to parse contigs file
            return new PipeLineResult("Successfully executed sga processes, but failed to parse the resulting FASTA file.");
        }
    }

    /**
     * Call the right sga module, log runtime information, and return the output file name if succeed.
     * If process erred, the string returned is null.
     * @param moduleName            SGA module name to be run
     * @param inputFASTQFile        FASTQ file tobe fed to SGA modul
     * @param sgaPath               full path to the SGA program
     * @param workingDir            directory the SGA pipeline is working in
     * @param threads               threads to use
     * @param indexer               module representing SGA index
     * @param indexerArgs           arguments used by SGA index
     * @param collectedRuntimeInfo  runtime information collected along the process
     * @return                      the name of file produced by running this SGA module
     */
    private static String runAndStopEarly(final String moduleName,
                                          final File inputFASTQFile,
                                          final Path sgaPath,
                                          final File workingDir,
                                          final int threads,
                                          final SGAModule indexer,
                                          final List<String> indexerArgs,
                                          final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        String filenameToReturn = null;
        if(moduleName.equalsIgnoreCase("preprocess")){
            filenameToReturn = runSGAPreprocess(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("correct")){
            filenameToReturn = runSGACorrect(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("filter")){
            filenameToReturn = runSGAFilter(sgaPath, inputFASTQFile, workingDir, threads, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("rmdup")){
            filenameToReturn = runSGARmDuplicate(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("fm-merge")){
            filenameToReturn = runSGAFMMerge(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("assemble")){
            filenameToReturn = runSGAOverlapAndAssemble(sgaPath, inputFASTQFile, workingDir, threads, collectedRuntimeInfo);
        }else{
            throw new GATKException("Wrong module called"); // should never occur, implementation mistake
        }

        final SGAModule.RuntimeInfo.ReturnStatus returnStatus = collectedRuntimeInfo.get(collectedRuntimeInfo.size()-1).returnStatus;

        if(!(returnStatus.equals( SGAModule.RuntimeInfo.ReturnStatus.SUCCESS))){
            return null;
        }else{
            return filenameToReturn;
        }
    }

    @VisibleForTesting
    static String runSGAPreprocess(final Path sgaPath,
                                   final File inputFASTQFile,
                                   final File outputDirectory,
                                   final SGAModule indexer,
                                   final List<String> indexerArgs,
                                   final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final String prefix = FilenameUtils.getBaseName(inputFASTQFile.getName());
        final String preprocessedFASTAFileName = prefix+".pp.fa";

        final SGAModule preprocess = new SGAModule("preprocess");
        final List<String> ppArgs = new ArrayList<>();
        ppArgs.add("--pe-mode");    ppArgs.add("2");
        ppArgs.add("--pe-orphans"); ppArgs.add(prefix+".pp.orphan.fa");
        ppArgs.add("--out");        ppArgs.add(preprocessedFASTAFileName);
        ppArgs.add(inputFASTQFile.getName());

        final SGAModule.RuntimeInfo ppInfo = preprocess.run(sgaPath, outputDirectory, ppArgs);
        collectedRuntimeInfo.add(ppInfo);

        indexerArgs.set(indexerArgs.size()-1, preprocessedFASTAFileName);
        final SGAModule.RuntimeInfo indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);
        collectedRuntimeInfo.add(indexerInfo);

        return preprocessedFASTAFileName;
    }

    @VisibleForTesting
    static String runSGACorrect(final Path sgaPath,
                                final File inputFASTAFile,
                                final File outputDirectory,
                                final int threads,
                                final SGAModule indexer,
                                final List<String> indexerArgs,
                                final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
        return runSimpleModuleFollowedByIndexing(sgaPath, "correct", ".ec.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs, collectedRuntimeInfo);
    }

    @VisibleForTesting
    static String runSGAFilter(final Path sgaPath,
                               final File inputFASTAFile,
                               final File outputDirectory,
                               final int threads,
                               final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final String prefix = FilenameUtils.getBaseName(inputFASTAFile.getName());
        final SGAModule filter = new SGAModule("filter");
        final List<String> filterArgs = new ArrayList<>();
        filterArgs.add("--threads"); filterArgs.add(Integer.toString(threads));
        filterArgs.add(prefix+".fa");
        final SGAModule.RuntimeInfo filterInfo = filter.run(sgaPath, outputDirectory, filterArgs);
        collectedRuntimeInfo.add(filterInfo);

        return prefix+".filter.pass.fa";
    }

    @VisibleForTesting
    static String runSGARmDuplicate(final Path sgaPath,
                                    final File inputFASTAFile,
                                    final File outputDirectory,
                                    final int threads,
                                    final SGAModule indexer,
                                    final List<String> indexerArgs,
                                    final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
        return runSimpleModuleFollowedByIndexing(sgaPath, "rmdup", ".rmdup.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs, collectedRuntimeInfo);
    }

    @VisibleForTesting
    static String runSGAFMMerge(final Path sgaPath,
                                final File inputFASTAFile,
                                final File outputDirectory,
                                final int threads,
                                final SGAModule indexer,
                                final List<String> indexerArgs,
                                final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
        return runSimpleModuleFollowedByIndexing(sgaPath, "fm-merge", ".merged.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs, collectedRuntimeInfo);
    }

    @VisibleForTesting
    static String runSGAOverlapAndAssemble(final Path sgaPath,
                                           final File inputFASTAFile,
                                           final File outputDirectory,
                                           final int threads,
                                           final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final SGAModule overlap = new SGAModule("overlap");
        final List<String> overlapArgs = new ArrayList<>();
        overlapArgs.add("--threads"); overlapArgs.add(Integer.toString(threads));
        overlapArgs.add(inputFASTAFile.getName());

        final SGAModule.RuntimeInfo overlapInfo = overlap.run(sgaPath, outputDirectory, overlapArgs);
        collectedRuntimeInfo.add(overlapInfo);

        final String prefix = FilenameUtils.getBaseName(inputFASTAFile.getName());

        final SGAModule assemble = new SGAModule("assemble");
        final List<String> assembleArgs = new ArrayList<>();
        assembleArgs.add("--out-prefix"); assembleArgs.add(prefix);
        assembleArgs.add(prefix+".asqg.gz");
        final SGAModule.RuntimeInfo assembleInfo = assemble.run(sgaPath, outputDirectory, assembleArgs);
        collectedRuntimeInfo.add(assembleInfo);

        return prefix+"-contigs.fa";
    }

    // boiler plate code for running simple sga modules (simple in the sense that no options needs to be specified to make it work)
    // that perform a task followed by indexing its output
    private static String runSimpleModuleFollowedByIndexing(final Path sgaPath,
                                                            final String moduleName,
                                                            final String extensionToAppend,
                                                            final File inputFASTAFile,
                                                            final File outputDirectory,
                                                            final int threads,
                                                            final SGAModule indexer,
                                                            final List<String> indexerArgs,
                                                            final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final SGAModule module = new SGAModule(moduleName);
        final List<String> args = new ArrayList<>();
        args.add("--threads");   args.add(Integer.toString(threads));
        args.add(inputFASTAFile.getName());

        final SGAModule.RuntimeInfo moduleInfo = module.run(sgaPath, outputDirectory, args);
        collectedRuntimeInfo.add(moduleInfo);

        final String outputFileName = FilenameUtils.getBaseName(inputFASTAFile.getName()) + extensionToAppend;

        indexerArgs.set(indexerArgs.size()-1, outputFileName);
        final SGAModule.RuntimeInfo indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);
        collectedRuntimeInfo.add(indexerInfo);

        return outputFileName;
    }

    /**
     * Final return type of the whole process of SGA local assembly.
     * assembledContigFile is the file containing the assembled contigs, if the process executed successfully, or null if not.
     * runtimeInformation contains the runtime information logged along the process up until the process erred, if errors happen,
     *   or until the last step if no errors occur along the line.
     *   The list is organized along the process of executing the assembly pipeline.
     */
    @VisibleForTesting
    static final class SGAAssemblyResult implements Serializable{
        private static final long serialVersionUID = 1L;

        public final ContigsCollection assembledContigs;
        public final List<SGAModule.RuntimeInfo> collectiveRuntimeInfo;

        public SGAAssemblyResult(final List<String> fastaContents, final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
            this.assembledContigs      = new ContigsCollection(fastaContents);
            this.collectiveRuntimeInfo = collectedRuntimeInfo;
        }
    }

    /**
     * Representing results on the whole calling SGA on spark pipeline.
     * If nonSGAStepsErrorMessages is non-null, then sgaStepsResult must be null.
     *   This could be because the pipeline erred before SGA modules are called, or erred after SGA modules successfully
     *   did their job but somewhere else along the pipeline things went wrong.
     * This class constructed with a nonSGA related String message is used to store the error message.
     */
    static final class PipeLineResult implements Serializable{
        private static final long serialVersionUID = 1L;

        public final SGAAssemblyResult sgaStepsResult;
        public final String nonSGAStepsErrorMessages;

        public PipeLineResult(final SGAAssemblyResult sgaStepsResult){
            this.sgaStepsResult = sgaStepsResult;
            nonSGAStepsErrorMessages = null;
        }

        public PipeLineResult(final String nonSGAErrorMsg){
            this.sgaStepsResult = null;
            this.nonSGAStepsErrorMessages = nonSGAErrorMsg;
        }
    }

    /**
     * Represents a collection of assembled contigs (not including the variants) produced by "sga assemble".
     */
    @VisibleForTesting
    static final class ContigsCollection implements Serializable{
        private static final long serialVersionUID = 1L;

        @VisibleForTesting
        static final class ContigSequence implements Serializable{
            private static final long serialVersionUID = 1L;

            private final String sequence;
            public ContigSequence(final String sequence){ this.sequence = sequence; }
            public String getSequenceAsString(){ return sequence; }
        }

        @VisibleForTesting
        static final class ContigID implements Serializable{
            private static final long serialVersionUID = 1L;

            private final String id;
            public ContigID(final String idString) { this.id = idString; }
            public String getId() { return id; }
        }

        private final List<Tuple2<ContigID, ContigSequence>> contents;

        public List<Tuple2<ContigID, ContigSequence>> getContents(){
            return contents;
        }

        public ContigsCollection(final List<String> fileContents){

            contents = new ArrayList<>();
            for(int i=0; i<fileContents.size(); i+=2){
                contents.add(new Tuple2<>(new ContigID(fileContents.get(i)), new ContigSequence(fileContents.get(i+1))));
            }
        }
    }
}