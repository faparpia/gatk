package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.net.URI;
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

    @Argument(doc       = "An URI to headerless file where each line contains the absolute path to one raw FASTQ file " +
                            "corresponding to a putative breakpoint",
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
        return true;
    }

    @Override
    public void runTool(final JavaSparkContext ctx){

        final JavaRDD<String> rawFASTQFiles = ctx.textFile(Paths.get(pathToFASTQListFile).toAbsolutePath().toString());
        final JavaPairRDD<Long, URI> seqsArrangedByBreakpoints = rawFASTQFiles.mapToPair(RunSGAViaProcessBuilderOnSpark::assignFASTQToBreakpoints);

        final Path sgaPath = Paths.get(pathToSGA);

        // map paired reads to FASTA file (temp files live in temp dir that cleans self up automatically) then feed to SGA for assembly
        final JavaPairRDD<Long, SGAAssemblyResult> assembledContigs = seqsArrangedByBreakpoints.mapToPair(entry -> performAssembly(entry, sgaPath, threads, runCorrectionSteps));

        validateAndSaveResults(assembledContigs, outputDir);
    }

    /**
     * Converts from a line of text, delimited with a tab, where the first entry is the breakpoint ID and the second entry
     *   is URI to its associated FASTQ file on HDFS.
     * @param recordLine    a line of text containing the breakpoint ID and URI to associated FASTQ file
     * @return              a pair constructed by splitting the string into the ID part and the URI part
     */
    @VisibleForTesting
    static Tuple2<Long, URI> assignFASTQToBreakpoints(final String recordLine){
        final String[] recordForOneBreakPoint = recordLine.split("\t");
        final Long breakpointID = Long.valueOf(recordForOneBreakPoint[0]);
        try{
            final URI fastqURI = new URI(recordForOneBreakPoint[1]);

            return new Tuple2<>(breakpointID, fastqURI);
        }catch (final Exception e){
            return new Tuple2<>(breakpointID, null);
        }
    }

    /**
     * Validates the returned result from running the local assembly pipeline:
     *   if all steps executed successfully, the contig file is nonnull so we save the contig file and discard the runtime information
     *   if any step returns non-zero code, the contig file is null so we save the runtime information for that break point
     * @param results       the local assembly result and its associated breakpoint ID
     * @param outputDir     output directory to save the contigs (if assembly succeeded) or runtime info (if erred)
     */
    private static void validateAndSaveResults(final JavaPairRDD<Long, SGAAssemblyResult> results, final String outputDir){
        results.cache(); // cache because Spark doesn't have an efficient RDD.split(predicate) yet
        final JavaPairRDD<Long, SGAAssemblyResult> success = results.filter(entry -> entry._2().assembledContigs!=null);
        success.mapToPair(entry -> new Tuple2<>(entry._1(), entry._2().assembledContigs))
                .saveAsObjectFile(outputDir);
        final JavaPairRDD<Long, SGAAssemblyResult> failure = results.filter(entry -> entry._2().assembledContigs==null);
        failure.mapToPair(entry -> new Tuple2<>(entry._1(), entry._2().runtimeInformation))
                .saveAsObjectFile(outputDir);
    }

    /**
     * Final return type of the whole process of SGA local assembly.
     * assembledContigFile is the file containing the assembled contigs, if the process executed successfully, or null if not.
     * runtimeInformation contains the runtime information logged along the process up until the process erred, if errors happen,
     *  or until the last step if no errors occur along the line.
     *  The list is organized along the process of executing the assembly pipeline, inside each entry of the list is
     *      the name of the module,
     *      return status of the module,
     *      stdout message of the module, if any,
     *      stderr message of the module, if any
     */
    @VisibleForTesting
    static final class SGAAssemblyResult implements Serializable{
        private static final long serialVersionUID = 1L;

        public final ContigsCollection assembledContigs;
        public final List<Tuple2<String, Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String>>> runtimeInformation;

        public SGAAssemblyResult(final File contigFile, final List<Tuple2<String, Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String>>> collectedRuntimeInfo){
            ContigsCollection proxy = null;
            try{
                proxy = new ContigsCollection(contigFile);
            } catch (final IOException e) { // on IO error caused by converting contigs FASTA file to memory, set in-memory struct to null and append error message
                collectedRuntimeInfo.add(new Tuple2<>(ContigsCollection.class.getName(),
                                                      new Tuple3<>(ExternalCommandlineProgramModule.ReturnStatus.STDIOFAIL, "", e.getMessage())));
            }
            assembledContigs = proxy;
            runtimeInformation = collectedRuntimeInfo;
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
     * @throws IOException          when creating a temporary directory on the local disks for copying the FASTQ file to ,
     *                              or when copying the FASTQ file from HDFS to local disk.
     */
    @VisibleForTesting
    static Tuple2<Long, SGAAssemblyResult> performAssembly(final Tuple2<Long, URI> fastqOfABreakpoint,
                                                           final Path sgaPath,
                                                           final int threads,
                                                           final boolean runCorrections)
            throws IOException{

        final Long breakpointId = fastqOfABreakpoint._1();
        final String pathStringToRawFASTQ = fastqOfABreakpoint._2().getPath();
        final String rawFASTQFileName = FilenameUtils.getName(pathStringToRawFASTQ);

        final File workingDir = Files.createTempDirectory( "assembly" + breakpointId.toString() ).toAbsolutePath().toFile();
        workingDir.deleteOnExit();

        BucketUtils.copyFile(pathStringToRawFASTQ, null, workingDir.toPath().toAbsolutePath().toString()+"/"+rawFASTQFileName);

        final SGAAssemblyResult assembledContigsFileAndRuntimeInfo = runSGAModulesInSerial(sgaPath, workingDir, rawFASTQFileName, threads, runCorrections);

        return new Tuple2<>(breakpointId, assembledContigsFileAndRuntimeInfo);
    }

    /**
     * Linear pipeline for running the SGA local assembly process on a particular FASTQ file for its associated putative breakpoint.
     *
     * @param sgaPath           full path on the executors to the SGA program
     * @param workingDir        directory for SGA to work in (since many files are produced along the process)
     * @param rawFASTQFileName  file name of the FASTQ file, which is assumed to live in workingDir
     * @param threads           threads to be used by SGA modules, if the module supports multithreading
     * @param runCorrections    to run SGA correction steps--correct, filter, rmdup, merge--or not
     * @return                  the result accumulated through running the pipeline, where the contigs could be null if the process erred.
     */
    @VisibleForTesting
    static SGAAssemblyResult runSGAModulesInSerial(final Path sgaPath,
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
        final ArrayList<String> indexerArgs = new ArrayList<>();
        indexerArgs.add("--algorithm"); indexerArgs.add("ropebwt");
        indexerArgs.add("--threads");   indexerArgs.add(Integer.toString(threadsToUse));
        indexerArgs.add("--check");
        indexerArgs.add("");

        // collect runtime information along the way
        final List<Tuple2<String, Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String>>> runtimeInfo = new ArrayList<>();

        String preppedFileName = runAndStopEarly("preprocess", rawFASTQ, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if( null == preppedFileName ){
            return new SGAAssemblyResult(null, runtimeInfo);
        }

        if(runCorrections){// correction, filter, and remove duplicates stringed together
            final File preprocessedFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("correct", preprocessedFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResult(null, runtimeInfo);
            }
            final File correctedFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("filter", correctedFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResult(null, runtimeInfo);
            }
            final File filterPassingFile = new File(workingDir, preppedFileName);

            preppedFileName = runAndStopEarly("rmdup", filterPassingFile, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResult(null, runtimeInfo);
            }
        }

        final File fileToMerge      = new File(workingDir, preppedFileName);
        final String fileNameToAssemble = runAndStopEarly("fm-merge", fileToMerge, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if(null == fileNameToAssemble){
            return new SGAAssemblyResult(null, runtimeInfo);
        }

        final File fileToAssemble   = new File(workingDir, fileNameToAssemble);
        final String contigsFileName = runAndStopEarly("assemble", fileToAssemble, sgaPath, workingDir, threadsToUse, indexer, indexerArgs, runtimeInfo);
        if(null == contigsFileName){
            return new SGAAssemblyResult(null, runtimeInfo);
        }

        // if code reaches here, all steps in the SGA pipeline went smoothly, but the following conversion from File to ContigsCollection may throw IOException
        final File assembledContigsFile = new File(workingDir, contigsFileName);
        return new SGAAssemblyResult(assembledContigsFile, runtimeInfo);
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
     * @param collectiveRuntimeInfo runtime information collected along the process
     * @return                      the name of file produced by running this SGA module
     */
    private static String runAndStopEarly(final String moduleName,
                                          final File inputFASTQFile,
                                          final Path sgaPath,
                                          final File workingDir,
                                          final int threads,
                                          final SGAModule indexer,
                                          final ArrayList<String> indexerArgs,
                                          final List<Tuple2<String, Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String>>> collectiveRuntimeInfo){

        SGAUnitRuntimeInfoStruct thisRuntime = null;
        if(moduleName.equalsIgnoreCase("preprocess")){
            thisRuntime = runSGAPreprocess(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("correct")){
            thisRuntime = runSGACorrect(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("filter")){
            thisRuntime = runSGAFilter(sgaPath, inputFASTQFile, workingDir, threads);
        }else if(moduleName.equalsIgnoreCase("rmdup")){
            thisRuntime = runSGARmDuplicate(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("fm-merge")){
            thisRuntime = runSGAFMMerge(sgaPath, inputFASTQFile, workingDir, threads, indexer, indexerArgs);
        }else if(moduleName.equalsIgnoreCase("assemble")){
            thisRuntime = runSGAOverlapAndAssemble(sgaPath, inputFASTQFile, workingDir, threads);
        }else{
            throw new GATKException("Wrong module called");
        }

        collectiveRuntimeInfo.addAll(thisRuntime.runtimeInfo);

        if( thisRuntime.runtimeInfo.stream().mapToInt(entry -> entry._2()._1().ordinal()).sum()!=0 ){
            return null;
        }else{
            return thisRuntime.fileNameToReturn;
        }
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct runSGAPreprocess(final Path sgaPath,
                                                     final File inputFASTQFile,
                                                     final File outputDirectory,
                                                     final SGAModule indexer,
                                                     final ArrayList<String> indexerArgs){

        final String prefix = FilenameUtils.getBaseName(inputFASTQFile.getName());//extractBaseNameWithoutExtension(inputFASTQFile);
        final String preprocessedFASTAFileName = prefix+".pp.fa";

        final SGAModule preprocess = new SGAModule("preprocess");
        final ArrayList<String> ppArgs = new ArrayList<>();
        ppArgs.add("--pe-mode");    ppArgs.add("2");
        ppArgs.add("--pe-orphans"); ppArgs.add(prefix+".pp.orphan.fa");
        ppArgs.add("--out");        ppArgs.add(preprocessedFASTAFileName);
        ppArgs.add(inputFASTQFile.getName());

        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> ppInfo = preprocess.run(sgaPath, outputDirectory, ppArgs);

        indexerArgs.set(indexerArgs.size()-1, preprocessedFASTAFileName);
        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);

        final SGAUnitRuntimeInfoStruct ppUnitInfo = new SGAUnitRuntimeInfoStruct("preprocess", preprocessedFASTAFileName, ppInfo._1(), ppInfo._2(), ppInfo._3());
        ppUnitInfo.add("index", indexerInfo._1(), indexerInfo._2(), indexerInfo._3());

        return ppUnitInfo;
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct runSGACorrect(final Path sgaPath,
                                                  final File inputFASTAFile,
                                                  final File outputDirectory,
                                                  final int threads,
                                                  final SGAModule indexer,
                                                  final ArrayList<String> indexerArgs){
        return runSimpleModuleFollowedByIndexing(sgaPath, "correct", ".ec.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs);
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct runSGAFilter(final Path sgaPath,
                                                 final File inputFASTAFile,
                                                 final File outputDirectory,
                                                 final int threads){
        final String prefix = FilenameUtils.getBaseName(inputFASTAFile.getName());//extractBaseNameWithoutExtension(inputFASTAFile);
        final SGAModule filter = new SGAModule("filter");
        final ArrayList<String> filterArgs = new ArrayList<>();
        filterArgs.add("--threads"); filterArgs.add(Integer.toString(threads));
        filterArgs.add(prefix+".fa");
        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> filterInfo = filter.run(sgaPath, outputDirectory, filterArgs);

        return new SGAUnitRuntimeInfoStruct("filer", prefix+".filter.pass.fa", filterInfo._1(), filterInfo._2(), filterInfo._3());
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct runSGARmDuplicate(final Path sgaPath,
                                                      final File inputFASTAFile,
                                                      final File outputDirectory,
                                                      final int threads,
                                                      final SGAModule indexer,
                                                      final ArrayList<String> indexerArgs){
        return runSimpleModuleFollowedByIndexing(sgaPath, "rmdup", ".rmdup.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs);
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct runSGAFMMerge(final Path sgaPath,
                                                  final File inputFASTAFile,
                                                  final File outputDirectory,
                                                  final int threads,
                                                  final SGAModule indexer,
                                                  final ArrayList<String> indexerArgs){
        return runSimpleModuleFollowedByIndexing(sgaPath, "fm-merge", ".merged.fa", inputFASTAFile, outputDirectory, threads, indexer, indexerArgs);
    }

    @VisibleForTesting
    static SGAUnitRuntimeInfoStruct runSGAOverlapAndAssemble(final Path sgaPath,
                                                             final File inputFASTAFile,
                                                             final File outputDirectory,
                                                             final int threads){

        final SGAModule overlap = new SGAModule("overlap");
        final ArrayList<String> overlapArgs = new ArrayList<>();
        overlapArgs.add("--threads"); overlapArgs.add(Integer.toString(threads));
        overlapArgs.add(inputFASTAFile.getName());
        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> overlapInfo = overlap.run(sgaPath, outputDirectory, overlapArgs);

        final String prefix = FilenameUtils.getBaseName(inputFASTAFile.getName());//extractBaseNameWithoutExtension(inputFASTAFile);

        final SGAModule assemble = new SGAModule("assemble");
        final ArrayList<String> assembleArgs = new ArrayList<>();
        assembleArgs.add("--out-prefix"); assembleArgs.add(prefix);
        assembleArgs.add(prefix+".asqg.gz");
        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> assembleInfo = assemble.run(sgaPath, outputDirectory, assembleArgs);

        final SGAUnitRuntimeInfoStruct assembleUnitInfo = new SGAUnitRuntimeInfoStruct("assemble", prefix+"-contigs.fa",
                                                                                        assembleInfo._1(), assembleInfo._2(), assembleInfo._3());

        assembleUnitInfo.add("overlap", overlapInfo._1(), overlapInfo._2(), overlapInfo._3());
        return assembleUnitInfo;
    }

    // boiler plate code for running simple sga modules (simple in the sense that no options needs to be specified to make it work)
    // that perform a task followed by indexing its output
    private static SGAUnitRuntimeInfoStruct runSimpleModuleFollowedByIndexing(final Path sgaPath,
                                                                              final String moduleName,
                                                                              final String extensionToAppend,
                                                                              final File inputFASTAFile,
                                                                              final File outputDirectory,
                                                                              final int threads,
                                                                              final SGAModule indexer,
                                                                              final ArrayList<String> indexerArgs){

        final SGAModule module = new SGAModule(moduleName);
        final ArrayList<String> args = new ArrayList<>();
        args.add("--threads");   args.add(Integer.toString(threads));
        args.add(inputFASTAFile.getName());
        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> moduleInfo = module.run(sgaPath, outputDirectory, args);

        final String outputFileName = FilenameUtils.getBaseName(inputFASTAFile.getName()) + extensionToAppend;//extractBaseNameWithoutExtension(inputFASTAFile) + extensionToAppend;

        final SGAUnitRuntimeInfoStruct unitInfo = new SGAUnitRuntimeInfoStruct(moduleName, outputFileName, moduleInfo._1(), moduleInfo._2(), moduleInfo._3());

        indexerArgs.set(indexerArgs.size()-1, outputFileName);
        final Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String> indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);
        unitInfo.add("index", indexerInfo._1(), indexerInfo._2(), indexerInfo._3());

        return unitInfo;
    }

    // From https://www.stackoverflow.com/questions/4545937
//    @VisibleForTesting
//    static String extractBaseNameWithoutExtension(final File file){
//        final String[] tokens = file.getName().split("\\.(?=[^\\.]+$)");
//        return tokens[0];
//    }

    /**
     * A struct to represent the runtime information returned by running a unit of sga modules.
     * Because some of the modules are designed to be run together in serial form in a unit (e.g. correction followed by indexing)
     *   all return status and stdout stderr messages in this unit are logged.
     * But only one file name is important for consumption by downstream units/modules, so only that file name is kept.
     * The ctor and the add() method are designed asymmetrically, because when a unit is consisted of multiple modules,
     *   only one generates the file name to be returned, and the other modules are simply to be logged.
     */
    @VisibleForTesting
    static final class SGAUnitRuntimeInfoStruct implements Serializable{
        private static final long serialVersionUID = 1L;

        public final String fileNameToReturn;
        public final List<Tuple2<String, Tuple3<ExternalCommandlineProgramModule.ReturnStatus, String, String>>> runtimeInfo;

        public SGAUnitRuntimeInfoStruct(final String moduleName,
                                        final String fileName,
                                        final ExternalCommandlineProgramModule.ReturnStatus returnStatus,
                                        final String stdoutMessage,
                                        final String stderrMessage){
            fileNameToReturn = fileName;
            runtimeInfo = new ArrayList<>();
            add(moduleName, returnStatus, stdoutMessage, stderrMessage);
        }

        public void add(final String moduleName, final ExternalCommandlineProgramModule.ReturnStatus returnStatus, final String stdoutMessage, final String stderrMessage){
            runtimeInfo.add(new Tuple2<>(moduleName, new Tuple3<>(returnStatus, stdoutMessage, stderrMessage)));
        }
    }

    /**
     * Represents a collection of assembled contigs in the final output of "sga assemble".
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

        private List<Tuple2<ContigID, ContigSequence>> contents;

        public List<Tuple2<ContigID, ContigSequence>> getContents(){
            return contents;
        }

        public ContigsCollection(final File fastaFile) throws IOException{

            if(null==fastaFile){// early return if the fastaFile is null, which signals something went wrong in sga pipeline
                throw new IOException(fastaFile.getName() + "is null");
            }

            final List<String> lines = Files.readAllLines(Paths.get(fastaFile.getAbsolutePath()));

            contents = new ArrayList<>();
            for(int i=0; i<lines.size(); i+=2){
                contents.add(new Tuple2<>(new ContigID(lines.get(i)), new ContigSequence(lines.get(i+1))));
            }
        }
    }
}