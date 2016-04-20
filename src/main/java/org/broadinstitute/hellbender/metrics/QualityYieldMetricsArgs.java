package org.broadinstitute.hellbender.metrics;

import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;

/**
 * MetricsArgs argument collection for QualityYield metrics. All members should be
 * instantiable as command line arguments.
 */
public class QualityYieldMetricsArgs extends MetricsArgs {

    @Argument(shortName = StandardArgumentDefinitions.USE_ORIGINAL_QUALITIES_SHORT_NAME,
            fullName = StandardArgumentDefinitions.USE_ORIGINAL_QUALITIES_LONG_NAME,
            doc = "If available in the OQ tag, use the original quality scores " +
                    "as inputs instead of the quality scores in the QUAL field.")
    public boolean useOriginalQualities = false;

}
