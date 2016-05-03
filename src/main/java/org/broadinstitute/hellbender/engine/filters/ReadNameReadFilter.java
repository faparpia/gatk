package org.broadinstitute.hellbender.engine.filters;

import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ReadFilterArgument;
import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * Keep only reads with this read name.
 * Matching is done by case-sensitive exact match.
 */
@ReadFilterArgument(
        fullName = "Readname",
        shortName = "rn"
)
public final class ReadNameReadFilter implements ReadFilter {
    private static final long serialVersionUID = 1L;

    @Argument(fullName = "readName", shortName = "rn", doc="Keep only reads with this read name", optional=false)
    public String readName;

    @Override
    public boolean test( final GATKRead read ) {
        return read.getName() != null && read.getName().equals(readName);
    }
}
