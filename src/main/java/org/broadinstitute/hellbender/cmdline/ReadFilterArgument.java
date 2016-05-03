package org.broadinstitute.hellbender.cmdline;

import java.lang.annotation.*;

    import java.lang.annotation.Documented;
    import java.lang.annotation.ElementType;
    import java.lang.annotation.Retention;
    import java.lang.annotation.RetentionPolicy;
    import java.lang.annotation.Target;

/**
 * Used to annotate a class which can be specified as a command line read filter argument
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface ReadFilterArgument {

    /**
     * The full name of the read filter.
     * @return Selected full name.
     */
    String fullName() default "";

    /**
     * Specified short name of the read filter.
     * @return Selected short name.
     */
    String shortName() default "";

}
