package org.broadinstitute.hellbender.tools.spark.sv;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Useful scraps of this and that.
 */
public final class SVUtils {

    /**
     * return a good initialCapacity for a HashMap that will hold a given number of elements
     */
    public static int hashMapCapacity( final int nElements )
    {
        return (int)((nElements*4L)/3) + 1;
    }

    /**
     * Iterator over 1 or 0 elements.
     */
    public static final class SingletonIterator<T> implements Iterator<T> {
        private T value;

        SingletonIterator() { this.value = null; }
        SingletonIterator( final T value ) { this.value = value; }

        @Override
        public boolean hasNext() { return value != null; }

        @Override
        public T next() {
            if ( value == null ) throw new NoSuchElementException("Singleton iterator exhausted.");
            final T result = value;
            value = null;
            return result;
        }
    }

    /**
     * Kinda, sorta like Scala Option: an Iterable over 1 or 0 elements.
     */
    public static final class SingletonIterable<T> implements Iterable<T> {
        private final T value;

        SingletonIterable() { this.value = null; }
        SingletonIterable( final T value ) { this.value = value; }

        @Override
        public Iterator<T> iterator() { return new SingletonIterator<>(value); }
    }

    public static <T> int iteratorSize( final Iterator<T> itr ) {
        int result = 0;
        while ( itr.hasNext() ) { result += 1; itr.next(); }
        return result;
    }
}
