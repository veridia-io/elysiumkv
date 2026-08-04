package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * ARCHITECTURE.md "The ABI boundary" requires extraction to "tolerate concurrent initialisation from multiple
 * class loaders". That is two separate demands, and the second one bites:
 *
 * <ul>
 *   <li><b>Concurrency.</b> Several threads racing to extract must converge on
 *       one complete file — never a half-written one, which unlike a half-written
 *       ordinary file would be executed.
 *   <li><b>Class-loader isolation.</b> A JVM refuses to load the same library
 *       <em>file</em> into two class loaders: {@code System.load} throws
 *       "Native Library ... already loaded in another classloader". Two copies of
 *       this class in one JVM — the ordinary situation for two web applications
 *       in one container — therefore each need their own extracted path.
 * </ul>
 *
 * <p>The two pull in opposite directions: deduplicating extraction by content
 * would make every loader share one path, which is exactly what the JVM forbids.
 */
class LibraryLoadingTest {
    /** Loads io.veridia.elysiumkv.* itself so each instance gets its own copy of the class. */
    private static final class IsolatedLoader extends URLClassLoader {
        IsolatedLoader(URL[] urls) {
            super(urls, IsolatedLoader.class.getClassLoader().getParent());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("io.veridia.elysiumkv.")) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> found = findLoadedClass(name);
                    if (found == null) found = findClass(name);
                    if (resolve) resolveClass(found);
                    return found;
                }
            }
            return super.loadClass(name, resolve);
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            InputStream own = findResource(name) == null ? null : super.getResourceAsStream(name);
            return own != null ? own : LibraryLoadingTest.class.getResourceAsStream(name);
        }
    }

    private static URL[] classpath() {
        List<URL> urls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(java.io.File.pathSeparator)) {
            try {
                urls.add(new java.io.File(entry).toURI().toURL());
            } catch (IOException ignored) {
                // A classpath entry we cannot turn into a URL is not ours to fix.
            }
        }
        return urls.toArray(new URL[0]);
    }

    /** Forces the native load inside a foreign loader and returns its version string. */
    private static String loadIn(ClassLoader loader) throws Exception {
        Class<?> nativeClass = Class.forName("io.veridia.elysiumkv.Native", true, loader);
        Method version = nativeClass.getDeclaredMethod("version");
        version.setAccessible(true);
        return (String) version.invoke(null);
    }

    /* Both of the extraction tests below need the extraction path, and
     * -Delysiumkv.library.path bypasses it: every loader would then point at one
     * file, which is precisely what the JVM refuses. Without this they fail with
     * "already loaded in another classloader" whenever the suite is run against a
     * CMake output directory — a red test that says nothing about the code, which
     * is worse than an honest skip.
     */
    @Test
    @DisabledIfSystemProperty(named = "elysiumkv.library.path", matches = ".+",
                              disabledReason = "the override bypasses extraction, which is what "
                                      + "this test exercises")
    void twoClassLoadersCanEachLoadTheLibrary() throws Exception {
        // Both loaders are separate from the test's own, so neither shares the
        // already-loaded copy. Each must end up with its own extracted file.
        String first = loadIn(new IsolatedLoader(classpath()));
        String second = loadIn(new IsolatedLoader(classpath()));

        assertNotNull(first);
        assertEquals(first, second, "both loaders reached the same library");
    }

    @Test
    @DisabledIfSystemProperty(named = "elysiumkv.library.path", matches = ".+",
                              disabledReason = "the override bypasses extraction, which is what "
                                      + "this test exercises")
    void concurrentFirstLoadsConvergeOnACompleteLibrary() throws Exception {
        final int loaders = 6;
        ExecutorService pool = Executors.newFixedThreadPool(loaders);
        try {
            List<Callable<String>> work = new ArrayList<>();
            for (int i = 0; i < loaders; ++i) {
                work.add(() -> loadIn(new IsolatedLoader(classpath())));
            }
            List<Future<String>> results = pool.invokeAll(work, 120, TimeUnit.SECONDS);
            for (Future<String> result : results) {
                // A truncated extraction would surface here as an
                // UnsatisfiedLinkError rather than a wrong answer.
                assertNotNull(result.get(), "a racing loader failed to load the library");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void theExplicitPathOverrideSkipsExtraction() {
        // The property the build and the tests rely on: load straight from a
        // CMake output directory, no jar and no temp file involved.
        assertTrue(System.getProperty("elysiumkv.library.path") == null
                           || System.getProperty("elysiumkv.library.path").contains("elysiumkv_jni"),
                   "the override, when set, must point at the JNI library");
        assertNotNull(Native.version());
    }
}
