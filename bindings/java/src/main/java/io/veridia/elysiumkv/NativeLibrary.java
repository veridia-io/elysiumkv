package io.veridia.elysiumkv;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;

/**
 * Finds and loads {@code libelysiumkv_jni}.
 *
 * <p>The jar carries {@code native/{os}-{arch}/libelysiumkv_jni.{so,dylib}}, which
 * has to reach the filesystem before {@code System.load} will take it.
 *
 * <p>Every load must get its own file — deduplicating by content digest would be an error.
 * {@code System.load} refuses a library <em>file</em> already loaded in another class loader
 * ({@code "Native Library ... already loaded in another classloader"}), which two copies of this
 * class in one container would hit. {@link Files#createTempFile} is atomic and unique, so racing
 * loaders never contend for a name and a half-written library — which, unlike an ordinary file,
 * would be executed — is never visible.
 *
 * <p>Permissions are set explicitly rather than inherited, and the file goes directly into the
 * system temp directory: a predictably named subdirectory in a world-writable place can be
 * pre-created as a symlink before the JVM starts.
 *
 * <p>Set {@code -Delysiumkv.library.path=/path/to/libelysiumkv_jni.dylib} to skip
 * extraction entirely and load a build directory directly.
 */
final class NativeLibrary {
    private NativeLibrary() {}

    private static final String PROPERTY = "elysiumkv.library.path";

    private static volatile boolean loaded;

    static synchronized void load() {
        if (loaded) return;

        String override = System.getProperty(PROPERTY);
        if (override != null && !override.isEmpty()) {
            System.load(Path.of(override).toAbsolutePath().toString());
            loaded = true;
            return;
        }

        String resource = "/native/" + platform() + "/" + libraryName();
        try (InputStream in = NativeLibrary.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new UnsatisfiedLinkError(
                        "no native library at " + resource + " in this jar, and -D" + PROPERTY
                                + " is unset. The jar was built without a native artifact for "
                                + platform() + ".");
            }
            Path extracted = extract(in);
            System.load(extracted.toString());
            discard(extracted);
            loaded = true;
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("extracting " + resource + " failed: " + e);
        }
    }

    /** Writes the library to a path no other loader will use. */
    private static Path extract(InputStream in) throws IOException {
        String name = libraryName();
        int dot = name.lastIndexOf('.');
        Path target = createPrivateTempFile(name.substring(0, dot) + "-", name.substring(dot));
        boolean written = false;
        try {
            try (OutputStream out = Files.newOutputStream(target)) {
                byte[] chunk = new byte[64 * 1024];
                for (int read = in.read(chunk); read > 0; read = in.read(chunk)) {
                    out.write(chunk, 0, read);
                }
            }
            written = true;
            return target;
        } finally {
            if (!written) Files.deleteIfExists(target);
        }
    }

    /**
     * Owner-only from the moment it exists, where the filesystem can say so.
     * {@code createTempFile} happens to do this on most JVMs, but "happens to" is
     * not a permission model — an executable written to a shared directory is
     * worth asking for explicitly rather than inheriting from a umask.
     */
    private static Path createPrivateTempFile(String prefix, String suffix) throws IOException {
        Path directory = Path.of(System.getProperty("java.io.tmpdir"));
        try {
            return Files.createTempFile(directory, prefix, suffix,
                                        PosixFilePermissions.asFileAttribute(
                                                PosixFilePermissions.fromString("rw-------")));
        } catch (UnsupportedOperationException notPosix) {
            // Windows: ACLs, not mode bits. The temp directory is per-user there.
            return Files.createTempFile(directory, prefix, suffix);
        }
    }

    /**
     * Removes the extracted file once it is mapped. On POSIX an unlinked library
     * stays alive through the open mapping, so nothing is left behind even if the
     * JVM is killed; Windows holds the file open and refuses, so fall back to
     * cleanup at exit.
     */
    private static void discard(Path extracted) {
        try {
            Files.deleteIfExists(extracted);
        } catch (IOException windowsHoldsItOpen) {
            extracted.toFile().deleteOnExit();
        }
    }

    static String platform() {
        return osName() + "-" + archName();
    }

    private static String osName() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("mac") || os.contains("darwin")) return "darwin";
        if (os.contains("linux")) return "linux";
        if (os.contains("windows")) return "windows";
        return os.replaceAll("[^a-z0-9]+", "");
    }

    private static String archName() {
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        if (arch.equals("amd64") || arch.equals("x86_64")) return "x86_64";
        if (arch.equals("aarch64") || arch.equals("arm64")) return "aarch64";
        return arch.replaceAll("[^a-z0-9_]+", "");
    }

    private static String libraryName() {
        return osName().equals("darwin") ? "libelysiumkv_jni.dylib"
                : osName().equals("windows") ? "elysiumkv_jni.dll" : "libelysiumkv_jni.so";
    }
}
