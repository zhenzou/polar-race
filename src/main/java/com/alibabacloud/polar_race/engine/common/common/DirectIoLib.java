/**
 * Copyright (C) 2014 Stephen Macke (smacke@cs.stanford.edu)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibabacloud.polar_race.engine.common.common;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import net.smacke.jaydio.OpenFlags;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

/**
 * Class containing native hooks and utility methods for performing direct I/O, using
 * the Linux <tt>O_DIRECT</tt> flag. </p>
 *
 * <p> This class is initialized at class load time, by registering JNA hooks into native methods.
 * It also calculates Linux kernel version-dependent alignment amount (in bytes) for use with the <tt>O_DIRECT</tt> flag,
 * when given a string for a file or directory.</p>
 *
 * @author smacke
 */
public class DirectIoLib {
    private static final Logger logger = LoggerFactory.getLogger(DirectIoLib.class);

    static {
        try {
            Native.register(Platform.C_LIBRARY_NAME);
        } catch (Throwable e) {
            logger.warn("Unable to register libc at class load time: " + e.getMessage(), e);
        }
    }

    private static final int FS_BLOCK_SIZE = 512;

    private static final long FS_BLOCK_NOT_MASK = -((long) FS_BLOCK_SIZE);


    /**
     * Finds a block size for use with O_DIRECT. Choose it in the most paranoid
     * way possible to maximize probability that things work.
     *
     * @param fileOrDir A file or directory within which O_DIRECT access will be performed.
     */
    public static int getBlockSize(String fileOrDir) {

        int fsBlockSize = -1;

        // get file system block size for use with workingDir
        // see "man 3 posix_memalign" for why we do this
        final int _PC_REC_XFER_ALIGN = 0x11;

        fsBlockSize = pathconf(fileOrDir, _PC_REC_XFER_ALIGN);
        /* conservative for version >= 2.6
         * "man 2 open":
         *
         * Under Linux 2.6, alignment
         * to 512-byte boundaries suffices.
         */

        // Since O_DIRECT requires pages to be memory aligned with the file system block size,
        // we will do this too in case the page size and the block size are different for
        // whatever reason. By taking the least common multiple, everything should be happy:
        int pageSize = getpagesize();
        fsBlockSize = lcm(fsBlockSize, pageSize);

        // just being completely paranoid:
        // (512 is the rule for 2.6+ kernels as mentioned before)
        fsBlockSize = lcm(fsBlockSize, 512);

        // lastly, a sanity check
        if (fsBlockSize <= 0 || ((fsBlockSize & (fsBlockSize - 1)) != 0)) {
            logger.warn("file system block size should be a power of two, was found to be " + fsBlockSize);
            logger.warn("Disabling O_DIRECT support");
            return -1;
        }

        return fsBlockSize;
    }


    // -- Java interfaces to native methods

    /**
     * Interface into native pread function. Always reads an entire buffer,
     * unlike {@link #pwrite(int, AlignedDirectByteBuffer, long) pwrite()} which uses buffer state
     * to determine how much of buffer to write.</p>
     *
     * @param fd     A file discriptor to pass to native pread
     * @param buf    The buffer into which to record the file read
     * @param offset The file offset at which to read
     * @return The number of bytes successfully read from the file
     * @throws IOException
     */
    public static int pread(int fd, AlignedDirectByteBuffer buf, long offset) throws IOException {
        buf.clear(); // so that we read an entire buffer
        int n = pread(fd, buf.pointer(), new NativeLong(buf.capacity()), new NativeLong(offset)).intValue();
        if (n == 0) throw new EOFException("Tried to read past EOF at offset " + offset + " into ByteBuffer " + buf);
        if (n < 0) {
            throw new IOException("error reading file at offset " + offset + ": " + getLastError());
        }
        return n;
    }

    /**
     * Interface into native pwrite function. Writes bytes corresponding to the nearest file
     * system block boundaries between <tt>buf.position()</tt> and <tt>buf.limit()</tt>.</p>
     *
     * @param fd     A file descriptor to pass to native pwrite
     * @param buf    The buffer from which to write
     * @param offset The file offset at which to write
     * @return The number of bytes successfully written to the file
     * @throws IOException
     */
    public static int pwrite(int fd, AlignedDirectByteBuffer buf, long offset) throws IOException {

        // must always write to end of current block
        // To handle writes past the logical file size,
        // we will later truncate.
        final int start = buf.position();
        assert start == blockStart(start);
        final int toWrite = blockEnd(buf.limit()) - start;

        int n = pwrite(fd, buf.pointer().share(start), new NativeLong(toWrite), new NativeLong(offset)).intValue();
        if (n < 0) {
            throw new IOException("error writing file at offset " + offset + ": " + getLastError());
        }
        return n;
    }

    /**
     * Use the <tt>open</tt> Linux system call and pass in the <tt>O_DIRECT</tt> flag.
     * Currently the only other flags passed in are <tt>O_RDONLY</tt> if <tt>readOnly</tt>
     * is <tt>true</tt>, and (if not) <tt>O_RDWR</tt> and <tt>O_CREAT</tt>.
     *
     * @param pathname The path to the file to open. If file does not exist and we are opening
     *                 with <tt>readOnly</tt>, this will throw an error. Otherwise, if it does
     *                 not exist but we have <tt>readOnly</tt> set to false, create the file.
     * @param readOnly Whether to pass in <tt>O_RDONLY</tt>
     * @return An integer file descriptor for the opened file
     * @throws IOException
     */
    public static int openFileDirect(String pathname, boolean readOnly) throws IOException {
        int flags = OpenFlags.O_DIRECT;
        if (readOnly) {
            flags |= OpenFlags.O_RDONLY;
        } else {
            flags |= OpenFlags.O_RDWR | OpenFlags.O_CREAT;
        }
        int fd = open(pathname, flags, 00644);
        if (fd < 0) {
            throw new IOException("Error opening " + pathname + ", got " + getLastError());
        }
        return fd;
    }

    /**
     * Hooks into errno using Native.getLastError(), and parses it with native strerror function.
     *
     * @return An error message corresponding to the last <tt>errno</tt>
     */
    public static String getLastError() {
        return strerror(Native.getLastError());
    }


    // -- alignment logic utility methods

    /**
     * @return The soft block size for use with transfer multiples
     * and memory alignment multiples
     */
    public static int blockSize() {
        return FS_BLOCK_SIZE;
    }

    /**
     * Returns the default buffer size for file channels doing O_DIRECT
     * I/O. By default this is equal to the block size.
     *
     * @return The default buffer size
     */
    public static int defaultBufferSize() {
        return FS_BLOCK_SIZE;
    }

    /**
     * Given <tt>value</tt>, find the largest number less than or equal
     * to <tt>value</tt> which is a multiple of the fs block size.
     *
     * @param value
     * @return The largest number less than or equal to <tt>value</tt>
     * which is a multiple of the soft block size
     */
    public static long blockStart(long value) {
        return value & FS_BLOCK_NOT_MASK;
    }


    /**
     * @see #blockStart(long)
     */
    public static int blockStart(int value) {
        return (int) (value & FS_BLOCK_NOT_MASK);
    }


    /**
     * Given <tt>value</tt>, find the smallest number greater than or equal
     * to <tt>value</tt> which is a multiple of the fs block size.
     *
     * @param value
     * @return The smallest number greater than or equal to <tt>value</tt>
     * which is a multiple of the soft block size
     */
    public static long blockEnd(long value) {
        return (value + FS_BLOCK_SIZE - 1) & FS_BLOCK_NOT_MASK;
    }


    /**
     * @see #blockEnd(long)
     */
    public static int blockEnd(int value) {
        return (int) ((value + FS_BLOCK_SIZE - 1) & FS_BLOCK_NOT_MASK);
    }


    /**
     * Static variant of {@link #blockEnd(int)}.
     *
     * @param blockSize
     * @param position
     * @return The smallest number greater than or equal to <tt>position</tt>
     * which is a multiple of the <tt>blockSize</tt>
     */
    public static long blockEnd(int blockSize, long position) {
        long ceil = (position + blockSize - 1) / blockSize;
        return ceil * blockSize;
    }

    /**
     * @param x
     * @param y
     * @return The least common multiple of <tt>x</tt> and <tt>y</tt>
     */
    // Euclid's algo for gcd is more general than we need
    // since we only have powers of 2, but w/e
    public static int lcm(long x, long y) {
        long g = x; // will hold gcd
        long yc = y;

        // get the gcd first
        while (yc != 0) {
            long t = g;
            g = yc;
            yc = t % yc;
        }

        return (int) (x * y / g);
    }


    /**
     * Given a pointer-to-pointer <tt>memptr</tt>, sets the dereferenced value to point to the start
     * of an allocated block of <tt>size</tt> bytes, where the starting address is a multiple of
     * <tt>alignment</tt>. It is guaranteed that the block may be freed by calling @{link {@link #free(Pointer)}
     * on the starting address. See "man 3 posix_memalign".
     *
     * @param memptr    The pointer-to-pointer which will point to the address of the allocated aligned block
     * @param alignment The alignment multiple of the starting address of the allocated block
     * @param size      The number of bytes to allocate
     * @return 0 on success, one of the C error codes on failure.
     */
    public static native int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size);


    /**
     * See "man 3 free".
     *
     * @param ptr The pointer to the hunk of memory which needs freeing
     */
    public static native void free(Pointer ptr);


    /**
     * See "man 2 close"
     *
     * @param fd The file descriptor of the file to close
     * @return 0 on success, -1 on error
     */
    public static native int close(int fd); // musn't forget to do this


    // -- more native function hooks --


    public static native int ftruncate(int fd, long length);

    public static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);

    public static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

    public static native int open(String pathname, int flags);

    public static native int open(String pathname, int flags, int mode);

    public static native int getpagesize();

    public static native int pathconf(String path, int name);

    public static native String strerror(int errnum);

}
