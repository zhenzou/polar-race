package com.alibabacloud.polar_race.engine.common.util;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.common.DirectIoLib;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.PointerByReference;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @since 0.1
 * create at 2018/11/26
 */
public interface IOUtils {
    static void createFile(File file) throws EngineException {
        if (file.exists()) {
            return;
        }
        try {
            boolean b = file.createNewFile();
            if (!b) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "can not create new file " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
    }

    static void createFile(String fp) throws EngineException {
        createFile(new File(fp));
    }

    static void createDir(String path) throws EngineException {
        final File dir = new File(path);
        if (!dir.exists()) {
            final boolean mkdirs = dir.mkdirs();
            if (!mkdirs) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "can not make new index dir " + path);
            }
        }
    }

    static File newFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                Utils.rethrow(e);
            }
        }
        return file;
    }

    static RandomAccessFile newRaf(String path, String mode) {
        final File file = newFile(path);
        try {
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException e) {
            Utils.rethrow(e);
        }
        return null;
    }

    static void write(File file, byte[] bytes) {
        try (FileOutputStream out = new FileOutputStream(file)) {
            out.write(bytes);
        } catch (IOException e) {
            Utils.rethrow(e);
        }
    }

    static void append(File file, byte[] bytes) {
        try (FileOutputStream out = new FileOutputStream(file, true)) {
            out.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String getFileName(String fp) {
        final Path path = Paths.get(fp);
        final String fn = path.getFileName().toString();
        final int i = fn.lastIndexOf('.');
        if (i < 0) {
            return fn;
        } else {
            return fn.substring(0, i);
        }
    }

    static void close(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void deleteFile(File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            FileChannel fc = raf.getChannel();
            fc.truncate(0);
            fc.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            Files.delete(Paths.get(file.getAbsolutePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void pwirte(String fp, byte[] bytes, long offset) {
        try (RandomAccessFile raf = new RandomAccessFile(fp, "rw")) {
            raf.seek(offset);
            raf.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static PointerByReference newPointer() {
        PointerByReference ref = new PointerByReference();
        DirectIoLib.posix_memalign(ref, EngineRace.ALIGNMENT, Constants.NATIVE_VALUE_SIZE);
        return ref;
    }

    static PointerByReference newPointer(long size) {
        PointerByReference ref = new PointerByReference();
        DirectIoLib.posix_memalign(ref, EngineRace.ALIGNMENT, new NativeLong(size));
        return ref;
    }
}
