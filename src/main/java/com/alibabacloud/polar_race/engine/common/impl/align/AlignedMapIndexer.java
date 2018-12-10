package com.alibabacloud.polar_race.engine.common.impl.align;

import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.Timer;
import com.alibabacloud.polar_race.engine.common.common.MMapper;
import com.alibabacloud.polar_race.engine.common.common.UncheckedOnce;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.index.Indexer;
import com.alibabacloud.polar_race.engine.common.util.Convert;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.hash.HashLongLongMap;
import com.koloboke.collect.map.hash.HashLongLongMapFactory;
import com.koloboke.collect.map.hash.HashLongLongMaps;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibabacloud.polar_race.engine.common.util.Utils.log;

/**
 * @since 0.1
 * create at 2018/10/1
 */
public class AlignedMapIndexer implements Indexer {

    HashLongLongMap indexes = HashLongLongMaps.newMutableMap(8);

    private String keyFilePath;

    private File keyFile;

    private long checkPoint;

    private long maxValueOffset;

    private long size;

    private MMapper mapper;

    private UncheckedOnce<Void> once = new UncheckedOnce<>(this::load);

    private long block;

    AlignedMapIndexer(long maxValueOffset) {
        this.maxValueOffset = maxValueOffset;
    }

    private void initIndexes() {
        HashLongLongMapFactory factory = HashLongLongMaps.getDefaultFactory();
        HashConfig config = factory.getHashConfig();
        config = config.withMaxLoad(1.0);
        config = config.withGrowthFactor(1.01);
        config = config.withTargetLoad(0.99);
        config = config.withMinLoad(0.99);
        factory = factory.withHashConfig(config);
        indexes = factory.newMutableMap(4020000);
    }

    @Override
    public void open(String path) {
        keyFilePath = String.join(File.separator, path, Constants.KEY_FILE_NAME);

        keyFile = new File(keyFilePath);

        if (!keyFile.exists()) {
            try {
                IOUtils.createFile(keyFile);
                mapper = new MMapper(keyFile, Constants.INDEX_MMAP_BLOCK_SIZE);
                block = size / Constants.INDEX_MMAP_BLOCK_SIZE;
            } catch (EngineException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void load(Void arg) {
        if (!keyFile.exists() || keyFile.length() == 0) {
            return;
        }
        initIndexes();
        Timer timer = Timer.start(keyFile.getParent() + "-load");
        try (FileInputStream in = new FileInputStream(keyFile)) {
            FileChannel fc = in.getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.INDEX_FLUSH_BATCH_SIZE);
            long checkPoint = this.checkPoint;
            long offset = 0;
            while (true) {
                buffer.clear();
                final int read = fc.read(buffer);
                final int count = read / Constants.KEY_SIZE;
                buffer.flip();
                for (int i = 0; i < count; i++) {
                    if (offset >= maxValueOffset) {
                        this.checkPoint = checkPoint;
                        return;
                    }
                    final long key = buffer.getLong();
                    indexes.put(key, offset);
                    if (offset > checkPoint) {
                        checkPoint = offset;
                    }
                    offset += Constants.VALUE_SIZE;
                }
                if (read < Constants.INDEX_FLUSH_BATCH_SIZE) {
                    break;
                }
            }
            this.checkPoint = checkPoint;
        } catch (EOFException e) {
            log(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            timer.end();
        }
    }

    @Override
    public void set(byte[] key, long address) {
        indexes.put(Convert.bytes2long(key), address);
    }

    @Override
    public void add(byte[][] keys, int count) {
        long size = this.size;
        for (int i = 0; i < count; i++) {
            byte[] key = keys[i];
            mapper.put(key);
            size += Constants.KEY_SIZE;
            if (size % Constants.INDEX_MMAP_BLOCK_SIZE == 0) {
                block++;
                mapper.roll();
            }
        }
        this.size = size;
    }

    @Override
    public void add(byte[] key) {
        mapper.put(key);
        size += Constants.KEY_SIZE;
        if (size % Constants.INDEX_MMAP_BLOCK_SIZE == 0) {
            block++;
            mapper.roll();
        }
    }

    @Override
    public long get(byte[] key) {
        once.exec(null);
        return indexes.getOrDefault(Convert.bytes2long(key), -1);
    }

    @Override
    public long getCheckPoint() {
        return checkPoint;
    }

    @Override
    public void close() {
        if (mapper != null) {
            mapper.close();
        }
        indexes = null;
    }
}
