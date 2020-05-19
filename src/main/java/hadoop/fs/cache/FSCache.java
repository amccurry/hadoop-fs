package hadoop.fs.cache;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Value;

public class FSCache {

  private static final String CACHE_NAME = "fs-cache";
  private static FSCache FS_CACHE;

  public synchronized static FSCache getInstance() {
    if (FS_CACHE == null) {
      FS_CACHE = new FSCache();
    }
    return FS_CACHE;
  }

  private final Cache<FileBlockCacheKey, byte[]> _cache;
  private final int _blockSize = 5 * 1024 * 1024;

  private FSCache() {
    File cacheDir = new File("./cache");
    ResourcePoolsBuilder disk = ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                    .heap(256, MemoryUnit.MB)
                                                    .disk(20, MemoryUnit.GB, true);

    CacheConfigurationBuilder<FileBlockCacheKey, byte[]> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
        FileBlockCacheKey.class, byte[].class, disk);

    PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                                                             .with(CacheManagerBuilder.persistence(cacheDir))
                                                             .withCache(CACHE_NAME, builder)
                                                             .withSerializer(FileBlockCacheKey.class,
                                                                 FileBlockCacheKeySerializer.class)
                                                             .build(true);

    _cache = cacheManager.getCache(CACHE_NAME, FileBlockCacheKey.class, byte[].class);
    Runtime.getRuntime()
           .addShutdownHook(new Thread(() -> cacheManager.close()));
  }

  public int read(FileStatus fileStatus, FSDataInputStream input, byte[] b, int off, int len) throws IOException {
    long pos = input.getPos();
    if (pos >= fileStatus.getLen()) {
      return -1;
    }
    int blockOffset = (int) (pos % _blockSize);
    long blockId = pos / _blockSize;

    FileBlockCacheKey key = FileBlockCacheKey.builder()
                                             .length(fileStatus.getLen())
                                             .modificationTime(fileStatus.getModificationTime())
                                             .path(fileStatus.getPath()
                                                             .toString())
                                             .blockId(blockId)
                                             .build();
    byte[] data = _cache.get(key);
    if (data == null) {
      long position = blockId * _blockSize;
      int l = (int) Math.min(_blockSize, fileStatus.getLen() - position);
      data = new byte[_blockSize];
      input.readFully(position, data, 0, l);
      _cache.put(key, data);
    }
    int remainingDataInBlock = _blockSize - blockOffset;
    long remainingDataInFile = fileStatus.getLen() - pos;
    int length = (int) Math.min(Math.min(remainingDataInBlock, len), remainingDataInFile);
    System.arraycopy(data, blockOffset, b, off, length);
    input.seek(pos + length);
    return length;
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @EqualsAndHashCode
  public static class FileBlockCacheKey {

    long length;

    long modificationTime;

    String path;

    long blockId;

  }

  public static class FileBlockCacheKeySerializer implements Serializer<FileBlockCacheKey> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public FileBlockCacheKeySerializer() {

    }

    public FileBlockCacheKeySerializer(ClassLoader classLoader) {

    }

    @Override
    public ByteBuffer serialize(FileBlockCacheKey object) throws SerializerException {
      try {
        return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(object));
      } catch (JsonProcessingException e) {
        throw new SerializerException(e);
      }
    }

    @Override
    public FileBlockCacheKey read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      try {
        return OBJECT_MAPPER.readValue(toBytes(binary), FileBlockCacheKey.class);
      } catch (IOException e) {
        throw new SerializerException(e);
      }
    }

    @Override
    public boolean equals(FileBlockCacheKey object, ByteBuffer binary)
        throws ClassNotFoundException, SerializerException {
      FileBlockCacheKey key = read(binary);
      return object.equals(key);
    }

    private byte[] toBytes(ByteBuffer binary) {
      byte[] buffer = new byte[binary.remaining()];
      binary.get(buffer);
      return buffer;
    }

  }

}
