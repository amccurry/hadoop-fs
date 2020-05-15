package hadoop.fs.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public abstract class GenericFileSystemTest {

  static final int MAX_LENGTH = 10_000_000;
  static final int MIN_LENGTH = 10_000;

  protected abstract Path getRootPath();

  protected abstract Configuration getConfiguration();

  protected UserGroupInformation getUgi() throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  @Test
  public void testRWFile() throws Exception {
    getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
      Path path = new Path(getRootPath(), UUID.randomUUID()
                                              .toString());

      FileSystem fileSystem = path.getFileSystem(getConfiguration());
      assertFalse(fileSystem.exists(path));
      byte[] writeMd5;
      try (FSDataOutputStream outputStream = fileSystem.create(path)) {
        writeMd5 = writeData(outputStream);
      }
      assertTrue(fileSystem.exists(path));
      byte[] readMd5;
      try (FSDataInputStream input = fileSystem.open(path)) {
        readMd5 = readData(input);
      }
      assertTrue(Arrays.equals(writeMd5, readMd5));
      return null;
    });
  }

  @Test
  public void testCDFile() throws Exception {
    getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
      Path path = new Path(getRootPath(), UUID.randomUUID()
                                              .toString());

      FileSystem fileSystem = path.getFileSystem(getConfiguration());
      assertFalse(fileSystem.exists(path));
      touchFile(path);
      assertTrue(fileSystem.exists(path));
      fileSystem.delete(path, false);
      assertFalse(fileSystem.exists(path));
      return null;
    });
  }

  @Test
  public void testRenameFile() throws Exception {
    getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
      Path path1 = new Path(getRootPath(), UUID.randomUUID()
                                               .toString());
      Path path2 = new Path(getRootPath(), UUID.randomUUID()
                                               .toString());

      FileSystem fileSystem = path1.getFileSystem(getConfiguration());
      assertFalse(fileSystem.exists(path1));
      assertFalse(fileSystem.exists(path2));
      touchFile(path1);
      assertTrue(fileSystem.exists(path1));
      assertFalse(fileSystem.exists(path2));
      fileSystem.rename(path1, path2);
      assertFalse(fileSystem.exists(path1));
      assertTrue(fileSystem.exists(path2));
      return null;
    });
  }

  protected byte[] readData(FSDataInputStream input) throws Exception {
    byte[] buffer = new byte[1024];
    int len;
    MessageDigest digest = MessageDigest.getInstance("MD5");
    while ((len = input.read(buffer)) != -1) {
      digest.update(buffer, 0, len);
    }
    return digest.digest();
  }

  protected byte[] writeData(FSDataOutputStream outputStream) throws Exception {
    byte[] buffer = new byte[1024];
    Random random = new Random();
    long length = random.nextInt(MAX_LENGTH) + MIN_LENGTH;
    MessageDigest digest = MessageDigest.getInstance("MD5");
    while (length > 0) {
      random.nextBytes(buffer);
      int len = (int) Math.min(length, buffer.length);
      digest.update(buffer, 0, len);
      outputStream.write(buffer, 0, len);
      length -= len;
    }
    return digest.digest();
  }

  protected void assertFileExists(Path path) throws IOException {
    FileStatus fileStatus = path.getFileSystem(getConfiguration())
                                .getFileStatus(path);
    assertTrue(fileStatus.isFile());
  }

  protected void touchFile(Path path) throws IOException {
    path.getFileSystem(getConfiguration())
        .create(path)
        .close();
  }

  protected void assertEmptyDir(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(getConfiguration());
    assertTrue(fileSystem.exists(path));
    assertEquals(0, fileSystem.listStatus(path).length);
  }

  protected void assertIsFile(Path path) throws IOException {
    FileStatus fileStatus = path.getFileSystem(getConfiguration())
                                .getFileStatus(path);
    assertTrue(fileStatus.isFile());
  }

  protected void mkdir(Path path) throws IOException {
    path.getFileSystem(getConfiguration())
        .mkdirs(path);
  }

  protected void assertDoesNotExist(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(getConfiguration());
    assertFalse(fileSystem.exists(path));
  }
}
