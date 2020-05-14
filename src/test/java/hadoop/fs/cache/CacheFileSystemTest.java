package hadoop.fs.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import hadoop.fs.base.GenericFileSystemTest;

public class CacheFileSystemTest extends GenericFileSystemTest {

  private static final String CACHE_TEST_FS = "cache.test.fs";
  private File ROOT = new File("./target/tmp/" + getClass().getName());
  private Configuration _conf;
  private Path _cacheFsRoot;
  private Path _realPath;

  @Before
  public void setup() throws Exception {
    FileSystem.closeAll();
    String rootPathStr = ROOT.getCanonicalPath();
    _conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(_conf);
    Path rootPath = new Path(rootPathStr);
    local.delete(rootPath, true);
    local.mkdirs(rootPath);
    rootPath = local.makeQualified(rootPath);

    _realPath = new Path(rootPath, "real");
    local.mkdirs(_realPath);
    _realPath = local.makeQualified(_realPath);
    _cacheFsRoot = new Path("cache", "test", _realPath.toUri()
                                                      .getPath());
    FileSystem fileSystem = _realPath.getFileSystem(_conf);
    fileSystem.mkdirs(_realPath);

    _conf.set(CACHE_TEST_FS, _realPath.toString());
    assertEmptyDir(_cacheFsRoot);
  }

  @Override
  protected Path getRootPath() {
    return _cacheFsRoot;
  }

  @Override
  protected Configuration getConfiguration() {
    return _conf;
  }

  @Test
  public void testFileStatusWhileWriting() throws IOException {
    Path file = new Path(_cacheFsRoot, UUID.randomUUID()
                                           .toString());

    FileSystem fileSystem = file.getFileSystem(_conf);

    assertFalse(fileSystem.exists(file));

    try (FSDataOutputStream output = fileSystem.create(file)) {
      assertTrue(fileSystem.exists(file));
    }

    assertTrue(fileSystem.exists(file));
    assertFileExists(file);
    assertFileExists(new Path(_realPath, file.getName()));

  }

  @Test
  public void testCRDFile() throws IOException {
    Path file = new Path(_cacheFsRoot, UUID.randomUUID()
                                           .toString());
    touchFile(file);
    assertFileExists(file);
    assertFileExists(new Path(_realPath, file.getName()));
  }

  @Test
  public void testFileStatus() throws IOException {
    Path file = new Path(_cacheFsRoot, UUID.randomUUID()
                                           .toString());
    touchFile(file);
    assertFileExists(file);

    FileSystem fileSystem = file.getFileSystem(_conf);
    FileStatus[] listStatus = fileSystem.listStatus(file);
    assertEquals(1, listStatus.length);
    assertEquals(file, listStatus[0].getPath());
  }

  @Test
  public void testFileNotFoundError() throws IOException {
    Path file = new Path(_cacheFsRoot, UUID.randomUUID()
                                           .toString());
    FileSystem fileSystem = file.getFileSystem(_conf);
    try {
      fileSystem.getFileStatus(file);
      fail();
    } catch (Exception e) {
      if (e instanceof FileNotFoundException) {

      } else {
        fail();
      }
    }
  }

}
