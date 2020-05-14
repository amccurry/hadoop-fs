package hadoop.fs.mount;

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

public class MountFileSystemTest extends GenericFileSystemTest {

  private static final String MOUNT_TEST_DEFAULT_MOUNT = "mount.test.default.mount";
  private static final String MOUNT_TEST_AUTOMATIC_UPDATES_DISABLED = "mount.test.automatic.updates.disabled";
  private static final String MOUNT_TEST_PATH = "mount.test.path";

  private File ROOT = new File("./target/tmp/" + getClass().getName());
  private Path _realPath;
  private Configuration _conf;
  private Path _mountFsRoot;
  private Path _mountDir;
  private Path _realDirPath;

  @Override
  public Path getRootPath() {
    return _mountFsRoot;
  }

  @Override
  public Configuration getConfiguration() {
    return _conf;
  }

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

    _mountFsRoot = new Path("mount", "test", "/");

    Path mounts = new Path(_realPath, "mounts.file");
    mounts = local.makeQualified(mounts);

    _conf.set(MOUNT_TEST_PATH, mounts.toString());
    _conf.set(MOUNT_TEST_DEFAULT_MOUNT, _realPath.toString());
    _conf.setBoolean(MOUNT_TEST_AUTOMATIC_UPDATES_DISABLED, true);

    _mountDir = new Path(_mountFsRoot, "mnt");
    assertDoesNotExist(_mountDir);
    _realDirPath = new Path(_realPath, "testmount");
    mkdir(_realDirPath);
    MountFileSystem.addMount(_conf, _realDirPath, _mountDir);
    assertEmptyDir(_mountDir);
  }

  @Test
  public void testFileStatusWhileWriting() throws IOException {
    Path file = new Path(_mountDir, UUID.randomUUID()
                                        .toString());

    FileSystem fileSystem = file.getFileSystem(_conf);

    assertFalse(fileSystem.exists(file));

    try (FSDataOutputStream output = fileSystem.create(file)) {
      assertTrue(fileSystem.exists(file));
    }

    assertTrue(fileSystem.exists(file));
    assertIsFile(file);
    assertIsFile(new Path(_realDirPath, file.getName()));

  }

  @Test
  public void testCRDFile() throws IOException {
    Path file = new Path(_mountDir, UUID.randomUUID()
                                        .toString());
    touchFile(file);
    assertIsFile(file);
    assertIsFile(new Path(_realDirPath, file.getName()));
  }

  @Test
  public void testFileStatus() throws IOException {
    Path file = new Path(_mountDir, UUID.randomUUID()
                                        .toString());
    touchFile(file);
    assertIsFile(file);

    FileSystem fileSystem = file.getFileSystem(_conf);
    FileStatus[] listStatus = fileSystem.listStatus(file);
    assertEquals(1, listStatus.length);
    assertEquals(file, listStatus[0].getPath());
  }

  @Test
  public void testFileNotFoundError() throws IOException {
    Path file = new Path(_mountDir, UUID.randomUUID()
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
