package hadoop.fs.mount;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class MountFileSystemTest {

  private static final String MOUNT_TEST_DEFAULT_MOUNT = "mount.test.default.mount";

  private static final String MOUNT_TEST_AUTOMATIC_UPDATES_DISABLED = "mount.test.automatic.updates.disabled";

  private static final String MOUNT_TEST_PATH = "mount.test.path";

  private File ROOT = new File("./target/tmp/" + getClass().getName());
  private Path _realPath;
  private Configuration _conf;

  private Path _mountPath;

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

    _mountPath = new Path("mount", "test", _realPath.toUri()
                                                    .getPath());

    Path mounts = new Path(_realPath, "mounts.file");
    mounts = local.makeQualified(mounts);

    _conf.set(MOUNT_TEST_PATH, mounts.toString());
    _conf.set(MOUNT_TEST_DEFAULT_MOUNT, _realPath.toString());
    _conf.setBoolean(MOUNT_TEST_AUTOMATIC_UPDATES_DISABLED, true);
  }

  @Test
  public void testCRD() throws IOException {
    Path dstOfMount = new Path(_mountPath, "mount1");
    assertDoesNotExist(dstOfMount);

    Path srcOfMount = new Path(_realPath, "testmount");
    mkdir(srcOfMount);

    MountFileSystem.addMount(_conf, srcOfMount, dstOfMount);

    assertEmptyDir(dstOfMount);

    Path file = new Path(dstOfMount, UUID.randomUUID()
                                         .toString());
    touchFile(file);
    assertFileExists(file);
    assertFileExists(new Path(srcOfMount, file.getName()));
  }

  private void assertFileExists(Path path) throws IOException {
    FileStatus fileStatus = path.getFileSystem(_conf)
                                .getFileStatus(path);
    assertTrue(fileStatus.isFile());
  }

  private void touchFile(Path path) throws IOException {
    path.getFileSystem(_conf)
        .create(path)
        .close();
  }

  private void mkdir(Path path) throws IOException {
    path.getFileSystem(_conf)
        .mkdirs(path);
  }

  private void assertDoesNotExist(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(_conf);
    assertFalse(fileSystem.exists(path));
  }

  private void assertEmptyDir(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(_conf);
    assertTrue(fileSystem.exists(path));
    assertEquals(0, fileSystem.listStatus(path).length);
  }
}
