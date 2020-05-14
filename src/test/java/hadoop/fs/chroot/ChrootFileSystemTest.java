package hadoop.fs.chroot;

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

public class ChrootFileSystemTest extends GenericFileSystemTest {

  private static final String CHROOT_TEST_FS = "chroot.test.fs";
  private File ROOT = new File("./target/tmp/" + getClass().getName());
  private Configuration _configuration;
  private Path _chrootFsRoot;
  private Path _realChrootPath;

  @Override
  public Path getRootPath() {
    return _chrootFsRoot;
  }

  @Override
  public Configuration getConfiguration() {
    return _configuration;
  }

  @Before
  public void setup() throws Exception {
    FileSystem.closeAll();
    String rootPathStr = ROOT.getCanonicalPath();
    _configuration = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(_configuration);
    Path rootPath = new Path(rootPathStr);
    local.delete(rootPath, true);
    local.mkdirs(rootPath);
    rootPath = local.makeQualified(rootPath);

    Path realPath = new Path(rootPath, "real");
    local.mkdirs(realPath);
    realPath = local.makeQualified(realPath);
    _chrootFsRoot = new Path("chroot", "test", "/");
    _realChrootPath = new Path(realPath, "chroot");
    FileSystem fileSystem = _realChrootPath.getFileSystem(_configuration);
    fileSystem.mkdirs(_realChrootPath);

    _configuration.set(CHROOT_TEST_FS, _realChrootPath.toString());
    assertEmptyDir(_chrootFsRoot);
  }

  @Test
  public void testFileStatusWhileWriting() throws IOException {
    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
                                            .toString());

    FileSystem fileSystem = file.getFileSystem(_configuration);

    assertFalse(fileSystem.exists(file));

    try (FSDataOutputStream output = fileSystem.create(file)) {
      assertTrue(fileSystem.exists(file));
    }

    assertTrue(fileSystem.exists(file));
    assertFileExists(file);
    assertFileExists(new Path(_realChrootPath, file.getName()));

  }

  @Test
  public void testCRDFile() throws IOException {
    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
                                            .toString());
    touchFile(file);
    assertFileExists(file);
    assertFileExists(new Path(_realChrootPath, file.getName()));
  }

  @Test
  public void testFileStatus() throws IOException {
    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
                                            .toString());
    touchFile(file);
    assertFileExists(file);

    FileSystem fileSystem = file.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(file);
    assertEquals(1, listStatus.length);
    assertEquals(file, listStatus[0].getPath());
  }

  @Test
  public void testFileNotFoundError() throws IOException {
    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
                                            .toString());
    FileSystem fileSystem = file.getFileSystem(_configuration);
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
