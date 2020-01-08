package hadoop.fs.chroot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class ChrootFileSystemTest {

  private static final String CHROOT_TEST_FS = "chroot.test.fs";
  private File ROOT = new File("./target/tmp/" + getClass().getName());
  private Configuration _conf;
  private Path _chrootFsRoot;
  private Path _realChrootPath;

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

    Path realPath = new Path(rootPath, "real");
    local.mkdirs(realPath);
    realPath = local.makeQualified(realPath);
    _chrootFsRoot = new Path("chroot", "test", "/");
    _realChrootPath = new Path(realPath, "chroot");
    FileSystem fileSystem = _realChrootPath.getFileSystem(_conf);
    fileSystem.mkdirs(_realChrootPath);

    _conf.set(CHROOT_TEST_FS, _realChrootPath.toString());
  }

  @Test
  public void testCRDFile() throws IOException {
    assertEmptyDir(_chrootFsRoot);

    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
                                            .toString());
    touchFile(file);
    assertFileExists(file);
    assertFileExists(new Path(_realChrootPath, file.getName()));
  }

  @Test
  public void testFileStatus() throws IOException {
    assertEmptyDir(_chrootFsRoot);

    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
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
    assertEmptyDir(_chrootFsRoot);

    Path file = new Path(_chrootFsRoot, UUID.randomUUID()
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

  private void assertEmptyDir(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(_conf);
    assertTrue(fileSystem.exists(path));
    assertEquals(0, fileSystem.listStatus(path).length);
  }
}
