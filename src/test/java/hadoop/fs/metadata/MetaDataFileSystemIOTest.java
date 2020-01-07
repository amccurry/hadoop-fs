package hadoop.fs.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class MetaDataFileSystemIOTest {

  private static final String DATA_PATH = "metadata.data.path";
  private static final String META_PATH = "metadata.meta.path";

  private File ROOT = new File("./target/tmp/" + getClass().getName());
  private Path _metaPath;
  private Path _dataPath;
  private Path _linkPath;
  private Configuration _conf;

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

    _metaPath = new Path(rootPath, "meta");
    local.mkdirs(_metaPath);
    _metaPath = local.makeQualified(_metaPath);

    _dataPath = new Path(rootPath, "data");
    local.mkdirs(_dataPath);
    _dataPath = local.makeQualified(_dataPath);

    _linkPath = new Path(rootPath, "link");
    local.mkdirs(_linkPath);
    _linkPath = local.makeQualified(_linkPath);

    _conf.set(META_PATH, _metaPath.toString());
    _conf.set(DATA_PATH, _dataPath.toString());
  }

  @Test
  public void testFileCRD() throws Exception {
    Path path = new Path("metadata://test/test1");
    FileSystem fileSystem = path.getFileSystem(_conf);
    assertFalse(fileSystem.exists(path));

    long value = System.currentTimeMillis();

    try (FSDataOutputStream output = fileSystem.create(path)) {
      assertTrue(fileSystem.exists(path));
      output.writeLong(value);
      assertEquals(0, fileSystem.getFileStatus(path)
                                .getLen());
    }

    assertEquals(8, fileSystem.getFileStatus(path)
                              .getLen());

    try (FSDataInputStream input = fileSystem.open(path)) {
      assertEquals(value, input.readLong());
    }

    assertTrue(fileSystem.delete(path, false));
    assertFalse(fileSystem.exists(path));

    assertNoFiles(new Path("metadata://test/"));
    assertNoFiles(_metaPath);
    assertNoFiles(_dataPath);
  }

  @Test
  public void testRecursiveDelete() throws Exception {
    Path file1 = new Path("metadata://test/dir1/test1");
    Path file2 = new Path("metadata://test/dir1/subdir1/test1");
    touchFile(file1);
    touchFile(file2);

    FileSystem fileSystem = file1.getFileSystem(_conf);
    assertTrue(fileSystem.exists(file1));
    assertTrue(fileSystem.exists(file2));

    Path dir = new Path("metadata://test/dir1");
    fileSystem.delete(dir, true);

    assertNoFiles(new Path("metadata://test/"));
    assertNoFiles(_metaPath);
    assertNoFiles(_dataPath);
  }

  @Test
  public void testLink() throws IOException {
    FileSystem fs = _linkPath.getFileSystem(_conf);
    Path path = new Path(_linkPath, UUID.randomUUID()
                                        .toString());
    path = fs.makeQualified(path);

    long value = 76859403;

    try (FSDataOutputStream output = fs.create(path)) {
      output.writeLong(value);
    }

    Path link = new Path("metadata://test/linkfile");
    FileSystem fileSystem = link.getFileSystem(_conf);
    fileSystem.create(link)
              .close();

    assertEquals(0, fileSystem.getFileStatus(link)
                              .getLen());

    fileSystem.setXAttr(link, "metadata.file.link", path.toString()
                                                        .getBytes());

    assertEquals(8, fileSystem.getFileStatus(link)
                              .getLen());

    try (FSDataInputStream input = fileSystem.open(link)) {
      assertEquals(value, input.readLong());
    }

    assertTrue(fileSystem.delete(link, false));
    assertFalse(fileSystem.exists(link));
    assertTrue(fs.exists(path));

  }

  private void touchFile(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(_conf);
    fileSystem.mkdirs(path.getParent());
    try (FSDataOutputStream input = fileSystem.create(path)) {

    }
  }

  private void assertNoFiles(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(_conf);
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
    if (iterator.hasNext()) {
      fail();
    }
  }

}
