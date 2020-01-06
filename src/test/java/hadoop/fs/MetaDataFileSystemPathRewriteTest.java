package hadoop.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import hadoop.fs.MetaDataFileSystem;

public class MetaDataFileSystemPathRewriteTest {

  private MetaDataFileSystem _fileSystem;

  @Before
  public void setup() throws Exception {
    _fileSystem = new MetaDataFileSystem();
    Configuration conf = new Configuration();
    conf.set("metadata.meta.path", "file:///meta");
    conf.set("metadata.data.path", "file:///data");
    _fileSystem.initialize(new URI("metadata://test"), conf);
  }

  @After
  public void after() throws Exception {
    _fileSystem.close();
  }

  @Test
  public void testGetVirtualPath1() throws Exception {
    Path virtualPath = _fileSystem.getVirtualPath(new Path("file:///meta/test1"));
    assertEquals(new Path("metadata://test/test1"), virtualPath);
  }

  @Test
  public void testGetVirtualPath2() throws Exception {
    Path virtualPath = _fileSystem.getVirtualPath(new Path("file:///meta/test1/"));
    assertEquals(new Path("metadata://test/test1/"), virtualPath);
  }

  @Test
  public void testGetVirtualPath3() throws Exception {
    Path virtualPath = _fileSystem.getVirtualPath(new Path("file:///meta/test1/sub1"));
    assertEquals(new Path("metadata://test/test1/sub1"), virtualPath);
  }

  @Test
  public void testGetVirtualPathError() throws Exception {
    try {
      _fileSystem.getVirtualPath(new Path("file:///metaerror/test1"));
      fail();
    } catch (IOException e) {
    }
  }

  @Test
  public void testGetMetaPath1() throws Exception {
    Path metaPath = _fileSystem.getMetaPath(new Path("metadata://test/test1"));
    assertEquals(new Path("file:///meta/test1"), metaPath);
  }

  @Test
  public void testGetMetaPath2() throws Exception {
    Path metaPath = _fileSystem.getMetaPath(new Path("metadata://test/test1/"));
    assertEquals(new Path("file:///meta/test1/"), metaPath);
  }

  @Test
  public void testGetMetaPath3() throws Exception {
    Path metaPath = _fileSystem.getMetaPath(new Path("metadata://test/test1/sub1"));
    assertEquals(new Path("file:///meta/test1/sub1"), metaPath);
  }

  @Test
  public void testGetMetaPathError() throws Exception {
    try {
      _fileSystem.getMetaPath(new Path("metadata://testerror/test1"));
      fail();
    } catch (IOException e) {
    }
  }

}
