package hadoop.fs.metadata;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import hadoop.fs.metadata.MetaDataFileSystem;
import hadoop.fs.metadata.MetaEntry;

public class MetaDataFileSystemPathRewriteTest {

  private MetaDataFileSystem _fileSystem;

  @Before
  public void setup() throws Exception {
    _fileSystem = new MetaDataFileSystem();
    Configuration conf = new Configuration();
    conf.set("metadata.test.meta.path", "file:///meta");
    conf.set("metadata.test.data.path", "file:///data");
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
    MetaEntry metaEntry = _fileSystem.getMetaEntry(new Path("metadata://test/test1"));
    assertEquals(new Path("file:///meta/test1"), metaEntry.getMetaPath());
  }

  @Test
  public void testGetMetaPath2() throws Exception {
    MetaEntry metaEntry = _fileSystem.getMetaEntry(new Path("metadata://test/test1/"));
    assertEquals(new Path("file:///meta/test1/"), metaEntry.getMetaPath());
  }

  @Test
  public void testGetMetaPath3() throws Exception {
    MetaEntry metaEntry = _fileSystem.getMetaEntry(new Path("metadata://test/test1/sub1"));
    assertEquals(new Path("file:///meta/test1/sub1"), metaEntry.getMetaPath());
  }

  @Test
  public void testGetMetaPathError() throws Exception {
    try {
      _fileSystem.getMetaEntry(new Path("metadata://testerror/test1"));
      fail();
    } catch (IOException e) {
    }
  }

}
