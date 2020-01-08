package hadoop.fs.chroot;

import static org.junit.Assert.assertEquals;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class BasicChrootRootOnlyTest {

  private BasicChroot _basicChroot;

  @Before
  public void setup() throws Exception {
    URI chrootUri = new URI("chroot://test/");
    URI realFsUri = new URI("test://real");
    Path realPath = new Path("test://real/");
    _basicChroot = new BasicChroot();
    _basicChroot.initialize(chrootUri, realFsUri, realPath);
  }

  @Test
  public void testBasicChroot1() throws Exception {
    Path realPath = _basicChroot.getRealPath(new Path("chroot://test/"));
    assertEquals(new Path("test://real/"), realPath);
  }

  @Test
  public void testBasicChroot2() throws Exception {
    Path realPath = _basicChroot.getChrootPath(new Path("test://real/"));
    assertEquals(new Path("chroot://test/"), realPath);
  }

  @Test
  public void testBasicChroot3() throws Exception {
    Path realPath = _basicChroot.getRealPath(new Path("chroot://test/sub1"));
    assertEquals(new Path("test://real/sub1"), realPath);
  }

  @Test
  public void testBasicChroot4() throws Exception {
    Path realPath = _basicChroot.getChrootPath(new Path("test://real/sub1"));
    assertEquals(new Path("chroot://test/sub1"), realPath);
  }

  @Test
  public void testBasicChroot5() throws Exception {
    Path realPath = _basicChroot.getRealPath(new Path("chroot://test/sub1/sub2"));
    assertEquals(new Path("test://real/sub1/sub2"), realPath);
  }

  @Test
  public void testBasicChroot6() throws Exception {
    Path realPath = _basicChroot.getChrootPath(new Path("test://real/sub1/sub2"));
    assertEquals(new Path("chroot://test/sub1/sub2"), realPath);
  }
}
