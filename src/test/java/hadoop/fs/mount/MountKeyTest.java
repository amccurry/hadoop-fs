package hadoop.fs.mount;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class MountKeyTest {

  @Test
  public void testMountKeyCreate() {
    Path path = new Path("virt://test2/dst/test/1/");

    MountKey mountKey = MountKey.builder()
                                .authority("test2")
                                .scheme("virt")
                                .pathParts(Arrays.asList("", "dst", "test", "1"))
                                .build();

    assertEquals(mountKey, MountKey.create(path));
  }

  @Test
  public void testMountKeyParent() {
    Path path = new Path("virt://test2/dst/test/1/sub1/sub2");

    MountKey mountKey = MountKey.builder()
                                .authority("test2")
                                .scheme("virt")
                                .pathParts(Arrays.asList("", "dst", "test", "1"))
                                .build();

    MountKey mk = MountKey.create(path);

    assertEquals(mountKey, mk.getParentKey()
                             .getParentKey());
  }
}
