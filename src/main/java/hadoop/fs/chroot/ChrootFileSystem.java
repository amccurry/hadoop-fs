package hadoop.fs.chroot;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

import hadoop.fs.base.ContextFileSystem;

public class ChrootFileSystem extends ContextFileSystem {

  private URI _uri;
  private Chroot _chroot = new NoChroot();

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    setConf(conf);
    _uri = name;
    Class<? extends Chroot> chrootClass = conf.getClass(getConfigPrefix() + ".class", BasicChroot.class, Chroot.class);
    _chroot = ReflectionUtils.newInstance(chrootClass, conf);
    _chroot.initialize(conf, this);
  }

  @Override
  public String getScheme() {
    return "chroot";
  }

  @Override
  public URI getUri() {
    return _uri;
  }

  @Override
  protected Path getOriginalPath(Path realPath) throws IOException {
    return _chroot.getChrootPath(realPath);
  }

  @Override
  protected Path getContextPath(Path chrootPath) throws IOException {
    return _chroot.getRealPath(chrootPath);
  }

}
