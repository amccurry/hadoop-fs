package hadoop.fs.mount;

import java.io.IOException;

public interface MountFactory {

  MountFactory DEFAULT = new MountFactory() {

  };

  default void initialize() {

  }

  default Mount findMount(MountKey mountKey) throws IOException {
    return null;
  }

}
