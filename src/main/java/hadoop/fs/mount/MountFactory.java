package hadoop.fs.mount;

public interface MountFactory {

  MountFactory DEFAULT = new MountFactory() {

  };

  default void initialize() {

  }

  default Mount findMount(MountKey mountKey) {
    return null;
  }

}
