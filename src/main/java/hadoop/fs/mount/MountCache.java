package hadoop.fs.mount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MountCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(MountCache.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String PATH_SUFFIX = ".path";
  private static final String PERIOD_SUFFIX = ".period";
  private static final String DELAY_SUFFIX = ".delay";
  private static final String AUTOMATIC_UPDATES_DISABLED_SUFFIX = ".automatic.updates.disabled";
  private static final String DEFAULT_MOUNT_SUFFIX = ".default.mount";

  private static MountCache INSTANCE;

  public synchronized static MountCache getInstance(Configuration conf, String configPrefix, Path rootMountFsPath)
      throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new MountCache(conf, configPrefix, rootMountFsPath);
    }
    return INSTANCE;
  }

  private final AtomicReference<Map<MountKey, MountPathRewrite>> _mountsRef = new AtomicReference<>(
      new ConcurrentHashMap<>());
  private final Timer _timer;
  private final Path _mountPath;
  private final Configuration _conf;
  private final MountPathRewrite _defaultMount;

  private MountCache(Configuration conf, String configPrefix, Path rootMountFsPath) throws IOException {
    _timer = new Timer("mount-reload", true);
    _conf = conf;

    String defaultMount = conf.get(configPrefix + DEFAULT_MOUNT_SUFFIX);
    Path defaultMountPath = getDefaultMountPath(conf, defaultMount);
    _defaultMount = new MountPathRewrite(defaultMountPath, rootMountFsPath);
    boolean disableAutomaticUpdates = _conf.getBoolean(configPrefix + AUTOMATIC_UPDATES_DISABLED_SUFFIX, false);
    long delay = _conf.getLong(configPrefix + DELAY_SUFFIX, TimeUnit.SECONDS.toMillis(1));
    long period = _conf.getLong(configPrefix + PERIOD_SUFFIX, TimeUnit.MINUTES.toMillis(5));
    String configName = configPrefix + PATH_SUFFIX;
    String mountPathStr = _conf.get(configName);
    if (mountPathStr == null) {
      throw new IllegalArgumentException("Config param " + configName + " missing");
    }
    Path mountPath = new Path(mountPathStr);
    FileSystem fileSystem = mountPath.getFileSystem(conf);
    _mountPath = fileSystem.makeQualified(mountPath);
    if (!disableAutomaticUpdates) {
      _timer.schedule(getTimerTask(), delay, period);
    }
  }

  public void reloadMounts() throws IOException {
    FileSystem fileSystem = _mountPath.getFileSystem(_conf);
    if (fileSystem.exists(_mountPath)) {
      loadCache(fileSystem);
    }
  }

  private void loadCache(FileSystem fileSystem) throws IOException, JsonParseException, JsonMappingException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(_mountPath)))) {
      String line;
      ConcurrentHashMap<MountKey, MountPathRewrite> mounts = new ConcurrentHashMap<>();
      while ((line = reader.readLine()) != null) {
        if (!line.trim()
                 .isEmpty()) {
          MountEntry mountEntry = OBJECT_MAPPER.readValue(line, MountEntry.class);
          MountPathRewrite mountPathRewrite = new MountPathRewrite(mountEntry.getSrcPath(), mountEntry.getDstPath());
          mounts.put(mountPathRewrite.getMountKey(), mountPathRewrite);
        }
      }
      _mountsRef.set(mounts);
    }
  }

  public Mount getMount(Path path) {
    MountKey mountKey = MountKey.create(path);
    do {
      Map<MountKey, MountPathRewrite> mounts = _mountsRef.get();
      Mount mount = mounts.get(mountKey);
      if (mount != null) {
        return mount;
      }
    } while ((mountKey = mountKey.getParentKey()) != null);
    return _defaultMount;
  }

  private TimerTask getTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          reloadMounts();
        } catch (Exception e) {
          LOGGER.error("Unknown error", e);
        }
      }
    };
  }

  private Path getDefaultMountPath(Configuration conf, String defaultMount) throws IOException {
    Path path = new Path(defaultMount);
    FileSystem fileSystem = path.getFileSystem(conf);
    path = fileSystem.makeQualified(path);
    URI uri = path.toUri();
    return new Path(uri.getScheme(), uri.getAuthority(), "/");
  }

  public void addMount(Path srcPath, Path dstPath) {
    Map<MountKey, MountPathRewrite> mounts = _mountsRef.get();
    MountPathRewrite mountPathRewrite = new MountPathRewrite(srcPath, dstPath);
    mounts.put(mountPathRewrite.getMountKey(), mountPathRewrite);
  }
}
