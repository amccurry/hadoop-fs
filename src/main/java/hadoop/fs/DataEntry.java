package hadoop.fs;

import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(toBuilder = true)
public class DataEntry {

  String dataPathUri;

  boolean managed;

  WriteState writeState;

  @JsonIgnore
  public Path getDataPath() {
    return new Path(dataPathUri);
  }

}
