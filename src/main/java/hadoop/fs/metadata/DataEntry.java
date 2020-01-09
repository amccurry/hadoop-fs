package hadoop.fs.metadata;

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

  String managementId;

  String dataPathUri;

  boolean managed;

  @JsonIgnore
  public Path getDataPath() {
    return new Path(dataPathUri);
  }

}
