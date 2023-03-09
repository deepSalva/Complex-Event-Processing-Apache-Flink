package Stream.data;

public class KeyedDataPoint<T> extends DataPoint<T> {

  private String key;

  public KeyedDataPoint(){
    super();
    this.key = null;
  }

  public KeyedDataPoint(String key, long timeStampMs, T value) {
    super(timeStampMs, value);
    this.key = key;
  }
  public KeyedDataPoint(String key, long timeStampMs, T value, T value2) {
    super(timeStampMs, value, value2);
    this.key = key;
  }
  public KeyedDataPoint(String key, long timeStampMs, T value, T value2, String type) {
    super(timeStampMs, value, value2, type);
    this.key = key;
  }

  @Override
  public String toString() {
    return getTimeStampMs() + "," + getKey() + "," + getValue() + "," + getValue2() + "," + getType();
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public <R> KeyedDataPoint<R> withNewValue(R newValue, R newValue2, String newType){
    return new KeyedDataPoint<>(this.getKey(), this.getTimeStampMs(), newValue, newValue2, newType);
  }

}
