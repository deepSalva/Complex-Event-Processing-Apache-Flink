package Stream.data;

public class DataPoint<T> {

  private long timeStampMs;
  private T value;
  private T value2;
  private String type;

  public DataPoint() {
    this.timeStampMs = 0;
    this.value = null;
  }

  public DataPoint(long timeStampMs, T value) {
    this.timeStampMs = timeStampMs;
    this.value = value;
  }

  public DataPoint(long timeStampMs, T value, T value2) {
    this.timeStampMs = timeStampMs;
    this.value = value;
    this.value2 = value2;
  }
  public DataPoint(long timeStampMs, T value, T value2, String type) {
    this.timeStampMs = timeStampMs;
    this.value = value;
    this.value2 = value2;
    this.type = type;
  }


  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public T getValue2() {
    return value2;
  }

  public void setValue2(T value2) {
    this.value2 = value2;
  }

  public long getTimeStampMs() {
    return timeStampMs;
  }

  public void setTimeStampMs(long timeStampMs) {
    this.timeStampMs = timeStampMs;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public <R> DataPoint<R> withNewValue(R newValue, R newValue2, String newType){
    return new DataPoint<>(this.getTimeStampMs(), newValue, newValue2, newType);
  }

  public <R> KeyedDataPoint<R> withNewKeyAndValue(String key, R newValue, R newValue2, String newType){
    return new KeyedDataPoint<>(key, this.getTimeStampMs(), newValue, newValue2, newType);
  }

  public KeyedDataPoint withKey(String key){
    return new KeyedDataPoint<>(key, this.getTimeStampMs(), this.getValue(), this.getValue2(), this.getType());
  }

  @Override
  public String toString() {
    return "DataPoint(timestamp = " + timeStampMs + ", value = " + value + ", value2 = " + value2 +", type = "+type+")";
  }
}
