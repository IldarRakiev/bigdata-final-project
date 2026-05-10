// ORM class for table 'events'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 09 13:49:44 MSK 2026
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class events extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("event_type", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        events.this.event_type = (String)value;
      }
    });
    setters.put("repo_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        events.this.repo_id = (Long)value;
      }
    });
    setters.put("event_date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        events.this.event_date = (java.sql.Date)value;
      }
    });
    setters.put("event_count", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        events.this.event_count = (Integer)value;
      }
    });
    setters.put("unique_actors", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        events.this.unique_actors = (Integer)value;
      }
    });
  }
  public events() {
    init0();
  }
  private String event_type;
  public String get_event_type() {
    return event_type;
  }
  public void set_event_type(String event_type) {
    this.event_type = event_type;
  }
  public events with_event_type(String event_type) {
    this.event_type = event_type;
    return this;
  }
  private Long repo_id;
  public Long get_repo_id() {
    return repo_id;
  }
  public void set_repo_id(Long repo_id) {
    this.repo_id = repo_id;
  }
  public events with_repo_id(Long repo_id) {
    this.repo_id = repo_id;
    return this;
  }
  private java.sql.Date event_date;
  public java.sql.Date get_event_date() {
    return event_date;
  }
  public void set_event_date(java.sql.Date event_date) {
    this.event_date = event_date;
  }
  public events with_event_date(java.sql.Date event_date) {
    this.event_date = event_date;
    return this;
  }
  private Integer event_count;
  public Integer get_event_count() {
    return event_count;
  }
  public void set_event_count(Integer event_count) {
    this.event_count = event_count;
  }
  public events with_event_count(Integer event_count) {
    this.event_count = event_count;
    return this;
  }
  private Integer unique_actors;
  public Integer get_unique_actors() {
    return unique_actors;
  }
  public void set_unique_actors(Integer unique_actors) {
    this.unique_actors = unique_actors;
  }
  public events with_unique_actors(Integer unique_actors) {
    this.unique_actors = unique_actors;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof events)) {
      return false;
    }
    events that = (events) o;
    boolean equal = true;
    equal = equal && (this.event_type == null ? that.event_type == null : this.event_type.equals(that.event_type));
    equal = equal && (this.repo_id == null ? that.repo_id == null : this.repo_id.equals(that.repo_id));
    equal = equal && (this.event_date == null ? that.event_date == null : this.event_date.equals(that.event_date));
    equal = equal && (this.event_count == null ? that.event_count == null : this.event_count.equals(that.event_count));
    equal = equal && (this.unique_actors == null ? that.unique_actors == null : this.unique_actors.equals(that.unique_actors));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof events)) {
      return false;
    }
    events that = (events) o;
    boolean equal = true;
    equal = equal && (this.event_type == null ? that.event_type == null : this.event_type.equals(that.event_type));
    equal = equal && (this.repo_id == null ? that.repo_id == null : this.repo_id.equals(that.repo_id));
    equal = equal && (this.event_date == null ? that.event_date == null : this.event_date.equals(that.event_date));
    equal = equal && (this.event_count == null ? that.event_count == null : this.event_count.equals(that.event_count));
    equal = equal && (this.unique_actors == null ? that.unique_actors == null : this.unique_actors.equals(that.unique_actors));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.event_type = JdbcWritableBridge.readString(1, __dbResults);
    this.repo_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.event_date = JdbcWritableBridge.readDate(3, __dbResults);
    this.event_count = JdbcWritableBridge.readInteger(4, __dbResults);
    this.unique_actors = JdbcWritableBridge.readInteger(5, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.event_type = JdbcWritableBridge.readString(1, __dbResults);
    this.repo_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.event_date = JdbcWritableBridge.readDate(3, __dbResults);
    this.event_count = JdbcWritableBridge.readInteger(4, __dbResults);
    this.unique_actors = JdbcWritableBridge.readInteger(5, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(event_type, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(repo_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeDate(event_date, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(event_count, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(unique_actors, 5 + __off, 4, __dbStmt);
    return 5;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(event_type, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(repo_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeDate(event_date, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(event_count, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(unique_actors, 5 + __off, 4, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.event_type = null;
    } else {
    this.event_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.repo_id = null;
    } else {
    this.repo_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.event_date = null;
    } else {
    this.event_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.event_count = null;
    } else {
    this.event_count = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.unique_actors = null;
    } else {
    this.unique_actors = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.event_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, event_type);
    }
    if (null == this.repo_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.repo_id);
    }
    if (null == this.event_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.event_date.getTime());
    }
    if (null == this.event_count) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.event_count);
    }
    if (null == this.unique_actors) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.unique_actors);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.event_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, event_type);
    }
    if (null == this.repo_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.repo_id);
    }
    if (null == this.event_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.event_date.getTime());
    }
    if (null == this.event_count) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.event_count);
    }
    if (null == this.unique_actors) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.unique_actors);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(event_type==null?"null":event_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(repo_id==null?"null":"" + repo_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_date==null?"null":"" + event_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_count==null?"null":"" + event_count, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(unique_actors==null?"null":"" + unique_actors, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(event_type==null?"null":event_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(repo_id==null?"null":"" + repo_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_date==null?"null":"" + event_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_count==null?"null":"" + event_count, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(unique_actors==null?"null":"" + unique_actors, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.event_type = null; } else {
      this.event_type = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.repo_id = null; } else {
      this.repo_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_date = null; } else {
      this.event_date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_count = null; } else {
      this.event_count = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.unique_actors = null; } else {
      this.unique_actors = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.event_type = null; } else {
      this.event_type = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.repo_id = null; } else {
      this.repo_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_date = null; } else {
      this.event_date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_count = null; } else {
      this.event_count = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.unique_actors = null; } else {
      this.unique_actors = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    events o = (events) super.clone();
    o.event_date = (o.event_date != null) ? (java.sql.Date) o.event_date.clone() : null;
    return o;
  }

  public void clone0(events o) throws CloneNotSupportedException {
    o.event_date = (o.event_date != null) ? (java.sql.Date) o.event_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("event_type", this.event_type);
    __sqoop$field_map.put("repo_id", this.repo_id);
    __sqoop$field_map.put("event_date", this.event_date);
    __sqoop$field_map.put("event_count", this.event_count);
    __sqoop$field_map.put("unique_actors", this.unique_actors);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("event_type", this.event_type);
    __sqoop$field_map.put("repo_id", this.repo_id);
    __sqoop$field_map.put("event_date", this.event_date);
    __sqoop$field_map.put("event_count", this.event_count);
    __sqoop$field_map.put("unique_actors", this.unique_actors);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
