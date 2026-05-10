// ORM class for table 'repositories'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 09 13:48:51 MSK 2026
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

public class repositories extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("repo_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        repositories.this.repo_id = (Long)value;
      }
    });
    setters.put("repo_name", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        repositories.this.repo_name = (String)value;
      }
    });
    setters.put("first_seen_at", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        repositories.this.first_seen_at = (java.sql.Timestamp)value;
      }
    });
    setters.put("language", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        repositories.this.language = (String)value;
      }
    });
  }
  public repositories() {
    init0();
  }
  private Long repo_id;
  public Long get_repo_id() {
    return repo_id;
  }
  public void set_repo_id(Long repo_id) {
    this.repo_id = repo_id;
  }
  public repositories with_repo_id(Long repo_id) {
    this.repo_id = repo_id;
    return this;
  }
  private String repo_name;
  public String get_repo_name() {
    return repo_name;
  }
  public void set_repo_name(String repo_name) {
    this.repo_name = repo_name;
  }
  public repositories with_repo_name(String repo_name) {
    this.repo_name = repo_name;
    return this;
  }
  private java.sql.Timestamp first_seen_at;
  public java.sql.Timestamp get_first_seen_at() {
    return first_seen_at;
  }
  public void set_first_seen_at(java.sql.Timestamp first_seen_at) {
    this.first_seen_at = first_seen_at;
  }
  public repositories with_first_seen_at(java.sql.Timestamp first_seen_at) {
    this.first_seen_at = first_seen_at;
    return this;
  }
  private String language;
  public String get_language() {
    return language;
  }
  public void set_language(String language) {
    this.language = language;
  }
  public repositories with_language(String language) {
    this.language = language;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof repositories)) {
      return false;
    }
    repositories that = (repositories) o;
    boolean equal = true;
    equal = equal && (this.repo_id == null ? that.repo_id == null : this.repo_id.equals(that.repo_id));
    equal = equal && (this.repo_name == null ? that.repo_name == null : this.repo_name.equals(that.repo_name));
    equal = equal && (this.first_seen_at == null ? that.first_seen_at == null : this.first_seen_at.equals(that.first_seen_at));
    equal = equal && (this.language == null ? that.language == null : this.language.equals(that.language));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof repositories)) {
      return false;
    }
    repositories that = (repositories) o;
    boolean equal = true;
    equal = equal && (this.repo_id == null ? that.repo_id == null : this.repo_id.equals(that.repo_id));
    equal = equal && (this.repo_name == null ? that.repo_name == null : this.repo_name.equals(that.repo_name));
    equal = equal && (this.first_seen_at == null ? that.first_seen_at == null : this.first_seen_at.equals(that.first_seen_at));
    equal = equal && (this.language == null ? that.language == null : this.language.equals(that.language));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.repo_id = JdbcWritableBridge.readLong(1, __dbResults);
    this.repo_name = JdbcWritableBridge.readString(2, __dbResults);
    this.first_seen_at = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.language = JdbcWritableBridge.readString(4, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.repo_id = JdbcWritableBridge.readLong(1, __dbResults);
    this.repo_name = JdbcWritableBridge.readString(2, __dbResults);
    this.first_seen_at = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.language = JdbcWritableBridge.readString(4, __dbResults);
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
    JdbcWritableBridge.writeLong(repo_id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(repo_name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(first_seen_at, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(language, 4 + __off, 12, __dbStmt);
    return 4;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(repo_id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(repo_name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(first_seen_at, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(language, 4 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.repo_id = null;
    } else {
    this.repo_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.repo_name = null;
    } else {
    this.repo_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.first_seen_at = null;
    } else {
    this.first_seen_at = new Timestamp(__dataIn.readLong());
    this.first_seen_at.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.language = null;
    } else {
    this.language = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.repo_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.repo_id);
    }
    if (null == this.repo_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, repo_name);
    }
    if (null == this.first_seen_at) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.first_seen_at.getTime());
    __dataOut.writeInt(this.first_seen_at.getNanos());
    }
    if (null == this.language) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, language);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.repo_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.repo_id);
    }
    if (null == this.repo_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, repo_name);
    }
    if (null == this.first_seen_at) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.first_seen_at.getTime());
    __dataOut.writeInt(this.first_seen_at.getNanos());
    }
    if (null == this.language) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, language);
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
    __sb.append(FieldFormatter.escapeAndEnclose(repo_id==null?"null":"" + repo_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(repo_name==null?"null":repo_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(first_seen_at==null?"null":"" + first_seen_at, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(language==null?"null":language, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(repo_id==null?"null":"" + repo_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(repo_name==null?"null":repo_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(first_seen_at==null?"null":"" + first_seen_at, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(language==null?"null":language, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.repo_id = null; } else {
      this.repo_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.repo_name = null; } else {
      this.repo_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.first_seen_at = null; } else {
      this.first_seen_at = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.language = null; } else {
      this.language = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.repo_id = null; } else {
      this.repo_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.repo_name = null; } else {
      this.repo_name = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.first_seen_at = null; } else {
      this.first_seen_at = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.language = null; } else {
      this.language = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    repositories o = (repositories) super.clone();
    o.first_seen_at = (o.first_seen_at != null) ? (java.sql.Timestamp) o.first_seen_at.clone() : null;
    return o;
  }

  public void clone0(repositories o) throws CloneNotSupportedException {
    o.first_seen_at = (o.first_seen_at != null) ? (java.sql.Timestamp) o.first_seen_at.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("repo_id", this.repo_id);
    __sqoop$field_map.put("repo_name", this.repo_name);
    __sqoop$field_map.put("first_seen_at", this.first_seen_at);
    __sqoop$field_map.put("language", this.language);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("repo_id", this.repo_id);
    __sqoop$field_map.put("repo_name", this.repo_name);
    __sqoop$field_map.put("first_seen_at", this.first_seen_at);
    __sqoop$field_map.put("language", this.language);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
