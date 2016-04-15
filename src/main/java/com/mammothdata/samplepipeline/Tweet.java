package com.mammothdata.samplepipeline;

import java.io.Serializable;

/**
 * Created by dnelson on 4/13/16.
 */
public class Tweet implements Serializable {
  public Long id;

  public String user_name = new String();
  public Long user_id;

  public String from_user = new String();

  public String source = new String();
  public String created_at = new String();

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  public String getUser_name() {
    return user_name;
  }

  public Long getUser_id() {
    return user_id;
  }

  public String getFrom_user() {
    return from_user;
  }

  public String getSource() {
    return source;
  }

  public String getCreated_at() {
    return created_at;
  }

  public void setUser_name(String user_name) {
    this.user_name = user_name;
  }

  public void setUser_id(Long user_id) {
    this.user_id = user_id;
  }

  public void setFrom_user(String from_user) {
    this.from_user = from_user;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public void setCreated_at(String created_at) {
    this.created_at = created_at;
  }

  public String getDay() {
    return this.created_at.substring(0,10);
  }
}
