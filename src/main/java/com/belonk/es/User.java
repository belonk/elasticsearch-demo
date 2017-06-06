package com.belonk.es;

import com.alibaba.fastjson.JSON;

/**
 * Created by sun on 2017/6/6.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class User {
    //~ Static fields/initializers =====================================================================================


    //~ Instance fields ================================================================================================
    private Long id;
    private String username;
    private String password;
    private Integer age;
    private Integer gender;
    private String[] hovers;

    //~ Constructors ===================================================================================================
    public User() {
    }

    public User(Long id, String username, String password, Integer age, Integer gender, String[] hovers) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.age = age;
        this.gender = gender;
        this.hovers = hovers;
    }
    //~ Methods ========================================================================================================

    public String toJson() {
        return JSON.toJSONString(this);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getGender() {
        return gender;
    }

    public void setGender(Integer gender) {
        this.gender = gender;
    }

    public String[] getHovers() {
        return hovers;
    }

    public void setHovers(String[] hovers) {
        this.hovers = hovers;
    }
}
