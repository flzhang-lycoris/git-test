package com.hmdp.mapper;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 */
public interface UserMapper extends BaseMapper<User> {

    List<User> listIds(@Param("ids") List<Long> ids);
}
