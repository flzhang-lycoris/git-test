package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 1.获取登录用户信息
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        // 2.判断是否关注
        if(isFollow){
            // 2.1如果未关注插入数据
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            boolean isSuccess = save(follow);
            if(isSuccess){
                // 在redis存入中key为该用户，set集合的值为该用户关注的人
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            // 2.2如果已关注删除数据
            remove(new LambdaQueryWrapper<Follow>()
//                            .eq("user_id",userId)
//                            .eq("follow_user_id",followUserId)
                    .eq(Follow::getUserId,userId)
                    .eq(Follow::getFollowUserId,followUserId)
            );
            stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        // 1.获取登录用户信息
        Long userId = UserHolder.getUser().getId();
        // 2.查询是否关注
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        return Result.ok(count>0);
    }

    @Override
    public Result followCommons(Long id) {
        // 1.获取登录用户信息
        Long userId = UserHolder.getUser().getId();
        String key_login = "follows:" + userId;
        String key_looking = "follows:" + id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key_login, key_looking);
        if(intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //解析交集id集合
        List<Long> listIds = intersect.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        List<UserDTO> users = userService.listByIds(listIds).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}
