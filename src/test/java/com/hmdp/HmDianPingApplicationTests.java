package com.hmdp;

import cn.hutool.json.JSONUtil;
import com.hmdp.service.impl.ShopServiceImpl;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @org.junit.jupiter.api.Test
    void testSaveShop() {
        shopService.saveShop2Redis(1L, 5L);
    }
}
