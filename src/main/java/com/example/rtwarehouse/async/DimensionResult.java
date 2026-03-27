package com.example.rtwarehouse.async;

/**
 * 维度查询结果类
 * TODO: 后续可扩展为包含更多维度信息字段
 */
public class DimensionResult {
    private final Long userId;
    private final String dimensionInfo;

    /**
     * 构造函数
     * @param userId
     * @param dimensionInfo
     */
    public DimensionResult(Long userId, String dimensionInfo) {
        this.userId = userId;
        this.dimensionInfo = dimensionInfo;
    }

    /**
     * 获取用户ID
     * @return
     */
    public Long getUserId() {
        return userId;
    }

    

    /**
     * 获取维度信息
     * @return
     */
    public String getDimensionInfo() {
        return dimensionInfo;
    }
}
