package com.example.rtwarehouse.async;

import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import java.util.Collections;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * 异步维度查询函数
 * TODO: 后续可扩展为支持多种维度查询
 * TODO: 可添加超时处理机制
 */
public class AsyncDimensionLookupFunction extends AsyncTableFunction<DimensionResult> {
    private transient DimensionClient client;

    /**
     * 初始化函数
     * @param context 
     * @throws Exception
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        client = new DimensionClient();
    }

    /**
     * 执行异步查询
     * @param resultFuture
     * @param userId
     * TODO: 可支持更多参数，如维度类型等
     */
    public void eval(CompletableFuture<Collection<DimensionResult>> resultFuture, Long userId) {
        client.queryAsync(userId, dimension -> {
            resultFuture.complete(Collections.singleton(dimension));
        });
    }

    /**
     * 关闭函数
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (client != null) {
            client.close();
        }
    }
}
