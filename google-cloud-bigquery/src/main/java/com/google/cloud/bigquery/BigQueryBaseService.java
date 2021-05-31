package com.google.cloud.bigquery;

import com.google.cloud.ExceptionHandler;
import com.google.cloud.ServiceOptions;
import com.google.cloud.BaseService;


abstract class BigQueryBaseService<OptionsT extends ServiceOptions<?, OptionsT>> extends BaseService{

    protected BigQueryBaseService(ServiceOptions options) {
        super(options);
    }

    public static final ExceptionHandler BIGQUERY_EXCEPTION_HANDLER =
            ExceptionHandler.newBuilder()
                    .abortOn(RuntimeException.class)
                    .retryOn(java.net.ConnectException.class)//retry on Connection Exception
                    .retryOn(java.net.UnknownHostException.class)//retry on UnknownHostException
                    .addInterceptors(EXCEPTION_HANDLER_INTERCEPTOR)
                    .build();
}
