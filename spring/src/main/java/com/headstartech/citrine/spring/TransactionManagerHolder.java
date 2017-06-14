package com.headstartech.citrine.spring;

import org.springframework.transaction.PlatformTransactionManager;

/**
 * Created by per on 13/01/16.
 */
class TransactionManagerHolder {

    private final PlatformTransactionManager transactionManager;

    public TransactionManagerHolder(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }
}
