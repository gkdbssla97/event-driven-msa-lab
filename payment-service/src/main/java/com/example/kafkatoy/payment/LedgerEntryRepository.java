package com.example.kafkatoy.payment;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, String> {

    @Query("SELECT COALESCE(SUM(l.amount), 0) FROM LedgerEntry l WHERE l.orderId = :orderId")
    long sumAmountByOrderId(@Param("orderId") String orderId);
}
