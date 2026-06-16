package com.example.kafkatoy.inventory;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/inventory")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping("/{productId}")
    public ResponseEntity<Map<String, Object>> getStock(@PathVariable String productId) {
        long stock = inventoryService.getStock(productId);
        return ResponseEntity.ok(Map.of("productId", productId, "stock", stock));
    }

    @PostMapping("/{productId}/init")
    public ResponseEntity<Map<String, Object>> initStock(@PathVariable String productId,
                                                          @RequestParam(defaultValue = "100") int stock) {
        inventoryService.initStock(productId, stock);
        return ResponseEntity.ok(Map.of("productId", productId, "stock", stock, "message", "initialized"));
    }
}
