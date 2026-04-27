package com.example.kafkatoy.order;

import jakarta.validation.constraints.NotBlank;

public record OrderCreateRequest(@NotBlank String userId) {
}
