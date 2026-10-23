# FDE L2 Assessment - Idempotent Background Transaction Processing Service

## Overview
This repository contains the solution for the FDE L2 practical assessment, implementing an idempotent background transaction processing service with out-of-order handling, duplicate detection, resilience/retry mechanisms, and status lifecycle tracking.

## Execution Instructions
1. Copy the contents of `Main.java`.
2. Paste and run the code in any online Java compiler (such as JDoodle, OnlineGDB, or Replit).

## Design Notes & Architecture
- **Idempotency & Duplicate Detection:** Uses a thread-safe `ConcurrentHashMap` and a `Set` (`processedRequestIds`) to track unique request IDs (`requestId`), ensuring requests are processed only once and preventing duplicate balance or ledger updates.
- **Out-of-Order Handling Strategy:** Implements a sequence-based buffering strategy. Incoming requests with future sequence numbers are held in a pending buffer and processed dynamically once preceding sequences complete.
- **Resilience & Retry Mechanism:** Simulates transient network failures (e.g., `TXN-2005`), safely retrying up to a maximum threshold without duplicate side effects before transitioning to a permanent `FAILED` state.
- **Observability:** Generates a structured processing summary report detailing counts by status (`PROCESSED`, `DUPLICATE`, `FAILED`, `RETRY_PENDING`).
-
