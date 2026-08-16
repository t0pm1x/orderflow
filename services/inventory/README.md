# Inventory Service

Handles stock reservations: `POST /v1/inventory/reserve` decrements `Stock` rows
with optimistic locking and writes a TTL-bound reservation to Redis. Consumes
saga events to release/confirm reservations.

See `C:\Users\t0p_m\docs\superpowers\portfolio\orderflow-substages.md` sub-stages 3.6.a-f.
