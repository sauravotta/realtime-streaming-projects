#!/usr/bin/env bash
# Sends three orders: success, payment failure (amount too high), inventory failure (out of stock).
set -euo pipefail
URL=${URL:-http://localhost:8080}
post() { curl -s -X POST "$URL/orders" -H 'Content-Type: application/json' -d "$1"; echo; }
get_id() { python3 -c 'import sys,json;print(json.load(sys.stdin)["orderId"])'; }

OK=$(post '{"customerId":"c1","sku":"BOOK-1","quantity":2,"amount":40}' | tee /dev/stderr | get_id)
PAY=$(post '{"customerId":"c2","sku":"BOOK-1","quantity":1,"amount":5000}' | tee /dev/stderr | get_id)
STK=$(post '{"customerId":"c3","sku":"LAPTOP-1","quantity":1,"amount":900}' | tee /dev/stderr | get_id)

sleep 5
for id in "$OK" "$PAY" "$STK"; do curl -s "$URL/orders/$id"; echo; done
