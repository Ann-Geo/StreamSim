#!/bin/bash

# --- Step 1: Run inbound request ---
INBOUND_CMD=(s2uc inbound-request
  --server_cert=../certs/cons-server.crt
  --remote_ip 10.34.18.190
  --s2cs 128.219.128.160:30700
  --receiver_ports 5672
  --num_conn 1
)

echo "Running inbound request..."
INBOUND_OUTPUT=$("${INBOUND_CMD[@]}" 2>&1)
echo "$INBOUND_OUTPUT"

# --- Step 2: Extract UID from 2nd line of output ---
REQ_UID=$(echo "$INBOUND_OUTPUT" | awk 'NR==2 {print $1}')
LISTENER_LINE=$(echo "$INBOUND_OUTPUT" | grep -oE "10\.[0-9]+\.[0-9]+\.[0-9]+:510[0-9]" | head -n 1)
LISTENER_IP=$(echo "$LISTENER_LINE" | cut -d: -f1)
LISTENER_PORT=$(echo "$LISTENER_LINE" | cut -d: -f2)

if [[ -z "$REQ_UID" || -z "$LISTENER_PORT" ]]; then
  echo "Error: Failed to extract UID or listener port."
  exit 1
fi

# --- Step 3: Map port 5100–5110 to 3710–3720 ---
MAPPED_PORT=$((LISTENER_PORT - 5100 + 30710))

# --- Step 4: Run outbound request ---
OUTBOUND_CMD=(s2uc outbound-request
  --server_cert=../certs/prod-server.crt
  --remote_ip 128.219.128.160
  --s2cs 128.219.128.160:30500
  --receiver_ports "$MAPPED_PORT"
  --num_conn 1
  "$REQ_UID"
  "128.219.128.160:$MAPPED_PORT"
)

echo "Running outbound request..."
echo "${OUTBOUND_CMD[@]}"
"${OUTBOUND_CMD[@]}"

