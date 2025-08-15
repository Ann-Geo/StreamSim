#!/bin/bash
oc create -f cons_scistream_dcon_np.yaml
sleep 5
oc exec cons-scistream-pod -c cons-scistream-container -- ls -l /certs
oc cp cons-scistream-pod:/certs/cons-server.crt ./cons-server.crt
