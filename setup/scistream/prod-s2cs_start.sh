#!/bin/bash
oc create -f prod_scistream_dcon_np.yaml
sleep 5
oc exec prod-scistream-pod -c prod-scistream-container -- ls -l /certs
oc cp prod-scistream-pod:/certs/prod-server.crt ./prod-server.crt
