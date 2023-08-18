#!/bin/bash

kubeconfig_file=/projects/k8s_config_file.yaml
cabundle_file=certificate/cabundle.pem
clientcert_file=certificate/x509up_u1000

# Retrieving pods
echo "RETRIEVING PODS"
prefix="pod/"
staging_pod_fullname=$(kubectl --kubeconfig $kubeconfig_file get pod -n jh-staging-system -o name | grep staging-hub)
production_pod_fullname=$(kubectl --kubeconfig $kubeconfig_file get pod -n jh-prod-system -o name | grep prod-hub)
staging_pod=${staging_pod_fullname#"$prefix"}
production_pod=${production_pod_fullname#"$prefix"}
echo Staging pod : ${staging_pod}
echo Production pod : ${staging_pod}

# UPLOAD
# Staging
echo
echo STAGING UPLOAD ...
echo Uploading cabundle file \'$cabundle_file\' to staging
kubectl --kubeconfig $kubeconfig_file cp -n jh-staging-system $cabundle_file "${staging_pod}:/downloadservice-data/dcache_cabundle.pem"
echo Uploading clientcert file \'$clientcert_file\' to staging
kubectl --kubeconfig $kubeconfig_file cp -n jh-staging-system $clientcert_file "${staging_pod}:/downloadservice-data/dcache_clientcert.crt"

# Production
echo
echo PRODUCTION UPLOAD ...
echo Uploading cabundle file \'$cabundle_file\' to production
kubectl --kubeconfig $kubeconfig_file cp -n jh-prod-system $cabundle_file "${production_pod}:/downloadservice-data/dcache_cabundle.pem"
echo Uploading clientcert file \'$clientcert_file\' to production
kubectl --kubeconfig $kubeconfig_file cp -n jh-prod-system $clientcert_file "${production_pod}:/downloadservice-data/dcache_clientcert.crt"

# Finish
echo
echo Upload complete !
