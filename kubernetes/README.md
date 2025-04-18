# Kubernetes Deployment Guide

This directory contains the Kubernetes manifest files needed to deploy the CDM Spark Cluster Manager API to a Kubernetes cluster.

> **⚠️ Important:** This deployment has only been tested with Minikube. Other Kubernetes environments like Rancher or Docker Desktop might encounter errors.

## Prerequisites

- Minikube installed and running
- kubectl configured to use your Minikube cluster
- Docker for building the container image

## Deployment Steps

### 1. Build the Docker Image

From the project root directory:

```bash
# Build the image
docker build -t cdm-kube-spark-manager:latest .

# Load the image into Minikube (required since we're using imagePullPolicy: Never)
minikube image load cdm-kube-spark-manager:latest
```

### 2. Create the Namespace

```bash
kubectl create namespace cdm-jupyterhub
```

### 3. Deploy RBAC Resources

```bash
kubectl apply -f kubernetes/rbac.yaml
```

### 4. Create ConfigMap

Apply the ConfigMap:
```bash
kubectl apply -f kubernetes/configmap.yaml
```
### 5. Deploy the Application

```bash
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/service.yaml
```

### 6. Verify Deployment

```bash
# Check if pods are running
kubectl get pods -n cdm-jupyterhub

# Check service
kubectl get svc -n cdm-jupyterhub

# Access the service via Minikube
minikube service cdm-spark-cluster-manager-api -n cdm-jupyterhub
```

## Cleanup

To remove the deployment:

```bash
kubectl delete -f kubernetes/service.yaml
kubectl delete -f kubernetes/deployment.yaml
kubectl delete -f kubernetes/configmap.yaml
kubectl delete -f kubernetes/rbac.yaml
kubectl delete namespace cdm-jupyterhub
``` 