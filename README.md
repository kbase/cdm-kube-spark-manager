# CDM Spark Cluster Manager API

A simple API service for managing Spark clusters in Kubernetes for JupyterHub users. This service acts as an intermediary between user pods and Kubernetes, providing a secure way to manage Spark clusters without requiring direct Kubernetes access.

## Features

- Create and manage Spark clusters for individual users
- RESTful API endpoints for cluster management
- Automatic cleanup of resources
- User authentication and authorization

## Setup Local Development Environment

### Prerequisites

- Python 3.11+
- pip

### Installation

1. Clone the repository:
   ```
   git clone git@github.com:kbase/cdm-kube-spark-manager.git
   cd cdm-kube-spark-manager
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Local Unit Testing

The project uses pytest for unit testing. To run the tests:

```bash
# Run all tests
PYTHONPATH=src pytest -v --cov=src --cov-report=xml tests
```

## API Endpoints

### Health Check
- `GET /health` - Check the health status of the service

### Cluster Management
- `POST /clusters` - Creates a new Spark cluster for the authenticated user
- `GET /clusters` - List the status of all Spark clusters for the authenticated user
- `DELETE /clusters` - Deletes Spark cluster belongs to the authenticated user.

## Kubernetes Deployment

For instructions on deploying to local Kubernetes, see the [Kubernetes README](kubernetes/README.md).

