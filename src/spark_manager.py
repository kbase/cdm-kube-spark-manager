import logging
import os
import uuid
from typing import Dict, List, Optional

import kubernetes as k8s
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkClusterManager:
    """
    Manager for user-specific Spark clusters in Kubernetes.
    This class provides methods to create, manage, and destroy Spark clusters
    for individual users through the API.
    """

    # Default configuration values for cluster settings
    DEFAULT_WORKER_COUNT = 2
    DEFAULT_WORKER_CORES = 10
    DEFAULT_WORKER_MEMORY = "10G"
    DEFAULT_MASTER_CORES = 10
    DEFAULT_MASTER_MEMORY = "10G"
    DEFAULT_IMAGE_PULL_POLICY = "IfNotPresent"
    DEFAULT_EXECUTOR_CORES = 2
    DEFAULT_MAX_CORES_PER_APPLICATION = 10
    DEFAULT_MAX_EXECUTORS = 5
    DEFAULT_MASTER_PORT = 7077
    DEFAULT_MASTER_WEBUI_PORT = 8090
    DEFAULT_WORKER_WEBUI_PORT = 8081

    def __init__(self,
                 username: str,
                 namespace: str,
                 image: str,
                 image_pull_policy: str = DEFAULT_IMAGE_PULL_POLICY):
        """
        Initialize the SparkClusterManager with user-specific configuration.

        Args:
            username: Username of the authenticated user
            namespace: Kubernetes namespace for Spark clusters
            image: Docker image for Spark master and workers
            image_pull_policy: Image pull policy for containers
        """
        self.username = username
        self.namespace = namespace
        self.image = image
        self.image_pull_policy = image_pull_policy

        # Generate a unique identifier for this user's Spark cluster
        self.cluster_id = f"spark-{username.lower()}-{str(uuid.uuid4())[:8]}"

        # Service names
        self.master_name = f"spark-master-{username.lower()}"
        self.worker_name = f"spark-worker-{username.lower()}"

        # Initialize Kubernetes client
        k8s.config.load_incluster_config()
        self.core_api = k8s.client.CoreV1Api()
        self.apps_api = k8s.client.AppsV1Api()

        logger.info(f"Initialized SparkClusterManager for user {username} in namespace {namespace}")

    def create_cluster(self,
                      worker_count: int = DEFAULT_WORKER_COUNT,
                      worker_cores: int = DEFAULT_WORKER_CORES,
                      worker_memory: str = DEFAULT_WORKER_MEMORY,
                      master_cores: int = DEFAULT_MASTER_CORES,
                      master_memory: str = DEFAULT_MASTER_MEMORY) -> Dict:
        """
        Create a new Spark cluster for the user.

        Args:
            worker_count: Number of Spark worker replicas
            worker_cores: Number of CPU cores for each worker
            worker_memory: Memory allocation for each worker
            master_cores: Number of CPU cores for the master
            master_memory: Memory allocation for the master

        Returns:
            Dict containing cluster information including master URL
        """
        try:
            # Create the Spark master deployment and service
            self._create_master_deployment(master_cores, master_memory)
            self._create_master_service()

            # Create the Spark worker deployment
            self._create_worker_deployment(worker_count, worker_cores, worker_memory)

            # Return cluster information
            master_url = f"spark://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_PORT}"
            master_ui_url = f"http://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_WEBUI_PORT}"

            return {
                "cluster_id": self.cluster_id,
                "master_url": master_url,
                "master_ui_url": master_ui_url,
                "status": "creating"
            }

        except ApiException as e:
            logger.error(f"Error creating Spark cluster: {e}")
            raise

    def _create_master_deployment(self, cores: int, memory: str):
        """Create the Spark master deployment."""
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": self.master_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "spark",
                    "component": "master",
                    "user": self.username,
                    "cluster-id": self.cluster_id
                }
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "app": "spark",
                        "component": "master",
                        "user": self.username,
                        "cluster-id": self.cluster_id
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "spark",
                            "component": "master",
                            "user": self.username,
                            "cluster-id": self.cluster_id
                        }
                    },
                    "spec": {
                        "hostname": f"spark-master-{self.username.lower()}",
                        "containers": [{
                            "name": "spark-master",
                            "image": self.image,
                            "imagePullPolicy": self.image_pull_policy,
                            "ports": [
                                {"containerPort": self.DEFAULT_MASTER_PORT, "name": "master-comm", "protocol": "TCP"},
                                {"containerPort": self.DEFAULT_MASTER_WEBUI_PORT, "name": "master-ui", "protocol": "TCP"}
                            ],
                            "env": [
                                {"name": "SPARK_MODE", "value": "master"},
                                {"name": "SPARK_MASTER_PORT", "value": str(self.DEFAULT_MASTER_PORT)},
                                {"name": "SPARK_MASTER_WEBUI_PORT", "value": str(self.DEFAULT_MASTER_WEBUI_PORT)},
                                {"name": "MAX_EXECUTORS", "value": str(self.DEFAULT_MAX_EXECUTORS)},
                                {"name": "MAX_CORES_PER_APPLICATION", "value": str(self.DEFAULT_MAX_CORES_PER_APPLICATION)},
                                {"name": "EXECUTOR_CORES", "value": str(self.DEFAULT_EXECUTOR_CORES)}
                            ],
                            "resources": {
                                "requests": {
                                    "memory": memory,
                                    "cpu": str(cores)
                                }
                            }
                        }]
                    }
                }
            }
        }

        try:
            self.apps_api.create_namespaced_deployment(
                namespace=self.namespace,
                body=deployment
            )
            logger.info(f"Created Spark master deployment: {self.master_name}")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                self.apps_api.delete_namespaced_deployment(
                    name=self.master_name,
                    namespace=self.namespace
                )
                self.apps_api.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=deployment
                )
            else:
                raise

    def _create_master_service(self):
        """Create a Kubernetes service for the Spark master."""
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": self.master_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "spark",
                    "component": "master",
                    "user": self.username,
                    "cluster-id": self.cluster_id
                }
            },
            "spec": {
                "ports": [
                    {"port": self.DEFAULT_MASTER_PORT, "targetPort": self.DEFAULT_MASTER_PORT, "name": "spark"},
                    {"port": self.DEFAULT_MASTER_WEBUI_PORT, "targetPort": self.DEFAULT_MASTER_WEBUI_PORT, "name": "webui"}
                ],
                "selector": {
                    "app": "spark",
                    "component": "master",
                    "user": self.username,
                    "cluster-id": self.cluster_id
                }
            }
        }

        try:
            self.core_api.create_namespaced_service(
                namespace=self.namespace,
                body=service
            )
            logger.info(f"Created Spark master service: {self.master_name}")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                self.core_api.delete_namespaced_service(
                    name=self.master_name,
                    namespace=self.namespace
                )
                self.core_api.create_namespaced_service(
                    namespace=self.namespace,
                    body=service
                )
            else:
                raise

    def _create_worker_deployment(self, worker_count: int, worker_cores: int, worker_memory: str):
        """Create the Spark worker deployment."""
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": self.worker_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "spark",
                    "component": "worker",
                    "user": self.username,
                    "cluster-id": self.cluster_id
                }
            },
            "spec": {
                "replicas": worker_count,
                "selector": {
                    "matchLabels": {
                        "app": "spark",
                        "component": "worker",
                        "user": self.username,
                        "cluster-id": self.cluster_id
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "spark",
                            "component": "worker",
                            "user": self.username,
                            "cluster-id": self.cluster_id
                        }
                    },
                    "spec": {
                        "containers": [{
                            "name": "spark-worker",
                            "image": self.image,
                            "imagePullPolicy": self.image_pull_policy,
                            "env": [
                                {"name": "SPARK_MODE", "value": "worker"},
                                {"name": "SPARK_MASTER_URL", "value": f"spark://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_PORT}"},
                                {"name": "SPARK_WORKER_CORES", "value": str(worker_cores)},
                                {"name": "SPARK_WORKER_MEMORY", "value": worker_memory},
                                {"name": "SPARK_WORKER_WEBUI_PORT", "value": str(self.DEFAULT_WORKER_WEBUI_PORT)}
                            ],
                            "resources": {
                                "requests": {
                                    "memory": worker_memory,
                                    "cpu": str(worker_cores)
                                }
                            }
                        }]
                    }
                }
            }
        }

        try:
            self.apps_api.create_namespaced_deployment(
                namespace=self.namespace,
                body=deployment
            )
            logger.info(f"Created Spark worker deployment: {self.worker_name} with {worker_count} replicas")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                self.apps_api.delete_namespaced_deployment(
                    name=self.worker_name,
                    namespace=self.namespace
                )
                self.apps_api.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=deployment
                )
            else:
                raise

    def get_cluster_status(self) -> Dict:
        """Get the status of the Spark cluster."""
        try:
            master_status = self._get_deployment_status(self.master_name)
            worker_status = self._get_deployment_status(self.worker_name)

            status = {
                "master": master_status,
                "workers": worker_status
            }

            if master_status.get("ready_replicas", 0) > 0:
                status["master_url"] = f"spark://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_PORT}"
                status["master_ui_url"] = f"http://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_WEBUI_PORT}"

            return status
        except ApiException as e:
            logger.error(f"Error getting cluster status: {e}")
            raise

    def _get_deployment_status(self, deployment_name: str) -> Dict:
        """Get the status of a deployment."""
        try:
            deployment = self.apps_api.read_namespaced_deployment(
                name=deployment_name,
                namespace=self.namespace
            )

            return {
                "available_replicas": deployment.status.available_replicas,
                "ready_replicas": deployment.status.ready_replicas,
                "replicas": deployment.status.replicas,
                "unavailable_replicas": deployment.status.unavailable_replicas
            }
        except ApiException as e:
            if e.status == 404:
                return {"exists": False}
            else:
                raise

    def delete_cluster(self):
        """Delete the entire Spark cluster."""
        try:
            # Delete worker deployment
            self.apps_api.delete_namespaced_deployment(
                name=self.worker_name,
                namespace=self.namespace
            )
            logger.info(f"Deleted Spark worker deployment: {self.worker_name}")

            # Delete master deployment
            self.apps_api.delete_namespaced_deployment(
                name=self.master_name,
                namespace=self.namespace
            )
            logger.info(f"Deleted Spark master deployment: {self.master_name}")

            # Delete master service
            self.core_api.delete_namespaced_service(
                name=self.master_name,
                namespace=self.namespace
            )
            logger.info(f"Deleted Spark master service: {self.master_name}")

        except ApiException as e:
            logger.error(f"Error deleting Spark cluster: {e}")
            raise

    def scale_workers(self, worker_count: int):
        """Scale the number of Spark worker replicas."""
        try:
            self.apps_api.patch_namespaced_deployment_scale(
                name=self.worker_name,
                namespace=self.namespace,
                body={"spec": {"replicas": worker_count}}
            )
            logger.info(f"Scaled Spark worker deployment to {worker_count} replicas")
        except ApiException as e:
            logger.error(f"Error scaling Spark worker deployment: {e}")
            raise 