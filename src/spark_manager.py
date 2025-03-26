import logging
import os
import uuid
from typing import Dict, List

import kubernetes as k8s
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KubeSparkManager:
    """
    Manager for user-specific Spark clusters in Kubernetes.

    This class provides methods to create, manage, and destroy Spark clusters
    for individual JupyterHub users.
    """

    # Required environment variables
    REQUIRED_ENV_VARS = {
        "KUBE_NAMESPACE": "Kubernetes namespace for Spark clusters",
        "SPARK_IMAGE": "Docker image for Spark master and workers",
        "POSTGRES_USER": "PostgreSQL username",
        "POSTGRES_PASSWORD": "PostgreSQL password",
        "POSTGRES_DB": "PostgreSQL database name",
        "POSTGRES_URL": "PostgreSQL connection URL"
    }

    # Default configuration values for cluster settings
    DEFAULT_WORKER_COUNT = 2
    DEFAULT_WORKER_CORES = 10
    DEFAULT_WORKER_MEMORY = "10G"
    DEFAULT_MASTER_CORES = 10
    DEFAULT_MASTER_MEMORY = "10G"

    DEFAULT_IMAGE_PULL_POLICY = os.environ.get("SPARK_IMAGE_PULL_POLICY", "IfNotPresent")

    DEFAULT_EXECUTOR_CORES = 2
    DEFAULT_MAX_CORES_PER_APPLICATION = 10
    DEFAULT_MAX_EXECUTORS = 5

    DEFAULT_MASTER_PORT = 7077
    DEFAULT_MASTER_WEBUI_PORT = 8090
    DEFAULT_WORKER_WEBUI_PORT = 8081

    @classmethod
    def validate_environment(cls) -> Dict[str, str]:
        """
        Validate that all required environment variables are set.

        Returns:
            Dict[str, str]: Dictionary of validated environment variables
        """
        missing_vars = []
        env_values = {}

        for var, description in cls.REQUIRED_ENV_VARS.items():
            value = os.environ.get(var)
            if not value or not value.strip():
                missing_vars.append(f"{var} ({description})")
            env_values[var] = value

        if missing_vars:
            raise ValueError(
                "Missing required environment variables:\n" +
                "\n".join(f"- {var}" for var in missing_vars)
            )

        return env_values

    def __init__(self, username: str):
        """
        Initialize the KubeSparkManager with user-specific configuration.
        This should only be run inside a kubernetes container.

        Args:
            username: Username of the JupyterHub user

        Raises:
            ValueError: If required environment variables are not set
        """
        # Validate environment variables
        env_vars = self.validate_environment()

        self.username = username
        self.namespace = env_vars["KUBE_NAMESPACE"]
        self.image = env_vars["SPARK_IMAGE"]
        self.image_pull_policy = self.DEFAULT_IMAGE_PULL_POLICY

        # Generate a unique identifier for this user's Spark cluster
        self.cluster_id = f"spark-{username.lower()}-{str(uuid.uuid4())[:8]}"

        # Service names
        self.master_name = f"spark-master-{username.lower()}"
        self.worker_name = f"spark-worker-{username.lower()}"

        # Initialize Kubernetes client
        k8s.config.load_incluster_config()
        self.core_api = k8s.client.CoreV1Api()
        self.apps_api = k8s.client.AppsV1Api()

        logger.info(f"Initialized KubeSparkManager for user {username} in namespace {self.namespace}")

    @staticmethod
    def _prepare_postgres_config() -> List[Dict[str, str]]:
        """
        Prepare Postgres environment variables for Hive metastore.

        """

        # Postgres configuration from environment variables
        postgres_config = {
            "POSTGRES_USER": os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
            "POSTGRES_DB": os.environ["POSTGRES_DB"],
            "POSTGRES_URL": os.environ["POSTGRES_URL"]
        }

        # Convert to Kubernetes env var format
        return [{"name": k, "value": v} for k, v in postgres_config.items()]

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
            The Spark master URL for connecting to the cluster
        """
        # Create or validate postgres config
        postgres_env = self._prepare_postgres_config()

        # Create the Spark master deployment and service
        self._create_master_deployment(master_cores, master_memory, postgres_env)
        self._create_master_service()

        # TODO: Create the Spark worker deployment

        # Return cluster information
        master_url = f"spark://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_PORT}"
        master_ui_url = f"http://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_WEBUI_PORT}"

        return {
            "cluster_id": self.cluster_id,
            "master_url": master_url,
            "master_ui_url": master_ui_url,
            "status": "creating"
        }

    def _create_master_deployment(self, cores: int, memory: str, postgres_env: List[Dict[str, str]]):
        """
        Create the Spark master deployment.

        Args:
            cores: Number of CPU cores for the master
            memory: Memory allocation for the master
            postgres_env: Postgres environment variables
        """
        # Define Spark master environment variables
        env = [
                  {"name": "SPARK_MODE", "value": "master"},
                  {"name": "SPARK_MASTER_PORT", "value": str(self.DEFAULT_MASTER_PORT)},
                  {"name": "SPARK_MASTER_WEBUI_PORT", "value": str(self.DEFAULT_MASTER_WEBUI_PORT)},
                  {"name": "MAX_EXECUTORS", "value": str(os.environ.get("MAX_EXECUTORS", self.DEFAULT_MAX_EXECUTORS))},
                  {"name": "MAX_CORES_PER_APPLICATION",
                   "value": str(os.environ.get("MAX_CORES_PER_APPLICATION", self.DEFAULT_MAX_CORES_PER_APPLICATION))},
                  {"name": "EXECUTOR_CORES",
                   "value": str(os.environ.get("EXECUTOR_CORES", self.DEFAULT_EXECUTOR_CORES))},
              ] + postgres_env

        # Create the deployment spec
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
                                {"containerPort": self.DEFAULT_MASTER_WEBUI_PORT, "name": "master-ui",
                                 "protocol": "TCP"}
                            ],
                            "env": env,
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
            # Try to create the deployment first
            self.apps_api.create_namespaced_deployment(
                namespace=self.namespace,
                body=deployment
            )
            logger.info(f"Created Spark master deployment: {self.master_name}")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                try:
                    # Delete the existing deployment
                    self.apps_api.delete_namespaced_deployment(
                        name=self.master_name,
                        namespace=self.namespace
                    )
                    logger.info(f"Deleted existing Spark master deployment: {self.master_name}")

                    # Create new deployment
                    self.apps_api.create_namespaced_deployment(
                        namespace=self.namespace,
                        body=deployment
                    )
                    logger.info(f"Recreated Spark master deployment: {self.master_name}")
                except ApiException as delete_error:
                    logger.error(f"Error replacing Spark master deployment: {delete_error}")
                    raise
            else:
                logger.error(f"Error creating Spark master deployment: {e}")
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
                    {"port": self.DEFAULT_MASTER_WEBUI_PORT, "targetPort": self.DEFAULT_MASTER_WEBUI_PORT,
                     "name": "webui"}
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
            # Try to create the service first
            self.core_api.create_namespaced_service(
                namespace=self.namespace,
                body=service
            )
            logger.info(f"Created Spark master service: {self.master_name}")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                try:
                    # Delete the existing service
                    self.core_api.delete_namespaced_service(
                        name=self.master_name,
                        namespace=self.namespace
                    )
                    logger.info(f"Deleted existing Spark master service: {self.master_name}")

                    # Create new service
                    self.core_api.create_namespaced_service(
                        namespace=self.namespace,
                        body=service
                    )
                    logger.info(f"Recreated Spark master service: {self.master_name}")
                except ApiException as delete_error:
                    logger.error(f"Error replacing Spark master service: {delete_error}")
                    raise
            else:
                logger.error(f"Error creating Spark master service: {e}")
                raise
