from kubernetes import client, config
from kubernetes.client import CustomObjectsApi
import openshift as oc

config.load_kube_config()
api_core = client.CoreV1Api()
api_apps = client.AppsV1Api()

def format_resource(namespace: str, name: str) -> str:
    return f"{namespace}/{name}"

def get_all_pvcs() -> set:
    return set(map(lambda pvc: format_resource(pvc.metadata.namespace, pvc.metadata.name), api_core.list_persistent_volume_claim_for_all_namespaces().items))

def get_replicas_from(resource) -> int:
    # Different resources have different fields for the number of replicas, either status.available_replicas or status.number_available
    try:
        return int(resource.status.available_replicas) if resource.status.available_replicas else 0
    except AttributeError:
        return int(resource.status.number_available) if resource.status.number_available else 0
    
def get_unused_pvcs_from(resources: list) -> set[tuple[str, str]]:
    stopped_resources = filter(lambda resource: get_replicas_from(resource) == 0, resources)
    resources_with_unused_pvcs = list(filter(lambda resource: resource.spec.template.spec.volumes and resource.spec.template.spec.volumes[0].persistent_volume_claim, stopped_resources))
    pvcs = set()
    for resource in resources_with_unused_pvcs:
        for pvc in map(lambda volume: volume.persistent_volume_claim, filter(lambda volume: volume.persistent_volume_claim, resource.spec.template.spec.volumes)):
            pvcs.add((format_resource(resource.metadata.namespace, pvc.claim_name), format_resource(resource.metadata.namespace, resource.metadata.name)))
    return pvcs

def get_unused_pvcs_from_deploymentconfigs() -> set[tuple[str, str]]:
    result = set()
    for resource in CustomObjectsApi().list_cluster_custom_object("apps.openshift.io", "v1", "deploymentconfigs").get("items", []):
        for volume in resource.get("spec").get("template").get("spec").get("volumes", []):
            if volume.get("persistentVolumeClaim", None) and not resource.get("status").get("availableReplicas"):
                result.add((format_resource(resource.get("metadata").get("namespace"), volume.get("persistentVolumeClaim").get("claimName")), format_resource(resource.get("metadata").get("namespace"), resource.get("metadata").get("name"))))
    return result

def get_pvcs_unused_by_resources() -> set:
    result = set()
    result |= get_unused_pvcs_from(api_apps.list_deployment_for_all_namespaces().items)
    result |= get_unused_pvcs_from(api_apps.list_stateful_set_for_all_namespaces().items)
    result |= get_unused_pvcs_from(api_apps.list_daemon_set_for_all_namespaces().items)
    result |= get_unused_pvcs_from(api_apps.list_replica_set_for_all_namespaces().items)
    result |= get_unused_pvcs_from_deploymentconfigs()
    return result

dangling_pvcs = get_pvcs_unused_by_resources()
dangling_pvcs_without_namespace = set(map(lambda pvc: pvc[0], dangling_pvcs))
dangling_pvcs_count = len(dangling_pvcs_without_namespace)
pvc_count = len(get_all_pvcs())

print(f"All PVCs count: {pvc_count}")
print(f"PVCs used by resources count: {pvc_count - dangling_pvcs_count}")
print(f"Dangling PVCs count: {dangling_pvcs_count}")
print("\nDANGLING PVCs (without NS):\n")
print("\n".join(sorted(list(dangling_pvcs_without_namespace))))
print("\nDANGLING PVCs (with NS):\n")
print("\n".join(list(f"{pvc[0]}\tFROM: {pvc[1]}" for pvc in sorted(list(dangling_pvcs), key=lambda pvc: pvc[0]))))
