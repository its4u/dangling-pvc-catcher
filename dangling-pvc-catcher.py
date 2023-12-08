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
    
def get_pvcs_from(resources: list) -> set:
    running_resources = filter(lambda resource: get_replicas_from(resource) > 0, resources)
    resources_with_pvc = filter(lambda resource: resource.spec.template.spec.volumes and resource.spec.template.spec.volumes[0].persistent_volume_claim, running_resources)
    pvcs = set()
    for resource in resources_with_pvc:
        for pvc in map(lambda volume: volume.persistent_volume_claim, filter(lambda volume: volume.persistent_volume_claim, resource.spec.template.spec.volumes)):
            pvcs.add(format_resource(resource.metadata.namespace, pvc.claim_name))
    return pvcs

def get_pvcs_from_deploymentconfigs() -> set:
    result = set()
    for resource in CustomObjectsApi().list_cluster_custom_object("apps.openshift.io", "v1", "deploymentconfigs").get("items", []):
        for volume in resource.get("spec").get("template").get("spec").get("volumes", []):
            if volume.get("persistentVolumeClaim", None) and resource.get("status").get("availableReplicas", 0) > 0:
                result.add(format_resource(resource.get("metadata").get("namespace"), volume.get("persistentVolumeClaim").get("claimName")))
    print(result)
    return result

def get_pvcs_used_by_resources() -> set:
    result = set()
    result |= get_pvcs_from(api_apps.list_deployment_for_all_namespaces().items)
    result |= get_pvcs_from(api_apps.list_stateful_set_for_all_namespaces().items)
    result |= get_pvcs_from(api_apps.list_daemon_set_for_all_namespaces().items)
    result |= get_pvcs_from(api_apps.list_replica_set_for_all_namespaces().items)
    result |= get_pvcs_from_deploymentconfigs()
    return result

all_pvcs = get_all_pvcs()
pvcs_used_by_resources = get_pvcs_used_by_resources()
dangling_pvcs = all_pvcs - pvcs_used_by_resources

print(f"All PVCs count: {len(all_pvcs)}")
print(f"PVCs used by resources count: {len(pvcs_used_by_resources)}")
print(f"Dangling PVCs count: {len(dangling_pvcs)}")
print("\nDANGLING PVCs:\n")
print("\n".join(sorted(list(all_pvcs - pvcs_used_by_resources))))
