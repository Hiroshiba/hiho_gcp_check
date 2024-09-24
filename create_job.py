"""
適当なjob名でGCP BatchでコンテナJobを作成する。
用途によってmachine_typeとdocker_imageを変えるべき。
"""

from pathlib import Path
import random
from google.cloud import batch_v1 as batch

from google.cloud import compute_v1

from env import PROJECT_ID, REGION, ZONE


def get_machine_type(machine_type: str):
    """指定したmachine_typeの情報を取得する"""
    client = compute_v1.MachineTypesClient()
    return client.get(project=PROJECT_ID, zone=ZONE, machine_type=machine_type)


def create_job(
    job_name: str,
    machine_type: str,
    docker_image: str,
    script_path: Path,
    bucket_names: list[str],
) -> batch.Job:
    client = batch.BatchServiceClient()
    task = batch.TaskSpec()

    # Runnable
    runnable = batch.Runnable()
    runnable.container.image_uri = docker_image
    runnable.container.commands = ["/bin/bash", "-c", script_path.read_text()]
    task.runnables = [runnable]

    # Backet
    for bucket_name in bucket_names:
        gcs_bucket = batch.GCS()
        gcs_bucket.remote_path = bucket_name
        gcs_volume = batch.Volume()
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = f"/mnt/disks/{bucket_name}"
        task.volumes.append(gcs_volume)

    # Resource
    machine = get_machine_type(machine_type)
    resources = batch.ComputeResource()
    resources.cpu_milli = machine.guest_cpus * 1000
    resources.memory_mib = machine.memory_mb * (1000**2) // (1024**2)
    task.compute_resource = resources

    # Retry
    action_condition = batch.LifecyclePolicy.ActionCondition()
    action_condition.exit_codes = [
        50001,  # VM プリエンプション
        50002,  # VM レポートのダイムアウト
        50003,  # 実行中に VM が再起動した
        # 50004,# VM とタスクが応答しない
        50005,  # タスクが最大ランタイムを超えている
        50006,  # 実行中に VM が再作成された
    ]
    lifecycle_policy = batch.LifecyclePolicy()
    lifecycle_policy.action = batch.LifecyclePolicy.Action.RETRY_TASK
    lifecycle_policy.action_condition = action_condition
    task.max_retry_count = 1
    task.lifecycle_policies = [lifecycle_policy]
    task.max_run_duration = f"{14 * 24 * 60 * 60}s"  # BatchのJobは最長で14日

    # Misc
    policy = batch.AllocationPolicy.InstancePolicy()
    policy.machine_type = machine_type
    if len(machine.accelerators) > 0:
        assert len(machine.accelerators) == 1, "複数のアクセラレータは未対応"
        accelerator = batch.AllocationPolicy.Accelerator()
        accelerator.type_ = machine.accelerators[0].guest_accelerator_type
        accelerator.count = machine.accelerators[0].guest_accelerator_count
        policy.accelerators = [accelerator]
    policy.provisioning_model = batch.AllocationPolicy.ProvisioningModel.SPOT
    policy.boot_disk = batch.AllocationPolicy.Disk()
    policy.boot_disk.size_gb = 100  # FIXME: GPU利用時のみにする
    instances = batch.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    instances.install_ops_agent = True
    instances.install_gpu_drivers = True  # FIXME: GPU利用時のみにする
    allocation_policy = batch.AllocationPolicy()
    allocation_policy.instances = [instances]

    # Job
    group = batch.TaskGroup()
    group.task_spec = task
    job = batch.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.logs_policy = batch.LogsPolicy()
    job.logs_policy.destination = batch.LogsPolicy.Destination.CLOUD_LOGGING

    # Request
    create_request = batch.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name
    create_request.parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    return client.create_job(create_request)


if __name__ == "__main__":
    create_job(
        job_name=f"check-job-{random.randint(0, 100000)}",
        # machine_type="e2-micro",
        machine_type="g2-standard-4",
        docker_image="pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
        script_path=Path("hiho_script.bash"),
        bucket_names=["accent_estimate"],
    )
