"""
Launch workers faster than AWS Auto Scaling
"""
from json import loads
from os import environ
from typing import Dict

from boto3 import client  # type: ignore

from prometheus_client.parser import text_string_to_metric_families  # type: ignore

from requests import get

autoscaling = client("autoscaling")

CONCOURSE_METRICS_URL = environ["CONCOURSE_METRICS_URL"]
TAG_TO_AUTO_SCALING_GROUP = loads(environ["TAG_TO_AUTO_SCALING_GROUP"])


def handler(event: None, context: None) -> None:  # pylint: disable=unused-argument
    """
    Check if a worker needs to be launched, and notify Auto Scaling if so
    """
    response = get(url=CONCOURSE_METRICS_URL, timeout=1)

    if response.status_code != 200:
        raise ValueError(f"Concourse returned {response.status_code}: {response.text}")

    parsed = text_string_to_metric_families(response.text)

    concourse_steps_waiting: Dict[str, int] = {}

    for metric in parsed:
        if metric.name == "concourse_steps_waiting":
            for sample in metric.samples:
                tag = sample.labels["workerTags"]
                if tag == "":
                    tag = "none"

                concourse_steps_waiting[tag] = concourse_steps_waiting.get(tag, 0) + sample.value

    for tag in concourse_steps_waiting:
        if concourse_steps_waiting[tag] == 0:
            continue
        if tag in TAG_TO_AUTO_SCALING_GROUP:
            group_name = TAG_TO_AUTO_SCALING_GROUP[tag]
            auto_scaling_groups = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name])[
                "AutoScalingGroups"
            ]

            if len(auto_scaling_groups) != 1:
                raise ValueError(
                    f"Auto Scaling returned {len(auto_scaling_groups)} results for search for {group_name}, expected exactly 1"  # noqa
                )

            group = auto_scaling_groups[0]

            if group["AutoScalingGroupName"] != group_name:
                raise ValueError(f"Auto Scaling returned group with different name than expected for {group_name}")

            if group["DesiredCapacity"] == 0:
                autoscaling.set_desired_capacity(
                    AutoScalingGroupName=group_name,
                    DesiredCapacity=1,
                )


if __name__ == "__main__":
    handler(None, None)
