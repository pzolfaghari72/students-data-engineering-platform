"""Render the existing enrollment dashboard against validated incremental views."""

import argparse
import json
from pathlib import Path
import re


MODEL_TABLE = re.compile(
    r"\b(FROM|JOIN)(\s+)(dim_(?:applicant|class_standing|course|date|degree|department|grade|student|term)"
    r"|fact_(?:application|course_offering|course_enrollment))\b",
    re.IGNORECASE,
)
DASHBOARD_ROUTE = re.compile(r"/d/students-enrollment-kpis(?:/[^?\s)]+)?(?=[?\s)]|$)")


def _retarget(sql):
    return MODEL_TABLE.sub(lambda match: f"{match[1]}{match[2]}incremental_{match[3]}", sql)


def _retarget_links(value, route):
    if isinstance(value, dict):
        for name, item in value.items():
            if name == "url" and isinstance(item, str):
                value[name] = DASHBOARD_ROUTE.sub(route, item)
            else:
                _retarget_links(item, route)
    elif isinstance(value, list):
        for item in value:
            _retarget_links(item, route)


def render(dashboard, *, preview):
    dashboard["title"] = "Incremental CDC— Student Enrollment, Capacity & Grades"
    dashboard["description"] = (
        "The last verified incremental warehouse publication. Live SQL Server changes "
        "publish on the 15-minute Airflow schedule. Academic term and department filters "
        "apply as before. Applications use entry term and all departments."
    )
    if preview:
        dashboard["uid"] = "students-enrollment-incremental"
    route = f"/d/{dashboard['uid']}"
    _retarget_links(dashboard, route)
    if preview:
        dashboard["links"].append({
            "title": "Batch Snapshot",
            "type": "link",
            "url": "/d/students-enrollment-kpis",
            "keepTime": False,
        })
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            target["rawSql"] = _retarget(target["rawSql"])
        if panel["type"] == "text":
            panel["options"]["content"] = DASHBOARD_ROUTE.sub(route, (
                panel["options"]["content"]
                .replace("Published warehouse snapshot", "Published incremental warehouse")
                .replace("Data updates after a successful warehouse load.",
                         "Verified changes publish on the 15-minute Airflow schedule.")
            ))
        if panel.get("description"):
            panel["description"] = panel["description"].replace(
                "published warehouse snapshot", "incremental warehouse publication",
            )
    for variable in dashboard["templating"]["list"]:
        for field in ("query", "definition"):
            variable[field] = _retarget(variable[field])
    return dashboard


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--promote", action="store_true", help="Render with the existing dashboard UID.")
    args = parser.parse_args()
    if args.source.resolve() == args.output.resolve():
        parser.error("Render to a separate file so the snapshot dashboard can be restored.")
    dashboard = render(json.loads(args.source.read_text()), preview=not args.promote)
    with args.output.open("x") as output:
        json.dump(dashboard, output, indent=2, ensure_ascii=False)
        output.write("\n")


if __name__ == "__main__":
    main()
