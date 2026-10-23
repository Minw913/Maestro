"""Common seed: schedules each job's operations in technological order, earliest-due-date first; feasible by construction."""
import argparse
import json


def schedule(instance):
    """Each operation starts once its job and its machine are both free, so precedence, release and capacity hold."""
    machine_free = [0.0] * instance["num_machines"]
    rows, total = [], 0.0
    for job in sorted(instance["jobs"], key=lambda j: (j["due_date"], j["job_id"])):
        ready, operations = float(job.get("release_date", 0)), []
        for op in job["operations"]:
            start = max(ready, machine_free[op["machine"]])
            ready = machine_free[op["machine"]] = start + op["processing_time"]
            operations.append({"machine": op["machine"], "start_time": start})
        tardiness = max(0.0, ready - job["due_date"])
        total += job["weight"] * tardiness
        rows.append({"job_id": job["job_id"], "completion_time": ready, "tardiness": tardiness,
                     "operations": operations})
    return {"objective_value": total, "schedule": sorted(rows, key=lambda r: r["job_id"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance_path", required=True)
    parser.add_argument("--solution_path", required=True)
    parser.add_argument("--time_limit", type=int, required=True)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()
    with open(args.instance_path) as handle:
        solution = schedule(json.load(handle))
    with open(args.solution_path, "w") as handle:
        json.dump(solution, handle)
