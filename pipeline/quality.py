import re
from collections import Counter

from pipeline.config import MAX_ID
from pipeline.source import inspect_source, iter_source


def parse_id(value):
    value = value.strip()
    if not re.fullmatch(r"[0-9]+", value):
        return None
    significant = value.lstrip("0")
    if not significant or len(significant) > 19:
        return None
    number = int(significant)
    return number if number <= MAX_ID else None


def reference_profile(path):
    """Small-data oracle/report only; production transformations use Spark."""
    info = inspect_source(path)
    pairs, rejected = [], []
    multi_rows = 0
    for row_number, (issue, duplicates) in enumerate(iter_source(path), 2):
        tokens = duplicates.split(",")
        multi_rows += len(tokens) > 1
        for token in tokens:
            a, b = parse_id(issue), parse_id(token)
            reason = "invalid_id" if a is None or b is None else "self_reference" if a == b else None
            if reason:
                rejected.append({"row_number": row_number, "issue_raw": issue,
                                 "duplicate_raw": token, "reason": reason})
            else:
                pairs.append((a, b))
    relations = set(pairs)
    ids = {v for pair in relations for v in pair}
    undirected = {tuple(sorted(pair)) for pair in relations}
    incoming = Counter(b for a, b in relations)
    outgoing = Counter(a for a, b in relations)
    neighbors = Counter(v for pair in undirected for v in pair)
    reciprocal = sum((b, a) in relations for a, b in relations) // 2
    report = {**info, "multi_id_rows": multi_rows,
              "expanded_rows": len(pairs) + len(rejected), "quarantined_rows": len(rejected),
              "duplicates_removed": len(pairs) - len(relations), "output_rows": len(relations),
              "issue_count": len(ids), "undirected_relation_count": len(undirected),
              "reciprocal_pair_count": reciprocal,
              "reciprocal_pair_ratio": reciprocal / len(undirected) if undirected else 0,
              "top_issues": [{"issue_id": i, "in_degree": incoming[i], "out_degree": outgoing[i],
                              "neighbor_count": neighbors[i]}
                             for i in sorted(ids, key=lambda i: (-neighbors[i], i))[:10]],
              "quarantine_sample": rejected[:20]}
    return report


def validate_metrics(metrics):
    fields = ("input_rows", "expanded_rows", "quarantined_rows", "duplicates_removed", "output_rows")
    if any(type(metrics.get(k)) is not int or metrics[k] < 0 for k in fields):
        raise ValueError("Missing or invalid quality metrics")
    if metrics["expanded_rows"] != sum(metrics[k] for k in
                                        ("quarantined_rows", "duplicates_removed", "output_rows")):
        raise ValueError("Row reconciliation failed")
    if metrics["expanded_rows"] < metrics["input_rows"]:
        raise ValueError("CSV records disappeared during normalization")
    if metrics["quarantined_rows"]:
        raise ValueError(f"Quality gate rejected {metrics['quarantined_rows']} relationships")
    if not metrics["input_rows"] or not metrics["output_rows"]:
        raise ValueError("Empty snapshot cannot be published")

