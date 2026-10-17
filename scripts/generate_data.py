import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

FIELDS = (
    "issue_id",
    "site",
    "building",
    "equipment_type",
    "equipment_tag",
    "manufacturer",
    "contractor",
    "cx_level",
    "severity",
    "status",
    "opened_at",
    "closed_at",
    "notes",
)
SITES = {"SIN1": "AB", "SIN2": "ABC", "JKT1": "A"}
EQUIPMENT = {
    "UPS": ("UPS", ["Voltaris", "Kestrel Power"]),
    "generator": ("GEN", ["Ironvale", "Northwind Engines"]),
    "chiller": ("CHL", ["Polar Dynamics", "Coldline"]),
    "CRAH": ("CRAH", ["Airvance", "Coldline"]),
    "switchgear": ("SWG", ["Gridmark", "Voltaris"]),
    "PDU": ("PDU", ["Kestrel Power", "Railspan"]),
    "transformer": ("TX", ["Gridmark", "Ferrocore"]),
    "BMS": ("CTRL", ["Helix Controls"]),
}
CONTRACTORS = ["Apex M&E", "Meridian Electrical", "Harbour Mechanical", "Northgate Controls"]
SEVERITY_WEIGHTS = {"low": 0.35, "medium": 0.4, "high": 0.2, "critical": 0.05}
NOTES = {
    "equipment_defect": [
        "{tag} fan failure during pre-functional test, unit replaced by vendor",
        "Faulty rectifier module on {tag}, alarm persists after reset",
        "Coolant leak observed at {tag} header flange",
    ],
    "installation_workmanship": [
        "Loose termination found on {tag} incomer, retorque required",
        "Cables mislabelled between {tag} and downstream panel",
        "Cable routing obstructs access panel on {tag}",
    ],
    "design_issue": [
        "Breaker rating on {tag} does not match single line diagram",
        "Insufficient service clearance in front of {tag}",
        "{tag} undersized for N+1 load at peak conditions",
    ],
    "controls_integration": [
        "BMS point mapping for {tag} shows wrong discharge temperature",
        "Setpoint drift on {tag} during sequence of operation test",
        "Modbus comms to {tag} dropping intermittently",
    ],
    "documentation": [
        "O&M manual for {tag} not submitted",
        "As-built drawing missing {tag} panel schedule",
        "Factory test certificate for {tag} outstanding",
    ],
    "ambiguous": [
        "{tag} did not transfer within expected window during integrated systems test",
        "Readings on {tag} inconsistent between local display and BMS, vendor suspects faulty sensor",
        "Contractor reports {tag} noisy under load, investigation ongoing",
        "{tag} tripped during load bank test, root cause under review",
    ],
}
NOTE_WEIGHTS = {k: (0.25 if k == "ambiguous" else 0.15) for k in NOTES}


def make_row(i: int, rng: random.Random, today: date) -> dict:
    site = rng.choice(list(SITES))
    building = rng.choice(SITES[site])
    equipment_type = rng.choice(list(EQUIPMENT))
    prefix, makers = EQUIPMENT[equipment_type]
    tag = f"{prefix}-{site}-{building}-{rng.randint(1, 12):02d}"
    kind = rng.choices(list(NOTE_WEIGHTS), weights=NOTE_WEIGHTS.values())[0]
    opened = today - timedelta(days=rng.randint(0, 120))
    closed = opened + timedelta(days=rng.randint(1, 30))
    is_closed = rng.random() < 0.7 and closed <= today
    return {
        "issue_id": f"CX-{i:06d}",
        "site": site,
        "building": building,
        "equipment_type": equipment_type,
        "equipment_tag": tag,
        "manufacturer": rng.choice(makers),
        "contractor": rng.choice(CONTRACTORS),
        "cx_level": rng.choice(["L1", "L2", "L3", "L4", "L5"]),
        "severity": rng.choices(list(SEVERITY_WEIGHTS), weights=SEVERITY_WEIGHTS.values())[0],
        "status": "closed" if is_closed else "open",
        "opened_at": opened.isoformat(),
        "closed_at": closed.isoformat() if is_closed else "",
        "notes": rng.choice(NOTES[kind]).format(tag=tag),
    }


def corrupt(row: dict, rng: random.Random) -> dict:
    row = dict(row)
    match rng.randrange(6):
        case 0:
            row["severity"] = "urgent"
        case 1:
            row["issue_id"] = row["issue_id"][:5]
        case 2:
            row["notes"] = ""
        case 3:
            row["opened_at"] = date.fromisoformat(row["opened_at"]).strftime("%d/%m/%Y")
        case 4:
            row |= {
                "status": "closed",
                "closed_at": (date.fromisoformat(row["opened_at"]) - timedelta(days=3)).isoformat(),
            }
        case 5:
            row |= {"status": "open", "closed_at": row["opened_at"]}
    return row


def generate(n: int, bad_rate: float = 0.05, seed: int = 42, today: date | None = None) -> list[dict]:
    rng = random.Random(seed)
    today = today or date.today()
    rows = []
    for i in range(1, n + 1):
        row = make_row(i, rng, today)
        if rng.random() < bad_rate:
            row = corrupt(row, rng) if rng.random() < 0.8 or not rows else dict(rng.choice(rows))
        rows.append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic commissioning issue logs")
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--bad-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path(f"data/issues_{date.today():%Y%m%d}.csv"))
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(generate(args.rows, args.bad_rate, args.seed))
    print(f"Wrote {args.rows} rows to {args.out}")


if __name__ == "__main__":
    main()
