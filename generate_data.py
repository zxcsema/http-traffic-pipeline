"""Reproduce the four demo CSVs using only the Python standard library."""

import random
from datetime import date, datetime, timedelta

from traffic import FIELDS, PROJECT_DIR, write_csv


SEED = 42
DAYS = [date(2025, 1, 30) + timedelta(days=offset) for offset in range(4)]
PATHS = ["/", "/catalog", "/product/1", "/product/2", "/cart", "/checkout", "/login"]


def generate_data(output_dir=PROJECT_DIR / "data" / "raw"):
    rng = random.Random(SEED)
    for day in DAYS:
        rows = []
        for index in range(4800):
            timestamp = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randrange(86400))
            path = rng.choices(PATHS, weights=[24, 30, 12, 10, 10, 8, 6])[0]
            if rng.random() < 0.4:
                path += f"?page={rng.randint(1, 5)}&source=demo"
            rows.append([
                f"{day}-{index:05d}", timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                f"192.0.2.{rng.randint(1, 200)}", "GET", path,
                str(rng.choices([200, 201, 301, 404, 500], weights=[70, 10, 5, 10, 5])[0]),
                f"{rng.randint(1000, 70000) / 100:.2f}", "DemoBrowser/1.0",
            ])

        # Exact duplicate rows, rather than repeated request IDs with different fields.
        duplicates = [row.copy() for row in rng.sample(rows, 100)]
        defects = [
            ("ip", ""),
            ("timestamp", "not-a-date"),
            ("timestamp", "2025-02-30 12:00:00"),
            ("timestamp", f"{day + timedelta(days=1)} 12:00:00"),
            ("status_code", "oops"),
            ("status_code", "600"),
            ("response_ms", "NaN"),
            ("response_ms", "-1.00"),
            ("method", ""),
            ("path", "catalog"),
        ]
        invalid = []
        for index in range(100):
            row = rows[index].copy()
            row[0] = f"{day}-invalid-{index:03d}"
            field, value = defects[index % len(defects)]
            row[FIELDS.index(field)] = value
            invalid.append(row)
        rows.extend(duplicates)
        rows.extend(invalid)
        rng.shuffle(rows)
        write_csv(output_dir / f"{day}.csv", FIELDS, rows)


if __name__ == "__main__":
    generate_data()
    print("Generated four demo CSVs: seed=42, 5000 rows per day")
