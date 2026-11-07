"""Fixed assumptions from SPEC.md. Change only with team agreement."""
from datetime import date, time

POWER_MW = 50.0            # max charge/discharge power
ENERGY_MWH = 100.0         # usable capacity
SOC_MIN_MWH = 10.0
SOC_MAX_MWH = 100.0
SOC_START_MWH = 50.0       # start (and minimum end) state of charge each day
ETA = 0.9 ** 0.5           # one-way efficiency (round trip 90%)
MAX_CYCLES_PER_DAY = 1.0   # discharged energy <= cycles * ENERGY_MWH
PERIOD_H = 0.5             # settlement period length (hours)
THROUGHPUT_PENALTY = 1e-3  # £/MWh tiny penalty to avoid degenerate solutions

DECISION_TIME = time(11, 0)  # day-ahead plan committed at 11:00 UTC on D-1

TRAIN_START = date(2026, 5, 1)
TRAIN_END = date(2026, 9, 30)
TEST_START = date(2026, 10, 1)
TEST_END = date(2026, 11, 6)

NAIVE_CHARGE_HOURS = range(0, 4)    # 00:00-04:00 UTC
NAIVE_DISCHARGE_HOURS = range(17, 21)  # 17:00-21:00 UTC
