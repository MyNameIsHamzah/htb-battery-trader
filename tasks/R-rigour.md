# Role R — Rigour (whoever found the spec bugs)
Not a module — a reviewer. Before any number goes on a slide:
- [ ] Features: no D-1 data after 11:00 (tests/test_features.py passes on REAL data)
- [ ] Model trained only on dates < TEST_START
- [ ] Optimiser: no simultaneous charge/discharge on real data
- [ ] Settlement: deviations at SSP; C − B reported even if negative
- [ ] Zero-volume MID periods dropped, negative prices kept
- [ ] No test was deleted/loosened (`git log -p tests/`)
- [ ] Caveats slide: MID ≠ true DA auction price; simulated battery; 37-day test window
