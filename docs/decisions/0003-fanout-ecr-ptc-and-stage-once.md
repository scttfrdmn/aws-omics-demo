# 0003 — Fan-out traps: ECR Pull-Through Cache, and stage reference data once

**Status:** accepted
**Date:** 2026-06-17

Two distinct things break *only at wide fan-out* that are invisible at N=3. Both
are general lessons for "one ephemeral instance per task" pipelines.

## Trap 1 — Docker Hub / registry rate limit → ECR Pull-Through Cache

**Symptom.** At wide N, the CALL_VARIANTS instances launch near-simultaneously and
each pulls the `bcftools`/`samtools` image. They share one NAT egress IP, so a
rate-limited public registry (Docker Hub's anonymous pull limit, 100 pulls /
6 hr / IP) can throttle the burst:

```
docker: Error response from daemon: toomanyrequests: You have reached your
unauthenticated pull rate limit.   → exit 125 → Nextflow aborted the tasks
```

Never seen at N=3 (≤3 pulls). The container is *correctly* multi-arch (this demo
pulls native arm64 from `quay.io/aarchbio` on Graviton and amd64
`quay.io/biocontainers` on x86) — this is purely a registry throttle, not an arch
problem. Note: BAMs are **pre-aligned** and read directly from `s3://1000genomes`,
so there is no SRA-fetch container to pull; the fan-out container pulls are the
bcftools/samtools images.

**Decision.** Front the public registry with an **ECR Pull-Through Cache**
repository (Docker Hub PAT in Secrets Manager for the Docker Hub upstream). Tasks
pull `<acct>.dkr.ecr.<region>.amazonaws.com/<upstream>/bcftools:<tag>`. ECR
authenticates upstream **once**, caches the image in-region, and serves all N pulls
from ECR. The multi-arch manifest is preserved (c7g → arm64, c7i → amd64).

**Lesson.** A correctly-built multi-arch image is *still* a fan-out bottleneck if
it lives on a rate-limited public registry. PTC collapses N anonymous pulls into 1
authenticated, cached pull. (Public ECR Gallery / `quay.io` mirrors are
alternatives; PTC is the most general because it caches *any* upstream image on
demand.)

> ⚠️ Store any registry credential (PAT) only in Secrets Manager; never paste it
> into a working session or commit it.

## Trap 2 — Stage reference data once, not per run

The reference genome is staged **from its canonical source** (not laundered through
old snapshots) and every phase is timed. The one-time staging path:

| reference | source | phase |
|-----------|--------|-------|
| `human_g1k_v37.fasta` | `s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz` (in-region S3) | download |
| | (local) | gunzip |
| | `samtools faidx` | index → `.fai` |
| | → S3 | sync |

The download (in-region S3) and the `faidx` are each **measured during staging**
(`build_fsx_db.py` times every phase and writes them to
`s3://BUCKET/reference-fsx/staging_timings.json`); the numbers will land there when
the staging is run. The point is structural, not a specific figure: this is a
one-time cost, paid once and amortized over all runs.

**Decision.** Stage the reference into S3/FSx **once**, as a measured one-time cost
(amortized over all runs). Never make a per-run, per-task path download + `faidx`
the reference N times. The per-task path stays zero-copy off the shared FSx mount.

**Lesson.** With ephemeral per-task instances it's tempting to "just fetch what you
need" on each task. For large, static reference data that is the single most
expensive mistake available — wasted compute (Option B in
[ADR-0001](0001-db-delivery-ami-ebs-fsx.md)) multiplied by your fan-out. Stage
once; read in place.
