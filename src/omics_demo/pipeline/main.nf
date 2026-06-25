#!/usr/bin/env nextflow
//
// main.nf — germline variant calling on 1000 Genomes low-coverage WGS.
//
//   CALL_VARIANTS (per sample, parallel)  →  MERGE_VCFS (cohort)  →  VCF_STATS (QC)
//
// Executor: nf-spawn (spore.host). Every process runs on its OWN ephemeral EC2
// instance, sized by its `label` (see nextflow.config), and self-terminates when
// the task finishes — there is no cluster and no queue to manage. The shared
// human reference genome is read read-only off an FSx for Lustre filesystem that
// nf-spawn mounts on each task instance; the aligned BAMs are read straight from
// the public 1000 Genomes S3 bucket. See pipeline/README.md for the full picture.
//
// Run it standalone (outside the demo) like any Nextflow pipeline:
//   nextflow run main.nf \
//     --samples samples.csv \           # sample_id,population,super_population,bam_path
//     --regions 20 \                    # contig to call on (human_g1k_v37 names it '20')
//     --outdir s3://my-bucket/results/ \
//     -c nextflow.config                # carries the nf-spawn executor + per-label sizing

nextflow.enable.dsl = 2

// ── Parameters ───────────────────────────────────────────────────────────────
params.samples        = params.samples        ?: 'samples.csv'
// Region restriction for demo speed. human_g1k_v37 names the contig '20'
// (NOT 'chr20' — the 'chr'-prefix is the hg19/GRCh38 convention).
params.regions        = params.regions        ?: '20'
// FSx mount point on each task instance (nf-spawn mounts the shared filesystem
// here via `ext.fsx`). The reference FASTA + its .fai live at <fsx_mount>/reference.
params.fsx_mount      = params.fsx_mount       ?: '/fsx'
// bcftools container image. CALL_VARIANTS invokes it via an explicit `docker run`
// (see that process); nextflow.config injects the arch-correct image (aarchbio on
// Graviton, biocontainers on x86). This fallback keeps main.nf runnable alone.
params.bcftools_image = params.bcftools_image  ?: 'quay.io/biocontainers/bcftools:1.21--h8b25389_0'


// ── CALL_VARIANTS ──────────────────────────────────────────────────────────────
// Per-sample fan-out: one EC2 instance per sample (nf-spawn). Reads the sample's
// pre-aligned chr20 BAM directly from s3://1000genomes, calls variants against the
// shared reference on FSx, emits a bgzipped + tabix-indexed per-sample VCF.
//
// WHY no `container` directive: this script does TWO kinds of work — host-level
// S3 I/O (`aws s3 cp`, which needs the AWS CLI that lives on the host AMI) and the
// bio-tool step (bcftools, which lives in a container). A `container` directive
// would run the WHOLE script inside the bcftools image, which has no AWS CLI
// (→ "command not found"). So we keep the script on the host and invoke bcftools
// through an explicit `docker run`. This is the same split the spore.host demos
// use for any process that must both touch S3 and run a single-tool BioContainer.
process CALL_VARIANTS {
    label 'process_medium'
    tag { sample_id }

    input:
    tuple val(sample_id), val(population), val(super_population), val(bam_path)

    output:
    tuple val(sample_id), val(population), val(super_population),
          path("${sample_id}.vcf.gz"), path("${sample_id}.vcf.gz.tbi")

    script:
    """
    set -euxo pipefail

    # ── Environment provenance (host-side; all probes best-effort) ───────────
    # Captured so every timing below is interpretable — a throughput number only
    # means something qualified by instance type / placement.
    TOK=\$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 120" 2>/dev/null || true)
    imds() { curl -sf -H "X-aws-ec2-metadata-token: \${TOK}" "http://169.254.169.254/latest/meta-data/\$1" 2>/dev/null; }
    INSTANCE_TYPE=\$(imds instance-type || echo unknown)
    INSTANCE_ID=\$(imds instance-id || echo unknown)
    AZ=\$(imds placement/availability-zone || echo unknown)
    LIFECYCLE=\$(imds instance-life-cycle || echo unknown)
    VCPUS=\$(nproc 2>/dev/null || echo 0)
    UNAME_ARCH=\$(uname -m 2>/dev/null || echo unknown)

    # ── Phase 1 (HOST): pull the chr20 BAM + its .bai from s3://1000genomes ──
    # 1000 Genomes is in us-east-1 — same region as these instances, no egress
    # charge (--no-sign-request: it's public Open Data). The BAMs are PRE-ALIGNED
    # and the .bai sibling exists on S3, so we fetch BOTH and skip samtools.
    T0=\$(date +%s.%N)
    aws s3 cp ${bam_path} ./${sample_id}.bam \\
        --no-sign-request --region us-east-1 --no-progress
    aws s3 cp ${bam_path}.bai ./${sample_id}.bam.bai \\
        --no-sign-request --region us-east-1 --no-progress
    T1=\$(date +%s.%N)
    BAM_BYTES=\$(stat -c%s ./${sample_id}.bam 2>/dev/null || echo 0)

    # ── Phase 2 (CONTAINER): call variants on chr20 via explicit docker run ──
    # bcftools mpileup | call -mv (multiallelic, variants-only), bgzipped, then
    # tabix-indexed. The container mounts the work dir (PWD, holding the bam+bai)
    # AND the FSx mount read-only, so bcftools reads the shared reference + its
    # real .fai DIRECTLY off FSx — genuinely zero-copy, no per-task reference copy.
    BCFTOOLS_IMG='${params.bcftools_image}'
    FSX='${params.fsx_mount}'
    T2=\$(date +%s.%N)
    docker run --rm -v "\${PWD}:/work" -v "\${FSX}:\${FSX}:ro" -w /work "\${BCFTOOLS_IMG}" \\
        sh -c "bcftools mpileup -f \${FSX}/reference -r ${params.regions} ./${sample_id}.bam \\
               | bcftools call -mv -Oz -o ${sample_id}.vcf.gz && \\
               bcftools index -t ${sample_id}.vcf.gz"
    T3=\$(date +%s.%N)
    VCF_BYTES=\$(stat -c%s ./${sample_id}.vcf.gz 2>/dev/null || echo 0)
    rm -f ./${sample_id}.bam ./${sample_id}.bam.bai

    # ── Emit per-sample data-movement timings (published to <outdir>/staging/) ─
    DL_S=\$(awk -v a=\$T0 -v b=\$T1 'BEGIN{printf "%.3f", b-a}')
    CALL_S=\$(awk -v a=\$T2 -v b=\$T3 'BEGIN{printf "%.3f", b-a}')
    DL_MBPS=\$(awk -v by=\$BAM_BYTES -v s=\$DL_S 'BEGIN{ if(s>0) printf "%.2f",(by/1048576)/s; else printf "0" }')
    cat > ${sample_id}.timings.json <<TIMINGS_EOF
{"sample_id":"${sample_id}","population":"${population}","super_population":"${super_population}","instance_type":"\${INSTANCE_TYPE}","instance_id":"\${INSTANCE_ID}","az":"\${AZ}","lifecycle":"\${LIFECYCLE}","vcpus":\${VCPUS},"arch":"\${UNAME_ARCH}","bam_download_s":\${DL_S},"bam_bytes":\${BAM_BYTES},"bam_mbps":\${DL_MBPS},"bcftools_call_s":\${CALL_S},"vcf_gz_bytes":\${VCF_BYTES}}
TIMINGS_EOF
    aws s3 cp ${sample_id}.timings.json ${params.outdir}staging/${sample_id}.timings.json \\
        --region us-east-1 --no-progress || true

    echo "Completed variant calling for sample ${sample_id}"
    """
}


// ── MERGE_VCFS ───────────────────────────────────────────────────────────────
// Cohort fan-in: merge all per-sample VCFs into one multi-sample VCF. This is a
// pure-bcftools step, so (unlike CALL_VARIANTS) it runs INSIDE the container via
// the normal Nextflow `container` directive (set in nextflow.config).
process MERGE_VCFS {
    label 'process_high'

    input:
    path('vcfs/*')
    path('vcfs_idx/*')

    output:
    tuple path('merged.vcf.gz'), path('merged.vcf.gz.tbi')

    script:
    """
    set -euxo pipefail
    # bcftools merge needs each VCF's tabix index NEXT TO its .vcf.gz. Nextflow
    # stages the VCFs and their .tbi into SEPARATE dirs (vcfs/ and vcfs_idx/),
    # both read-only mounts, so collect them together into one writable dir first.
    mkdir -p merged_in
    cp vcfs/*.vcf.gz merged_in/
    cp vcfs_idx/*.tbi merged_in/
    ls merged_in/*.vcf.gz > vcf_list.txt
    bcftools merge -l vcf_list.txt -Oz -o merged.vcf.gz
    bcftools index -t merged.vcf.gz
    echo "Completed merging of all per-sample VCFs"
    """
}


// ── VCF_STATS ────────────────────────────────────────────────────────────────
// Cohort QC: bcftools stats → a small stats.json the dashboard + analysis read.
// Ti/Tv (transition/transversion) ratio is the key population-genetics sanity
// check — ~2.0–2.1 genome-wide for real human variant calls.
process VCF_STATS {
    label 'process_single'

    input:
    tuple path(vcf), path(vcf_idx)

    output:
    path('stats.txt')
    path('stats.json')

    script:
    """
    set -euxo pipefail
    bcftools stats ${vcf} > stats.txt

    # Pull the population-genetics QC numbers out of the stats text. The 'SN'
    # (summary numbers) lines are tab-separated with the value in the last field.
    # The ts/tv ratio is NOT an SN line — it's the 'TSTV' data row whose columns
    # are [TSTV][id][ts][tv][ts/tv]..., so ts/tv is field 5. All greps are
    # `|| true` so a missing line never trips `set -e`; defaults fill in after.
    SNPS=\$(grep -m1 "number of SNPs:" stats.txt | awk '{print \$NF}' || true)
    INDELS=\$(grep -m1 "number of indels:" stats.txt | awk '{print \$NF}' || true)
    RECORDS=\$(grep -m1 "number of records:" stats.txt | awk '{print \$NF}' || true)
    TSTV=\$(grep -m1 -E "^TSTV" stats.txt | awk -F'\\t' '{print \$5}' || true)
    : "\${SNPS:=0}" "\${INDELS:=0}" "\${RECORDS:=0}" "\${TSTV:=0}"

    cat > stats.json <<STATS_EOF
{"total_records": \${RECORDS}, "snps": \${SNPS}, "indels": \${INDELS}, "ti_tv_ratio": \${TSTV}}
STATS_EOF
    echo "Completed statistics calculation"
    """
}


// ── Workflow ─────────────────────────────────────────────────────────────────
workflow {
    // Parse the samples CSV: sample_id,population,super_population,bam_path
    Channel
        .fromPath(params.samples)
        .splitCsv(header: true)
        .map { row -> [ row.sample_id, row.population, row.super_population, row.bam_path ] }
        .set { samples_ch }

    // Fan-out: one nf-spawn EC2 instance per sample. The shared reference is read
    // DIRECTLY off the FSx mount (params.fsx_mount/reference + .fai) inside the
    // task — not staged as a Nextflow path input — so it is genuinely zero-copy.
    CALL_VARIANTS(samples_ch)

    // Fan-in: collect all per-sample VCFs + their indexes, merge into a cohort VCF.
    CALL_VARIANTS.out.map { it[3] }.collect().set { vcfs_ch }
    CALL_VARIANTS.out.map { it[4] }.collect().set { vcfs_idx_ch }
    MERGE_VCFS(vcfs_ch, vcfs_idx_ch)

    // Cohort QC stats (Ti/Tv, SNP/indel counts).
    VCF_STATS(MERGE_VCFS.out)
}
