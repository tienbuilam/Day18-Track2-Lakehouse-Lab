# Bonus Challenge — Topic D: Multimodal RAG trên 10M Document Pháp Lý

> **Sinh viên:** Doumaa · VinUni AI20k · Day 18 Track 2

---

## 1. Problem Statement

Một văn phòng luật VN (50 luật sư, 2 engineer) cần hệ thống tra cứu thông minh trên **10 triệu document pháp lý** — bản án TAND, nghị định, thông tư, hợp đồng mẫu. Document gồm 3 dạng: text thuần (~60%), ảnh scan cần OCR (~25%), và bảng dữ liệu (~15%).

**Tại sao khó:**

- **Scale:** 10M PDF → **~30 tỉ chunks**.
- **Multimodal:** Bảng điều khoản phạt và ảnh scan chiếm ~40% nội dung quan trọng — không chỉ text.
- **Reproducibility 5 năm:** Khi bản án năm 2026 trích dẫn "NĐ 15/2020 version ngày 15/03/2021", retrieval phải trả đúng version đó — dù document đã qua 3 lần sửa đổi. Đây chính xác là use case **model reproducibility — pin training set version**.
- **Embedding lifecycle:** `doc_v × model_v` = 2 chiều versioning. Mỗi lần upgrade model (e.g., ada-002 → text-embedding-3-large), 30B chunks phải re-embed — không downtime.
- **Latency:** p95 < 200ms — vector DB = derived index cho online ANN.
- **Compliance:** **Nghị định 13/2023/NĐ-CP**: dữ liệu pháp lý = sensitive data → data residency VN → on-prem MinIO, không cloud nước ngoài.

**Budget:** $15K/tháng recurring, on-prem.

---

## 2. Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────┐
│ INGESTION PATH                                                        │
│                                                                       │
│ [PDF Upload API]                                                      │
│      │                                                                │
│      ▼                                                                │
│ ┌─────────┐  raw bytes   ┌────────────────────────────────────────┐   │
│ │ Kafka   │ ───────────► │ BRONZE (Delta Lake, MinIO on-prem)     │   │
│ │ pdf.raw │              │ s3://lh-law/bronze/raw_docs            │   │
│ └─────────┘              │ PARTITIONED BY (ingest_date)           │   │
│                          │ Snappy compression (fast write)        │   │
│                          │ Retention: 30d Standard → Glacier 5yr  │   │
│                          └────────────────┬───────────────────────┘   │
│                                           │                           │
│                 Unstructured.io (on-prem) │ OCR + table extraction    │
│                 Great Expectations gate   ▼                           │
│                          ┌────────────────────────────────────────┐   │
│                          │ SILVER (Delta Lake)                    │   │
│                          │ s3://lh-law/silver/doc_chunks          │   │
│                          │ PARTITIONED BY (doc_date)              │   │
│                          │ Z-ORDER BY (doc_type, source)          │   │
│                          │ ZSTD compression (slow write OK)       │   │
│                          │ CDF enabled → downstream incremental   │   │
│                          │ Deletion Vectors ON (GDPR delete)      │   │
│                          └────────────────┬───────────────────────┘   │
│                                           │                           │
│                Embedding model (versioned)│ CDF → only embed changed  │
│                doc_v × model_v tracking   ▼                           │
│                          ┌────────────────────────────────────────┐   │
│                          │ SILVER-EMBED (Lance)                   │   │
│                          │ s3://lh-law/silver-embed/              │   │
│                          │   ├── text-embedding-ada-002/ (frozen) │   │
│                          │   ├── text-embedding-3-large/ (active) │   │
│                          │   └── future-model/ (shadow build)     │   │
│                          │ Native HNSW + versioning built-in      │   │
│                          └────────────────┬───────────────────────┘   │
│                                           │                           │
│                Canary test (recall ≥ 98%) ▼                           │
│                          ┌────────────────────────────────────────┐   │
│                          │ GOLD (Delta + Lance)                   │   │
│                          │ - HNSW index (Lance, query-ready)      │   │
│                          │ - citation_log (Delta, audit trail)    │   │
│                          │ - doc_quality_metrics (Delta)          │   │
│                          │ ZSTD, Z-ORDER BY (query_date)          │   │
│                          └────────────────┬───────────────────────┘   │
└───────────────────────────────────────────┼───────────────────────────┘
                                            │
┌───────────────────────────────────────────▼───────────────────────────┐
│ QUERY PATH (p95 < 200ms budget)                                       │
│                                                                       │
│ [Luật sư query] → [Embed ~30ms] → [HNSW ANN ~50ms] → [Re-rank ~40ms]  │
│      → [Fetch chunks từ Silver Delta ~30ms] → [LLM generate] → [Cite] │
│                                                                       │
│ Vector DB = DERIVED INDEX, rebuildable                                │
│ System of record = Delta tables (Bronze + Silver)                     │
└───────────────────────────────────────────────────────────────────────┘
```

**Compression strategy**: Bronze = Snappy (fast write, append-only); Silver/Gold = ZSTD (3× smaller, slow write OK for batch transforms).

---

## 3. Các Quyết Định Chính

### QĐ 1: Storage format — Iceberg vs Delta Lake vs Hudi

**Chọn: Delta Lake** cho metadata/chunks, **Lance** cho embeddings.

| Tiêu chí | Delta Lake | Iceberg | Hudi |
|---|---|---|---|
| Workload | Append + MERGE | Multi-engine | Mutation-heavy |
| Hidden partitioning | ❌ Cần explicit | ✅ `days(ts)` auto-prune | ❌ |
| Lab đã dùng | ✅ NB1–NB4 sẵn | ❌ Setup mới | ❌ |
| Deletion Vectors | ✅ (v2.3+) | ✅ (v3) | ✅ (MOR) |
| CDF (Change Data Feed) | ✅ Native | ❌ Cần external | ❌ |
| Nessie branching | Tag only | ✅ Git-like | ❌ |

**Lý do chọn Delta thay Iceberg:**

Iceberg có hidden partitioning tốt hơn (đa số performance regression vì user quên partition column). Nhưng:

1. **CDF là critical** cho embedding pipeline — chỉ re-embed chunks đã thay đổi, không toàn bộ 30B. Delta CDF native; Iceberg cần custom.
2. **1 tuần deadline** — team đã quen Delta từ NB1–NB4 (chọn theo tooling fit).
3. **Deletion Vectors** cho GDPR/Nghị định 13: `DELETE WHERE user_id=X` → 10–100× nhanh hơn rewrite.

**Loại Hudi:** MOR fast update nhưng complex ops, team 2 người không đủ bandwidth debug.

**Trade-off chấp nhận:** Mất hidden partitioning → phải explicit `PARTITIONED BY (doc_date)` + Z-ORDER. Team phải nhớ filter `doc_date` — anti-pattern #2 nếu quên (partition theo high-cardinality).

**Lance cho embeddings** (Random access ~2000× faster than Parquet. Native HNSW. Built-in versioning.). Delta tối ưu cho batch scan, không cho vector lookup — đọc 1 embedding từ Delta = scan cả Parquet partition.

---

### QĐ 2: Vector Index — HNSW (tiered) vs IVF-PQ vs Brute Force

**Chọn: Tiered HNSW** — hot tier trên RAM, cold tier trên disk.

| | HNSW (hot) | IVF-PQ (cold) | Brute Force |
|---|---|---|---|
| Query latency | ~5-20ms | ~10-30ms | O(N) |
| Recall@10 | 98%+ | 90-95% | 100% |
| RAM | ~100 GB (20% corpus) | ~10-20 GB (quantized) | N/A |

**Loại IVF-PQ cho hot tier:** PQ quantization mất thông tin → recall 90-95%. Trong domain pháp lý, bỏ sót 5-10% kết quả = bỏ lỡ án lệ quan trọng. Vector DB = derived index → rebuild được, nên chọn recall cao.

**Loại Brute Force:** 30B vectors × 1536 dims × 4 bytes = 180 TB. Scan 180 TB trong 200ms = vô lý.

**Tiered approach:** Hot tier = 5 năm gần nhất (~20% corpus, ~6B vectors) → fit 100 GB RAM HNSW. Cold tier = cũ hơn → IVF-PQ trên disk, chấp nhận recall thấp hơn cho query hiếm.

---

### QĐ 3: OCR Pipeline — Unstructured.io (on-prem) vs Tesseract vs Azure Doc Intelligence

**Chọn: Unstructured.io self-hosted**

**Loại Azure Doc Intelligence:** Nghị định 13/2023/NĐ-CP: sensitive data → data residency VN. Document pháp lý không được gửi cloud nước ngoài.

**Loại Tesseract:** Chỉ ảnh → text, không hiểu table layout. Bảng điều khoản phạt trong hợp đồng VN = multi-column → Tesseract output hỗn độn.

**Trade-off:** Unstructured.io on-prem cần GPU server ($2K/tháng). Bất khả kháng vì constraint on-prem.

---

### QĐ 4: Embedding versioning — Separate Lance datasets vs Column-based

**Chọn: Separate Lance datasets per `model_v`**

Embedding version = `doc_v × model_v` → pin via Delta version + MLflow run_id.

```
s3://lh-law/silver-embed/
  ├── text-embedding-ada-002/    ← v1, frozen sau khi v2 ready
  ├── text-embedding-3-large/    ← v2, active serving
  └── future-model/              ← v3, shadow build (canary test chưa pass)
```

**Loại Column-based** (thêm `embedding_v2` column): 30B × 1536 dims × 4 bytes × 2 versions = **360 TB** trong 1 table. Scan cost gấp đôi cho mọi query.

**Migration flow** (zero-downtime):

1. **Full re-embed toàn bộ 30B chunks** với model v2 (background job, ~2-4 tuần) — migration model mới = không gian vector hoàn toàn khác, không tương thích với v1, phải embed lại tất cả.
2. Canary test 100 queries → recall ≥ 98% so với expected → pass → swap pointer
3. Keep v1 frozen 30 ngày → archive S3 IA → Glacier sau 90d
4. `citation_log` ghi `embed_model_version` + `silver_delta_version` mỗi query → audit trail reproducibility

> **CDF (Change Data Feed)** hữu ích cho **ongoing incremental** sau khi migration xong: chỉ re-embed chunks mới thêm hoặc đã sửa nội dung trong cùng model version. Không giúp được khi đổi model vì toàn bộ vector space thay đổi.

---

### QĐ 5: Catalog — Lakekeeper vs Unity Catalog vs Nessie

**Chọn: Lakekeeper** (REST Catalog, Rust, K8s-native)

REST Catalog spec = lingua franca 2026.

| | Lakekeeper | Unity Catalog | Nessie |
|---|---|---|---|
| Open-source | ✅ Rust, lightweight | ✅ (2024 OSS) | ✅ |
| On-prem deploy | ✅ K8s-native | ❌ Phức tạp | ✅ |
| Git-like branching | ❌ | ❌ | ✅ |
| Multi-engine | ✅ REST spec | Databricks-native | ✅ |

**Loại Nessie:** Git-like branching rất hấp dẫn cho embedding version migration (`nessie branch create exp-2026`). Nhưng Nessie chưa hỗ trợ Lance format, chỉ Iceberg. Nếu sau này migrate sang Iceberg + Lance hybrid, Nessie là candidate đầu tiên.

**Loại Unity Catalog:** Vendor lock-in Databricks. On-prem deployment = expensive license, không hợp budget $15K/tháng.

---

### QĐ 6: Data Quality — 2-tool stack

**Chọn: dbt tests + Soda** (bỏ Great Expectations riêng cho team 2 người)

| Gate | Tool | Kiểm tra gì |
|---|---|---|
| Bronze → Silver | **dbt tests** | `ocr_confidence > 0.85`, unique `chunk_id`, not_null `doc_id`, FK → `bronze.raw_docs`, table structure validation |
| Production monitoring | **Soda** | `freshness < 1h`; `row_count >= expected`; anomaly detection trên query latency |

**Loại Great Expectations riêng:** dbt tests đã cover phần lớn use case của GE (not_null, unique, accepted_values, custom SQL tests). Duy trì 3 tool stack = 3 places để debug khi pipeline fail — overhead quá cao cho team 2 engineer. Chỉ giữ lại GE nếu OCR validation cần expectations phức tạp dbt không handle được (ví dụ: distribution-based checks trên `ocr_confidence`).

Data Contract giữa OCR team → embedding team → RAG serving: schema + constraints + freshness SLA. Bể contract → block pipeline, không pass bad data downstream (run trong CI pre-merge và runtime per-batch).

---

### QĐ 7: Chunking strategy — Fixed-size 512 vs Semantic chunking

**Chọn: Fixed-size 512 tokens, 50% overlap, respect heading boundaries**

**Loại Semantic chunking:** Cho recall tốt hơn nhưng **non-deterministic** — cùng document, khác sentence splitter → khác chunks → khác embeddings → **không reproducible** sau 5 năm. Contradicts core requirement.

**Trade-off:** ~30B chunks thay vì ~20B (nhiều hơn ~50%). Đổi lại: deterministic, reproducible, dễ estimate storage.

---

## 4. Failure Modes

### Failure 1: Embedding model deprecated → stale vectors

**Kịch bản 3h sáng:** OpenAI deprecate `text-embedding-ada-002` trong 6 tháng. Toàn bộ 30B vectors v1 phải migrate.

**Detect:** Monitor deprecation announcements + alert khi API trả về deprecation warning header.

**Rollback:** Lance datasets versioned — nếu v2 migration bị lỗi (recall giảm):

```python
catalog.update_serving_pointer("silver-embed", version="text-embedding-ada-002")
```

Atomic pointer swap, < 5 phút, zero downtime. Delta `citation_log` ghi mọi query đã dùng version nào → biết user nào bị ảnh hưởng. Đây chính là use case **"model reproducibility — pin training set version"** nhưng áp dụng cho embedding serving thay vì training.

---

### Failure 2: OCR parse sai bảng → luật sư nhận thông tin sai

**Kịch bản:** Unstructured.io parse sai bảng điều khoản phạt → nhầm cột "mức phạt" với "thời hạn" → LLM generate số sai → luật sư tư vấn nhầm cho khách hàng.

**Detect:** Great Expectations gate tại Bronze → Silver:

- `table_cell_count` match metadata
- Cross-validate: Unstructured vs PaddleOCR disagreement > 10% → flag `review_status = "pending"`

**Rollback:** Chunk bị flag → không index vào Lance → không thể retrieve. Human review trước khi approve. (bể contract → block pipeline)

---

### Failure 3: Document xóa nhầm + bản án mất reference

**Kịch bản:** Admin xóa nhầm "NĐ 15/2020". Bản án 2022 trích dẫn nghị định này → reference broken.

**Detect:** FK check `citation_log.source_doc_id` ∉ Silver → alert.

**Rollback (`restoreToVersion`):**

```sql
RESTORE TABLE silver.doc_chunks VERSION AS OF <before_delete>;
-- Delta retention 1825 days (5 năm) → restore bất kỳ document nào
```

**Nhưng VACUUM là risk:** Nếu VACUUM chạy → physical files bị xóa → không restore được. Vì vậy:

- `delta.deletedFileRetentionDuration = 1825 days` (dài = nhiều cost, audit tốt)
- Anti-pattern #4: **KHÔNG BAO GIỜ** `VACUUM 0 HOURS` trên table pháp lý

---

### Failure 4: Right-to-forget request

**Kịch bản:** Đương sự yêu cầu xóa thông tin cá nhân theo Nghị định 13/2023/NĐ-CP.

**Approach:**

1. `DELETE FROM silver.doc_chunks WHERE contains_pii_of(user_id=X)` — dùng **Deletion Vectors**: không rewrite cả Parquet file, chỉ ghi bitmap sidecar. 10–100× nhanh hơn rewrite, đủ đáp ứng **72h SLA**.
2. Ghi audit log ngay vào `gold.pii_deletion_log` — bằng chứng compliance Nghị định 13 (soft delete đã xong).
3. Rebuild affected Lance embeddings (xóa vectors của deleted chunks).
4. Scheduled VACUUM (không phải 0 HOURS): chạy định kỳ hàng tuần để physical removal.

**Lưu ý kỹ thuật về VACUUM:**

- Delta **không hỗ trợ** `VACUUM WHERE partition=X` — VACUUM chạy trên toàn bộ table.
- Delta mặc định **chặn** `RETAIN < 7 days`, cần: `spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")` — override nguy hiểm, chỉ dùng khi có legal mandate.
- Với table pháp lý retention 5 năm: chỉ VACUUM khi có yêu cầu right-to-forget cụ thể, không scheduled thường xuyên.

**Conflict: Reproducibility vs GDPR delete.** Sau VACUUM, time travel cho chunks đó không còn. Giải pháp: `citation_log` giữ `chunk_id` + `query_ts` nhưng **không giữ chunk content** — đủ cho audit trail, PII đã physical delete.

---

## 5. Ước Tính Chi Phí (Show the Math)

**Methodology:** Dùng on-prem MinIO (Nghị định 13), estimate theo S3 pricing tương đương.

### Storage

| Layer | Calculation | Size | Tier | $/tháng |
|---|---|---|---|---|
| Bronze (raw PDF) | 10M × 5 MB avg | 50 TB | Snappy, IA sau 30d | $575 |
| Silver (chunks + tables) | 30B chunks × ~170 bytes avg (ZSTD) | ~5 TB | Standard | $115 |
| Silver-Embed v1 (frozen) | 30B × 1536 dims × 4 bytes | 180 TB | IA (frozen) | $2,070 |
| Silver-Embed v2 (active) | 30B × 1536 dims × 4 bytes | 180 TB | Standard | $4,140 |
| Gold (HNSW hot) | 20% corpus on RAM | ~100 GB | EC2 r6g.4xlarge | $730 |
| Gold (citation_log + metrics) | Negligible | <1 GB | Standard | ~$0 |
| **Storage total** | | | | **~$7,630/tháng** |

### Compute

| Component | Spec | $/tháng |
|---|---|---|
| OCR ongoing (~5K doc/ngày) | g4dn.xlarge × 8h/ngày on-prem | $403 |
| Embedding ongoing | ~15M tokens/ngày × $0.0001/1K | $450 |
| HNSW serving | r6g.4xlarge (128 GB RAM) | $730 |
| RAG API + re-ranker | c6g.2xlarge × 2 | $296 |
| **Compute total** | | **~$1,879/tháng** |

**Tổng recurring: ~$9,500/tháng** (dưới budget $15K/tháng)

**One-time backfill:**

- OCR 10M PDF: GPU g4dn.xlarge × 60 ngày = ~$6,000
- Embedding 30B tokens: $3,000 (text-embedding-3-small) hoặc $15,000 (text-embedding-3-large)

**FinOps rules** (savings chỉ materialize nếu actively optimize):

- Bronze → Glacier sau 30d (ít re-access): giảm ~60% bronze storage
- Silver-Embed v1 frozen → IA ngay sau swap: giảm ~40%
- `OPTIMIZE` nightly cron trên Silver: "bỏ qua OPTIMIZE → 10× chậm"
- KHÔNG chạy Spark cluster cho Silver query < 100 GB → DuckDB

---

## 6. MVP 1 Tuần

**Mục tiêu:** Prove kiến trúc trên 1,000 document mẫu — không cần 10M.

| Day | Task | Deliverable | Risk |
|---|---|---|---|
| D1 | Bronze ingest: 1K PDF → Delta (MinIO on-prem) | `bronze.raw_docs` có `_delta_log/` visible trên MinIO console | Thấp |
| D2 | Silver extraction: Unstructured.io → chunks + table JSON. Great Expectations gate. | `silver.doc_chunks` với `ocr_confidence` và `review_status` | Trung bình |
| D3 | Embedding v1 → Lance dataset. Pin Delta version + model version. | `silver-embed/text-embedding-3-small/` Lance dataset | Thấp |
| D4 | HNSW build + FastAPI search. Benchmark p95 latency. | Query "điều khoản bồi thường" → top-5 trong < 200ms | Trung bình |
| D5 | **Embedding migration v1→v2.** Shadow build, canary test, atomic swap. | Parallel Lance datasets, recall comparison, pointer swap | **Cao** |
| D6 | **Reproducibility test.** Query tại `VERSION AS OF` + citation_log replay. | Cùng query + cùng version → cùng kết quả sau 1 tuần | Trung bình |
| D7 | Buffer + writeup | `ARCHITECTURE.md` | Thấp |

**Slice nhỏ nhất shippable (3 ngày):** D1 + D3 + D5 — prove phần khó nhất (embedding versioning + migration) mà không cần full OCR pipeline. Dùng text-only PDF, skip OCR.
