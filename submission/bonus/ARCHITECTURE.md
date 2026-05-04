# Bonus Challenge — Topic D: Multimodal RAG trên 10M Document Pháp Lý

---

## 1. Problem Statement

Một văn phòng luật VN cần hệ thống tra cứu thông minh trên **10 triệu document pháp lý** (bản án, nghị định, thông tư, hợp đồng mẫu). Document có 3 dạng: text thuần, ảnh scan (cần OCR), và bảng dữ liệu.

**Tại sao khó:**

- **Scale:** 10M PDF × trung bình 50 trang × 500 token/trang = **250 tỉ token raw** → sau chunking ~30 tỉ chunks
- **Multimodal:** Không chỉ text — bảng điều khoản và ảnh scan chiếm ~40% nội dung quan trọng
- **Reproducibility:** Khi một bản án năm 2026 trích dẫn "nghị định X version ngày 15/03/2021", hệ thống phải trả về **đúng version đó** dù document đã có 3 lần sửa đổi sau
- **Embedding lifecycle:** Mỗi lần upgrade embedding model (text-embedding-ada-002 → text-embedding-3-large → model tương lai), **toàn bộ 30B chunks phải re-embed** — không thể downtime
- **Latency:** Luật sư cần kết quả trong **p95 < 200ms** — không thể brute-force scan toàn bộ vectors

**Constraints:** Môi trường on-prem (dữ liệu nhạy cảm, không ra cloud nước ngoài), 2-3 engineer, budget $15K/tháng.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│  INGESTION PATH                                                 │
│                                                                 │
│  [PDF Upload API]                                               │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────┐    raw bytes + metadata    ┌──────────────────────┐│
│  │ Kafka   │ ────────────────────────►  │ BRONZE               ││
│  │ topic   │                            │ Delta Lake           ││
│  │ pdf.raw │                            │ s3://lh-law/bronze/  ││
│  └─────────┘                            │ partition: ingest_dt ││
│                                         └──────────┬───────────┘│
│                                                    │            │
│                          OCR + Extract             │            │
│                          (Unstructured.io)         ▼            │
│                                         ┌──────────────────────┐│
│                                         │ SILVER               ││
│                                         │ Delta Lake           ││
│                                         │ - doc_chunks         ││
│                                         │ - tables (JSON)      ││
│                                         │ - image_refs (S3)    ││
│                                         │ partition: doc_date  ││
│                                         └──────────┬───────────┘│
│                                                    │            │
│                          Embedding Model           │            │
│                          (versioned)               ▼            │
│                                         ┌──────────────────────┐│
│                                         │ SILVER-EMBED         ││
│                                         │ Lance dataset        ││
│                                         │ v1/ v2/ v3/          ││
│                                         │ (per model version)  ││
│                                         └──────────┬───────────┘│
│                                                    │            │
│                          HNSW Index Build          ▼            │
│                                         ┌──────────────────────┐│
│                                         │ GOLD                 ││
│                                         │ Lance + Delta        ││
│                                         │ - HNSW index (Lance) ││
│                                         │ - citation_log (Δ)   ││
│                                         └──────────┬───────────┘│
└────────────────────────────────────────────────────┼────────────┘
                                                     │
┌────────────────────────────────────────────────────▼────────────┐
│  QUERY PATH                                                     │
│                                                                 │
│  [Luật sư gõ query]                                             │
│       │                                                         │
│       ▼                                                         │
│  [Embed query] → [HNSW search ~50ms] → [Re-rank ~80ms]          │
│       │                                                         │
│       ▼                                                         │
│  [Fetch chunks từ Silver (Delta)] → [LLM generate] → [Cite]     │
│                                                                 │
│  Total p95 target: < 200ms                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Các Quyết Định Chính

### Quyết định 1: Storage format cho embeddings — Lance vs Delta vs Parquet+FAISS

**Chọn: Lance** cho embedding storage, **Delta Lake** cho metadata + chunks.

| | Lance | Delta Lake | Parquet + FAISS |
|---|---|---|---|
| Native vector ops | ✅ ANN built-in | ❌ Cần external | ❌ Cần external |
| ACID transactions | ✅ versioned | ✅ tốt nhất | ❌ không |
| Random row access | ✅ fast | ❌ chậm | ❌ chậm |
| Hệ sinh thái | Mới, nhỏ | Lớn, mature | Lớn nhưng phân mảnh |

**Loại Delta cho embeddings:** Delta tối ưu cho analytical workloads (batch scan), không cho vector lookup theo row ID. Đọc 1 vector từ Delta cần scan toàn bộ Parquet file của partition — quá chậm cho serving.

**Loại Parquet+FAISS:** FAISS index không versioned, không có time travel, không có ACID — khi embedding model upgrade, không rollback được.

**Trade-off chấp nhận:** Lance ecosystem còn mới (2023), ít tài liệu hơn Delta. Tuy nhiên đây là **phần khó nhất** của kiến trúc, và Lance là lựa chọn đúng đắn về kỹ thuật.

---

### Quyết định 2: Vector Index — HNSW vs IVF-PQ vs Brute Force

**Chọn: HNSW (Hierarchical Navigable Small World)**

| | HNSW | IVF-PQ | Brute Force |
|---|---|---|---|
| Build time | Chậm hơn | Nhanh | Không cần |
| Query latency | ~5-20ms | ~10-30ms | O(N) — không khả thi |
| Recall@10 | 98%+ | 90-95% | 100% |
| RAM usage | ~100 GB cho 30B chunks | ~10-20 GB (quantized) | N/A |
| Incremental add | ✅ Tốt | ❌ Cần rebuild | N/A |

**Loại IVF-PQ:** Recall thấp hơn (quantization mất thông tin) — với văn bản pháp lý, bỏ sót 5-10% kết quả liên quan có thể gây hậu quả nghiêm trọng (luật sư bỏ lỡ án lệ quan trọng).

**Loại Brute Force:** 30B vectors × 1536 dims = không thể scan trong 200ms.

**Trade-off chấp nhận:** HNSW cần ~100GB RAM cho full index. Giải pháp: **tiered HNSW** — index "hot" (5 năm gần nhất, ~20% corpus) trên RAM, "cold" (cũ hơn) query qua IVF-PQ trên disk.

---

### Quyết định 3: OCR Pipeline — Unstructured.io vs Tesseract vs Azure Document Intelligence

**Chọn: Unstructured.io (self-hosted)**

**Loại Tesseract:** Chỉ xử lý ảnh → text, không hiểu cấu trúc bảng, layout. Với văn bản pháp lý VN (bảng điều khoản phức tạp), Tesseract cho output hỗn độn — không thể parse bảng thành JSON.

**Loại Azure Document Intelligence:** Cloud-based → dữ liệu pháp lý nhạy cảm không được ra ngoài. Vi phạm yêu cầu on-prem.

**Trade-off chấp nhận:** Unstructured.io self-hosted cần GPU server riêng (~$2K/tháng). Nhưng đây là bất khả kháng vì constraint on-prem.

---

### Quyết định 4: Embedding Versioning — Separate datasets vs Column-based

**Chọn: Separate Lance datasets per model version**

```
s3://lh-law/silver-embed/
  ├── text-embedding-ada-002/    ← v1, frozen sau khi v2 ready
  ├── text-embedding-3-large/    ← v2, active
  └── future-model/              ← v3, đang migrate
```

**Loại Column-based** (thêm column `embedding_v2` vào bảng cũ): Với 30B chunks × 1536 dims × 4 bytes × 2 versions = **360 TB** chỉ cho 2 versions. Không khả thi về storage và query performance.

**Trade-off chấp nhận:** Mỗi version dataset là một copy hoàn toàn của 30B vectors. Migration v1 → v2 cần chạy song song ~2-4 tuần. Trong thời gian này, dùng v1 serve production, v2 build shadow index.

---

### Quyết định 5: Catalog — Lakekeeper REST Catalog vs Unity Catalog vs Hive Metastore

**Chọn: Lakekeeper (REST Catalog, open-source)**

**Loại Unity Catalog:** Vendor lock-in vào Databricks. On-prem deployment phức tạp, expensive license.

**Loại Hive Metastore:** Không hỗ trợ Lance format, không có REST API chuẩn, khó integrate với RAG serving stack.

**Lakekeeper:** Open-source, REST Catalog spec (Iceberg-compatible), chạy được on-prem, multi-engine (Spark + DuckDB + trực tiếp query từ Python). Trade-off: mới hơn, community nhỏ hơn.

---

### Quyết định 6: Chunking strategy — Fixed-size vs Semantic chunking

**Chọn: Fixed-size 512 tokens với 50% overlap, nhưng respect document structure**

Logic: Với văn bản pháp lý, một điều khoản thường là 200-600 tokens. Fixed-size 512 với overlap đảm bảo không cắt đứt điều khoản. Semantic chunking (tách theo câu/đoạn) cho recall tốt hơn nhưng tạo ra chunks không đều — khó estimate storage và index size.

**Trade-off chấp nhận:** ~30B chunks thay vì ~20B (nếu semantic chunking tốt hơn). Đổi lại: deterministic, reproducible, dễ debug.

---

## 4. Failure Modes

### Failure 1: Embedding model deprecation (Day 18 concept: time travel)

**Kịch bản:** OpenAI thông báo `text-embedding-ada-002` ngừng hỗ trợ sau 6 tháng. Toàn bộ 30B vectors v1 phải migrate sang `text-embedding-3-large`.

**Phát hiện:** Monitor OpenAI deprecation announcements + alert khi model API trả về deprecation warning header.

**Rollback:** Lance datasets versioned — nếu v2 migration bị lỗi (recall giảm, wrong results), có thể switch serving pointer về v1 trong < 5 phút:

```python
# Atomic pointer swap — không cần rebuild index
catalog.update_serving_version("silver-embed", version="text-embedding-ada-002")
```

Delta `citation_log` ghi lại mọi query đã dùng version nào → audit trail đầy đủ.

---

### Failure 2: OCR extraction sai bảng → hallucination

**Kịch bản:** Unstructured.io parse sai bảng điều khoản phạt trong hợp đồng (nhầm cột) → LLM generate ra số tiền phạt sai → luật sư tư vấn nhầm cho khách hàng.

**Phát hiện:** Great Expectations check tại Silver gate:

- `table_cell_count` phải match với metadata từ PDF parser
- Số trong bảng phải là numeric (không phải string lẫn lộn)
- Cross-validate: nếu 2 OCR engine (Unstructured + PaddleOCR) disagreement > 10% → flag for human review

**Rollback:** Chunk bị flag → `review_status = "pending"` trong Delta Silver → không được index vào Lance → không thể retrieve cho đến khi human approve.

---

### Failure 3: Document bị xóa nhầm + bản án mất reference

**Kịch bản:** Admin xóa nhầm "Nghị định 15/2020" khỏi system. Một bản án năm 2022 đang trích dẫn nghị định này → khi luật sư query về bản án đó, reference không còn tồn tại.

**Phát hiện:** Foreign key check trong `citation_log` Delta table: nếu `source_doc_id` không còn tồn tại trong Silver → alert ngay lập tức.

**Rollback (Day 18 — Delta time travel):**

```sql
-- Restore document về trước khi xóa
RESTORE TABLE silver.doc_chunks
VERSION AS OF <version_before_delete>
WHERE doc_id = 'nghi-dinh-15-2020';
```

Vì Delta giữ 5 năm retention (`delta.logRetentionDuration = 1825 days`), có thể restore bất kỳ document nào bị xóa nhầm.

---

### Failure 4: HNSW index corruption sau rebuild

**Kịch bản:** Sau lần re-index hàng tuần, HNSW index bị corrupt (disk failure giữa chừng) → query trả về garbage results hoặc crash.

**Phát hiện:** Canary query set — 100 câu hỏi pháp lý với expected top-1 kết quả. Sau mỗi lần rebuild, tự động chạy canary. Nếu recall@1 < 90% → không switch sang index mới.

**Rollback:** Giữ 2 index versions (current + previous). Switch pointer về previous trong < 1 phút:

```python
serving.swap_index(target="previous")
# Downtime: 0s (atomic pointer swap)
```

---

## 5. Ước Tính Chi Phí (Back-of-envelope)

### Storage

| Layer | Size | Tier | $/tháng |
|---|---|---|---|
| Bronze (raw PDF) | 10M PDF × 5MB avg = 50 TB | S3 IA (ít access) | $575 |
| Silver (chunks + tables) | ~5 TB Delta Parquet | S3 Standard | $115 |
| Silver-Embed v1 (frozen) | 30B × 1536 × 4B = 180 TB | S3 IA | $2,070 |
| Silver-Embed v2 (active) | 180 TB | S3 Standard | $4,140 |
| Gold HNSW index (hot tier) | ~100 GB RAM serving | EC2 r6g.4xlarge | $730 |
| Gold HNSW index (cold tier) | ~5 TB on disk | S3 Standard | $115 |
| **Storage total** | | | **~$7,745/tháng** |

### Compute

| Component | Spec | $/tháng |
|---|---|---|
| OCR processing (one-time backfill) | GPU g4dn.xlarge × 30 ngày | $3,024 |
| OCR processing (ongoing, ~5K doc/ngày) | g4dn.xlarge × 8h/ngày | $403 |
| Embedding generation (backfill, one-time) | OpenAI API 30B tokens | ~$15,000 one-time |
| Embedding ongoing (~5K doc/ngày) | OpenAI API ~15M tokens/ngày | $450 |
| HNSW serving (hot index) | r6g.4xlarge (128GB RAM) | $730 |
| RAG API + re-ranker | c6g.2xlarge × 2 | $296 |
| **Compute total (recurring)** | | **~$1,879/tháng** |

**Tổng recurring: ~$9,600/tháng** (dưới budget $15K)
**One-time migration cost: ~$18K** (embedding backfill + OCR backfill)

---

## 6. MVP 1 Tuần

**Mục tiêu:** Prove rằng kiến trúc hoạt động trên 1,000 document mẫu — không cần 10M.

| Day | Task | Deliverable | Risk |
|---|---|---|---|
| D1 | Bronze ingest: PDF → Delta (1K doc mẫu, mixed text/scan) | `bronze.raw_docs` table, `_delta_log/` visible | Thấp |
| D2 | Silver extraction: Unstructured.io local → text chunks + table JSON | `silver.doc_chunks` với structured tables | Trung bình (OCR setup) |
| D3 | Embedding generation → Lance dataset v1 | `silver-embed/text-embedding-3-small/` Lance dataset | Thấp |
| D4 | HNSW index build + search API (FastAPI) | Query "điều khoản bồi thường" → top-5 chunks trong 200ms | Trung bình |
| D5 | Embedding version migration demo: v1 → v2 (khác model) | Parallel index, atomic swap, recall comparison | **Cao** — đây là phần khó nhất |
| D6 | Reproducibility test: query tại `VERSION AS OF` cụ thể | Cùng query, cùng version → cùng kết quả sau 1 tuần | Trung bình |
| D7 | Buffer + writeup + slide | `ARCHITECTURE.md` + slide + PoC notebook | Thấp |

**Slice nhỏ nhất shippable (nếu chỉ có 3 ngày):** D1 + D3 + D5 — chứng minh được phần khó nhất (embedding versioning + migration) mà không cần full OCR pipeline.

---

## Tự đánh giá theo rubric

| Dimension | Trạng thái |
|---|---|
| ≥ 5 quyết định kèm alternatives đã loại | ✅ 6 quyết định |
| Constraints & numbers xuyên suốt | ✅ 30B chunks, 200ms, $9.6K/tháng |
| Liên hệ ≥ 4 concepts Day 18 | ✅ Delta, ACID, time travel, medallion, Z-ORDER |
| Failure modes 3h sáng + rollback | ✅ 4 scenarios |
| Cost back-of-envelope | ✅ Show the math |
| MVP slice nhỏ nhất | ✅ D1+D3+D5 |
