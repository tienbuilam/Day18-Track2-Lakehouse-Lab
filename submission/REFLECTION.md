# Reflection — Day 18 Lakehouse Lab

## Anti-pattern nguy hiểm nhất: Small-File Problem

Anti-pattern mà team tôi có nguy cơ mắc phải nhất là **small-file problem** — tình trạng có hàng trăm hoặc hàng nghìn file nhỏ tích lũy trong Delta table do streaming ingestion hoặc append liên tục với batch size nhỏ.

Lý do dễ rơi vào bẫy này: trong các pipeline thực tế, data thường được đẩy vào theo từng batch nhỏ từ nhiều nguồn (IoT sensors, user events, log streams). Nếu không có compaction định kỳ, mỗi micro-batch tạo ra một file Parquet riêng. Sau vài ngày, một table có thể có hàng nghìn file nhỏ 1–10 KB thay vì vài chục file lớn tối ưu.

Trong lab này, NB2 đã minh chứng rõ: 200 batch nhỏ tạo ra 200 file, khiến query chậm **22×** so với sau khi chạy `OPTIMIZE + ZORDER`. Với production data ở scale 10× hoặc 100×, sự suy giảm hiệu năng này trở nên nghiêm trọng hơn nhiều và khó phát hiện vì diễn ra từ từ.

**Biện pháp phòng ngừa:** lên lịch `OPTIMIZE` chạy định kỳ (sau mỗi lần ingest lớn hoặc hàng đêm), kết hợp `VACUUM` để dọn file lỗi thời, và theo dõi `numFiles` qua `DESCRIBE DETAIL` như một health metric.
