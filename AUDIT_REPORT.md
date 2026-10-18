# Báo Cáo Rà Soát & Kiểm Tra Toàn Diện Pipeline Dữ Liệu NBA Facebook

**Mã phiên rà soát (Audit Run ID):** `audit_run_20261017T190903Z`  
**Thời điểm thực hiện:** 2026-10-18T02:09:03Z (Giờ Việt Nam: UTC+7)  
**Môi trường:** macOS ARM, Python 3.13.5, CloakBrowser, Playwright  
**Cơ sở dữ liệu:** `output/nba_60min/facebook_pipeline.sqlite`  
**Tập dữ liệu sạch xuất bản:** `output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.csv` (17 cột chuẩn)

---

## 1. Truy Tìm Nguồn Gốc và Định Nghĩa Các Con Số Thực Tế

### 1.1. Con số 2.963 ở output
- **Hiện tượng:** Người dùng nhìn thấy số 2.963 hiển thị tại dòng `Total Unique Accepted NBA Posts in DB` trong báo cáo terminal của lần chạy trước.
- **Nguồn gốc được chứng minh:** Đây là một lỗi toán tử ưu tiên SQL (operator precedence bug) trong hàm `PipelineDB.get_statistics()` tại file `facebook_pipeline.py` (dòng 494-499 cũ):
  ```python
  where_clause = "WHERE dataset_scope = ? OR dataset_scope IS NULL" if dataset_scope else ""
  # ...
  accepted_cnt = self.conn.execute(
      f"SELECT COUNT(DISTINCT post_id) FROM pipeline_tasks {where_clause} {'AND' if where_clause else 'WHERE'} is_accepted = 1",
      params
  ).fetchone()[0]
  ```
  Khi chuỗi ghép lại trở thành:
  ```sql
  SELECT COUNT(DISTINCT post_id) FROM pipeline_tasks 
  WHERE dataset_scope = 'facebook:basketball:nba' OR dataset_scope IS NULL AND is_accepted = 1
  ```
  Trong chuẩn SQL, toán tử `AND` có mức độ ưu tiên cao hơn `OR`. Do đó biểu thức được đánh giá thành:
  $$(dataset\_scope = \text{'facebook:basketball:nba'}) \quad\mathbf{OR}\quad (dataset\_scope \text{ IS NULL AND } is\_accepted = 1)$$
  Vì tất cả 2.963 bản ghi trong bảng `pipeline_tasks` đều có `dataset_scope = 'facebook:basketball:nba'`, vế đầu luôn luôn `TRUE` cho toàn bộ các dòng! Do đó truy vấn đã đếm toàn bộ số `post_id` của mọi trạng thái trong database (gồm cả failed, video_skipped, unmatched, uncertain, missing) thay vì chỉ đếm các bài được chấp nhận (`is_accepted = 1`).
- **Số lượng thực tế đúng:** Trước khi rà soát, cơ sở dữ liệu chỉ có **807 bài viết thực sự đạt tiêu chuẩn chấp nhận** (`is_accepted = 1`), hoàn toàn không phải 2.963 bài.
- **Đã khắc phục:** Bọc ngoặc đơn tường minh `WHERE (dataset_scope = ? OR dataset_scope IS NULL) AND is_accepted = 1`. Kết quả sau khi sửa: đếm chính xác số bài accepted.

---

### 1.2. Báo cáo 2.159 tác vụ của đợt chạy gần nhất (`run_20261017T061355_863975Z`)
Các con số trong báo cáo của người dùng:
```text
Attempted=2159
Valid NBA New=438
Video Skipped=19
Unmatched Sport=1602
Uncertain NBA=32
Missing Required=43
Unavailable=0
Duplicates=0
Failed=25
```
Đối soát bất biến tổng số:
$$438 + 19 + 1602 + 32 + 43 + 0 + 0 + 25 = 2.159 \quad (\text{Khớp 100\%})$$

**Nguồn gốc 2.159 tác vụ:**
Đây là toàn bộ các URL được phát hiện và xử lý trong phiên chạy `run_20261017T061355_863975Z`. Khi cộng với 804 tác vụ của 4 phiên cào trước đó (6 + 6 + 265 + 527 = 804), tổng số bản ghi tích lũy trong SQLite đúng bằng $2159 + 804 = 2.963$.

---

### 1.3. Nguyên nhân gốc của 1.602 bài Unmatched Sport
- **Bằng chứng từ database:** Trong 1.602 bài bị loại, có tới **1.592 bài** bị gán lý do `no_basketball_or_nba_evidence` mặc dù được thu thập trực tiếp từ các Fanpage chính thức của đội bóng NBA (`warriors`, `nba`) và các trang báo chí chuyên sâu về NBA (`HeatNation`, `8points9seconds`, `RaptorsNation`, `AirAlamo`, `DetroitBadBoys`).
- **Nguyên nhân cốt lõi:** Hàm `classify_nba_post` trước đây chạy theo cơ chế kiểm tra văn bản thuần túy (strict text only), không truyền và không sử dụng ngữ cảnh nguồn (`author_or_source`). Các bài viết ngắn đặc thù của đội bóng như:
  - *"T-minus seven days to regular season tip-off"* (`@warriors`)
  - *"Leaving Vegas with some winnings / IREN || Final Score"* (`@warriors`)
  - *"Half."* (`@warriors`)
  - *"Sharpening the shot"* (`@warriors`)
  - *"BP Dubs. Let's keep it going."* (`@warriors`)
  đều bị loại bỏ oan uổng vì không chứa trực tiếp chữ "NBA", "Lakers", hoặc tên đầy đủ của cầu thủ.

---

### 1.4. Nguyên nhân gốc của lỗi trích xuất `view_count`
- **Tái hiện lỗi:** Trong `DOM_EXTRACT_JS` và `SCOPED_EXTRACTION_JS` cũ có đoạn mã:
  ```javascript
  const m = cardText.match(/\b(\d[\d.,]*[KkMmB]?)\s+(?:views?|lượt xem)\b/i);
  ```
- **Hành vi sai:** Vì ký tự `\s` trong JavaScript regex bao gồm cả ký tự xuống dòng `\n`, biểu thức này đã bắt nhầm con số đứng trước các nút bấm điều hướng trên Facebook Comet DOM:
  - `"3\nView more comments"` $\rightarrow$ bắt nhầm số 3 thành view count!
  - `"28\nView 2 replies"` $\rightarrow$ bắt nhầm số 28 thành view count!
  - `"3\nView profile"` $\rightarrow$ bắt nhầm số 3 thành view count!
  - Caption *"My target is 1000 views this week"* $\rightarrow$ bắt nhầm 1000 thành view count!
  Điều này giải thích vì sao trong CSV cũ xuất hiện các giá trị view rất nhỏ (ví dụ 3, 28) hoàn toàn phi lý so với hàng chục nghìn lượt like.

---

### 1.5. Nguyên nhân gốc của nhãn ngôn ngữ `unknown`
- **Nguyên nhân:** Trước đây hệ thống dùng heuristic đếm từ tiếng Anh cố định (`COMMON_EN_WORDS`) với điều kiện cứng $\ge 2$ từ. Khi gặp câu tiếng Anh ngắn hoặc bài viết nhiều thuật ngữ thể thao không nằm trong bộ từ vựng cơ bản, hệ thống lập tức fallback về `unknown`.
- **Đã khắc phục:** Tích hợp bộ nhận diện ngôn ngữ thống kê offline chuẩn quốc tế `lingua-language-detector (2.2.0)` với singleton reuse, kết hợp calibration theo độ dài văn bản. Đồng thời thiết lập quy tắc rõ ràng: **ngôn ngữ không phải acceptance gate**, bài viết NBA không bị loại chỉ vì mang nhãn `unknown`.

---

## 2. Các Cải Thiện Đã Triển Khai và Kiểm Thử

### 2.1. Cải tiến mã nguồn trong `facebook_dataset.py`
1. **View Count Extractor có xác minh (`verified_only` vs `off`):**
   - Khởi tạo giá trị `null` cho mỗi bài đăng.
   - Loại trừ triệt để vùng caption, comment, replies, link preview, và các nút lệnh điều hướng (`^View\s+(?:\d+\s+|more\s+|previous\s+)?(?:comments?|replies?|profile)`).
   - Chỉ nhận số đo khi phần tử metric gắn liền với nhãn đo lường được xác nhận thuộc đối tượng bài viết.
   - Hỗ trợ chuẩn hóa dấu phân cách, locale, và ký tự khoảng trắng không ngắt dòng (NBSP `\u00a0`, `\u202f`).
   - Tuyệt đối không đoán số bằng `max(views, likes)`.
2. **Lingua Statistical Language Detector:**
   - Khởi tạo detector một lần duy nhất qua hàm `_get_lingua_detector()`.
   - Phân tầng ngưỡng: đoạn văn ngắn yêu cầu confidence $\ge 0.65$ và margin $\ge 0.20$; đoạn văn bình thường yêu cầu confidence $\ge 0.50$ và margin $\ge 0.10$.
   - Giữ nguyên văn bản gốc, chỉ chuẩn hóa bản sao phục vụ detection.
3. **Phân loại Basketball/NBA và Source Registry:**
   - Xây dựng `NBA_SOURCE_REGISTRY` chứa danh mục Fanpage NBA chính thức, 30 câu lạc bộ và các kênh truyền thông chuyên trách.
   - Triển khai policy `contextual` (mặc định) và `strict`.
   - Phân biệt giải sinh viên Philippines (UAAP) để không match nhầm *"UE Red Warriors"* thành NBA Warriors.
   - Hỗ trợ tên cầu thủ quốc tế có dấu (Dončić, Jokić, Porziņģis, Schröder, Valančiūnas).
   - Loại bỏ nhãn đa môn giả: bóng rổ `"shooting guard"`, `"sharpening the shot"`, `"running the floor"` không còn bị kích hoạt nhãn giả `Shooting` hay `Athletics`.
   - Phân biệt tuyển dụng hành chính (`uncertain`, review) với hàng hóa/vé/áo đấu NBA (`commercial_or_promo`, accepted).

### 2.2. Cải tiến mã nguồn trong `facebook_pipeline.py`
1. Sửa lỗi toán tử ưu tiên SQL trong `get_statistics()`.
2. Lưu vết đầy đủ lý do từ chối `acceptance_rejections` vào trường `review_reasons` của SQLite.
3. Bổ sung các cờ CLI chính thức: `--nba-policy {contextual,strict}`, `--view-policy {verified_only,off}`, `--reprocess-offline`, `--audit-dir`, `--run-id-filter`, `--no-db-update`.
4. Triển khai chức năng `reprocess_dataset_offline()`: thực hiện rà soát và làm sạch dữ liệu cũ 100% offline, không mở browser/network, xuất báo cáo đối soát chi tiết.

### 2.3. Kết quả kiểm thử tự động
Toàn bộ **47/47 unit & fixture tests** đều vượt qua xuất sắc (`pytest tests/`):
- `tests/test_audit_improvements.py`: 17 passed
- `tests/test_local_units.py`: 16 passed
- `tests/test_nba_pipeline.py`: 14 passed

---

## 3. Kết Quả Rà Soát Dữ Liệu Ngoại Tuyến (Offline Reprocess)

Chương trình đã trực tiếp thực thi rà soát lại toàn bộ cơ sở dữ liệu `output/nba_60min/facebook_pipeline.sqlite`.

### 3.1. Ma trận chuyển trạng thái trên toàn bộ 2.963 tác vụ (Database-Wide)

| Trạng thái trước rà soát | Trạng thái sau rà soát | Số lượng bài | Đánh giá & Phân loại hành động |
| :--- | :--- | ---: | :--- |
| **Unaccepted** (Unmatched Sport) | **Accepted** (Valid NBA Post) | **+568** | **PHỤC HỒI THÀNH CÔNG:** Các bài đăng từ Fanpage chính thức của Warriors, NBA và các báo NBA chuyên trách được cứu nhờ cơ chế `contextual policy` |
| **Accepted** (Valid NBA Post) | **Accepted** (Valid NBA Post) | **806** | **DUY TRÌ CHẤP NHẬN:** Các bài NBA chuẩn xác, đã làm sạch view lỗi thời và chuẩn hóa ngôn ngữ |
| **Accepted** (Valid NBA Post) | **Unmatched** (UAAP Collegiate) | **-1** | **LOẠI BỎ SAI SÓT CŨ:** Bài đăng giải sinh viên Philippines *"UP Fighting Maroons vs UE Red Warriors"* từ `OneSportsPHL` trước đây bị bắt nhầm thành NBA Warriors, nay đã được phân loại chuẩn xác và hạ giải |
| **Unaccepted** (Các loại) | **Unaccepted** (Excluded / Missing) | **1.525** | **GIỮ NGUYÊN LOẠI TRỪ:** Các bài thực sự ngoài phạm vi, bài thiếu timestamp hoặc bài thiếu huy hiệu reaction |
| **Video Skipped** | **Video Skipped** | **38** | **BẢO LƯU CHÍNH SÁCH:** Bài video/reel bỏ qua theo chính sách nghiên cứu |
| **Failed** (Lỗi mạng/Timeout cũ) | **Failed** | **25** | Chuyển hàng đợi live revisit có mục tiêu |
| **TỔNG CỘNG** | | **2.963** | **Kiểm tra bất biến tổng: ĐẠT (100% khớp)** |

---

### 3.2. Thay đổi số lượng bài được chấp nhận (Dataset Acceptance)
- **Số bài accepted trước rà soát:** **807 bài**
- **Số bài accepted sau rà soát:** **1.374 bài**
- **Tăng ròng:** **+567 bài hợp lệ** (phục hồi 568 bài, hạ giải 1 bài UAAP).
- **Tỷ lệ bài đạt chuẩn trên toàn bộ database:** Tăng từ **27,24%** lên **46,37%**.

---

### 3.3. Thay đổi về nhận diện ngôn ngữ
- **Số lượng `unknown` trước rà soát:** **251 bài**
- **Số lượng `unknown` sau rà soát:** **138 bài** (giảm 113 bài `unknown`, tương đương giảm **45,0%**).
- **Phân bổ mới:**
  - Tiếng Anh (`en`): Tăng từ 1.303 lên **1.409 bài** (+106 bài được định danh chính xác).
  - Tiếng Philippines / Tagalog (`tl`): Phát hiện mới **12 bài** từ các trang thể thao khu vực Đông Nam Á.
  - Tiếng Việt (`vi`): Giữ nguyên ổn định tuyệt đối ở mức **1.341 bài**.

---

### 3.4. Thay đổi về trích xuất lượt xem (view_count)
- **Số bài có view trước rà soát:** 781 bài (chứa nhiều số bắt nhầm từ nút "View more comments", "View replies").
- **Sau rà soát trong bản clean dataset:** **0 bài mang view giả** (100% view không có provenance được đặt thành `null` theo đúng chính sách bảo thủ).
- **Bảo toàn dữ liệu audit:** Toàn bộ 781 giá trị view cũ được lưu giữ an toàn trong metadata `tech_meta.legacy_view_count` với nhãn `legacy_unverified` để phục vụ đối soát, không bị xóa mất.

---

### 3.5. Đối soát riêng trên tập 2.159 tác vụ của đợt chạy gần nhất (`run_20261017T061355_863975Z`)

| Chỉ số đợt chạy | Trước rà soát | Sau rà soát | Biến động | Ghi chú giải trình |
| :--- | ---: | ---: | ---: | :--- |
| **Valid NBA New (Accepted)** | 438 | **723** | **+285** | Phục hồi thêm 285 bài đăng NBA hợp lệ từ các đội bóng chính thức |
| **Unmatched Sport** | 1.602 | **159** | **-1.443** | Giảm mạnh do không còn đánh đồng thiếu keyword với sai môn thể thao |
| **Uncertain NBA** | 32 | **7** | **-25** | Phần lớn được làm rõ nhờ nguồn đã xác minh |
| **Missing Required** | 43 | **1.226** | **+1.183** | Phân loại đúng bản chất: các bài từ nguồn NBA nhưng thiếu reaction badge hoặc thiếu timestamp |
| **Video Skipped** | 19 | **19** | 0 | Giữ nguyên theo chính sách không thu video |
| **Failed** | 25 | **25** | 0 | Giữ nguyên hàng đợi để truy cập lại có mục tiêu |
| **TỔNG LƯỢT THỬ** | **2.159** | **2.159** | **0** | **Bất biến tổng hoàn hảo** |

---

## 4. Kết Quả Truy Cập Lại Có Mục Tiêu (Targeted Live Revisit)

Chương trình đã khởi chạy phiên CloakBrowser để kiểm tra và xử lý hàng đợi hữu hạn các bài từng bị lỗi trích xuất hoặc ngắt mạng:
- **Đã kiểm tra trực tiếp:** Chạy thử nghiệm thành công 2 URL lỗi mạng cũ của Blazersedge; Playwright kết nối ổn định, DOM tải thành công trong 15.5 giây và trích xuất dữ liệu đầy đủ.
- **Tiến trình hàng đợi:** Hệ thống tiếp tục thực thi xử lý các URL lỗi mạng còn lại bằng lệnh `--retry-failed`. Cơ chế checkpoint tự động ghi nhận từng URL sau khi hoàn thành vào SQLite.

---

## 5. Danh Mục Đầu Ra và Đường Dẫn Tệp Tin

Toàn bộ kết quả đã được tạo lập và lưu trữ tại thư mục audit độc lập:

1. **Tập dữ liệu sạch hợp nhất (Clean Dataset 17 Cột):**
   - CSV: [`output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.csv`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.csv) (775 KB, đúng 17 cột, 1.374 bài unique, UTF-8-BOM)
   - JSON: [`output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.json`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.json) (1.26 MB)
   - JSONL: [`output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.jsonl`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/output/nba_60min/audit_run_20261017T190903Z/facebook_dataset_clean.jsonl) (1.15 MB)
2. **Bảng đối chiếu toàn bộ lịch sử (Audit Reconciliation):**
   - CSV: [`output/nba_60min/audit_run_20261017T190903Z/audit_reconciliation.csv`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/output/nba_60min/audit_run_20261017T190903Z/audit_reconciliation.csv) (1.46 MB, chi tiết 2.963 dòng: trạng thái cũ $\rightarrow$ mới, lý do thay đổi, căn cứ phân loại)
3. **Bản tóm tắt số liệu máy đọc được (Summary JSON):**
   - JSON: [`output/nba_60min/audit_run_20261017T190903Z/audit_summary.json`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/output/nba_60min/audit_run_20261017T190903Z/audit_summary.json)
4. **Tài liệu đặc tả toàn bộ quy tắc hệ thống:**
   - Markdown: [`PIPELINE_RULES.md`](file:///Users/dipedetran/Documents/GW/GW_ARL/SocialListening/playwright-demo/PIPELINE_RULES.md)
5. **Bản sao lưu an toàn trước can thiệp:**
   - SQLite Snapshot: `output/nba_60min/backups/facebook_pipeline_pre_audit_20261017T183958Z.sqlite`
   - Baseline Output Snapshot: `output/nba_60min/backups/baseline_before_audit/`
