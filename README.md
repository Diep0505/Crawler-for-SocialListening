# Facebook NBA Social Listening & Dataset Collection Pipeline

A production-grade, automated, and resumable social listening pipeline for discovering and extracting Facebook posts, engagement metrics, verified timestamps, author metadata, and strict NBA competition evidence into a standard 17-column dataset format.

Built with **Python**, **Playwright**, **CloakBrowser** (stealth anti-detection browser engine), and **SQLite** on macOS Apple Silicon.

---

## Mục lục
1. [Tổng quan chương trình](#1-tổng-quan-chương-trình)
2. [Cơ chế điều phối theo lô & Discovery liên tục](#2-cơ-chế-điều-phối-theo-lô--discovery-liên-tục)
3. [Tiêu chuẩn chất lượng dữ liệu & Bộ lọc NBA](#3-tiêu-chuẩn-chất-lượng-dữ-liệu--bộ-lọc-nba)
4. [Định dạng dữ liệu 17 cột](#4-định-dạng-dữ-liệu-17-cột)
5. [Cấu trúc thư mục & Artifacts](#5-cấu-trúc-thư-mục--artifacts)
6. [Thiết lập môi trường & Đăng nhập](#6-thiết-lập-môi-trường--đăng-nhập)
7. [Lệnh chạy thử nghiệm 60 phút (Pilot Benchmark)](#7-lệnh-chạy-thử-nghiệm-60-phút-pilot-benchmark)
8. [Lệnh tham khảo cho phiên dài 8 giờ](#8-lệnh-tham-khảo-cho-phiên-dài-8-giờ)
9. [Bảng tham số dòng lệnh (CLI Reference)](#9-bảng-tham-số-dòng-lệnh-cli-reference)
10. [Kiểm thử offline (Unit & Integration Tests)](#10-kiểm-thử-offline-unit--integration-tests)
11. [Xử lý sự cố & Phiên đăng nhập (Troubleshooting)](#11-xử-lý-sự-cố--phiên-đăng-nhập-troubleshooting)

---

## 1. Tổng quan chương trình

Chương trình phục vụ nghiên cứu Social Listening mảng **Sport → Basketball → NBA**, thu thập dữ liệu bài viết từ danh sách Facebook Fanpage chính thức và uy tín về NBA.

### Đặc điểm kiến trúc:
- **Luồng thu thập:** Khám phá URL (`Page discovery`) $\to$ lưu hàng đợi SQLite $\to$ mở từng URL bài viết để trích xuất chi tiết 17 cột.
- **Trình duyệt đơn phiên (Single Persistent Context):** Sử dụng 1 browser session duy nhất với profile lưu trữ tại `profiles/facebook` (seed: 12345), không mở song song nhiều browser gây xung đột session hoặc checkpoint.
- **Hai tab làm việc độc lập:**
  - `Discovery Tab`: Mở trang Fanpage, cuộn và phát hiện bài viết mới. Tab này được giữ mở và giữ nguyên vị trí cuộn khi chuyển sang trích xuất.
  - `Extraction Tab`: Mở từng URL bài viết để cào chi tiết caption, like, comment, share, timestamp và đóng/tái sử dụng tuần tự.
- **Chống trùng lặp & Ràng buộc Scope:** Mọi task được gắn `dataset_scope` (mặc định: `facebook:basketball:nba`). Dữ liệu bóng đá, điền kinh hoặc các nghiên cứu khác lưu trước đó trong SQLite hoàn toàn tách biệt, không bị trộn lẫn vào dataset NBA.
- **Xuất dữ liệu đồng bộ tuyệt đối:** Cả 3 định dạng `facebook_dataset.csv`, `facebook_dataset.json`, và `facebook_dataset.jsonl` được tạo ra từ **cùng một truy vấn** các bản ghi được chấp nhận (`is_accepted = 1`), ghi qua file tạm `.tmp` và thay thế nguyên tử (`atomic replace`), đảm bảo không có sự sai lệch giữa các file.

---

## 2. Cơ chế điều phối theo lô & Discovery liên tục

Trước đây, quá trình discovery thường phải quét xong toàn bộ Fanpage rồi mới bắt đầu trích xuất, hoặc nếu ngắt giữa chừng thì lần sau phải tải lại từ đầu Fanpage.

### Cải tiến vòng điều phối:
1. **Khám phá và trích xuất xen kẽ theo lô (`--batch-size 20`):**
   - Tab Discovery mở Fanpage nguồn **một lần duy nhất**.
   - Mỗi lượt cuộn, các link bài viết mới được chuẩn hóa (loại bỏ tracking `fbclid`, `ref`...) và ghi ngay lập tức vào SQLite.
   - Khi phát hiện đủ 20 bài mới (hoặc khi nguồn tạm dừng tiến triển), Discovery tạm nhường quyền điều khiển.
   - Tab Discovery **vẫn giữ nguyên vị trí cuộn trên trang**, không bị đóng và không gọi `goto()` lại từ đầu trang.
   - Tab Extraction tiến hành cào dữ liệu cho lô bài trong hàng đợi.
   - Sau khi hoàn thành lô, Tab Discovery tiếp tục cuộn xuống để tìm các bài cũ hơn.
2. **Kiểm soát hàng đợi tồn đọng (`--backlog-limit 150`):**
   - Tránh việc discovery chạy quá xa so với tốc độ trích xuất. Nếu hàng đợi còn hơn 150 task chưa xử lý, discovery sẽ tạm dừng để extraction giải phóng hàng đợi.
3. **Chuyển nguồn linh hoạt & Giới hạn vòng quét (`--source-max-scrolls 100`):**
   - Thay thế mức giới hạn cứng 60 vòng quét cũ bằng ngân sách có thể cấu hình.
   - Khi một nguồn không còn bài mới sau số vòng nghỉ (`--idle-rounds 10`), pipeline ghi nhận trạng thái và chuyển sang nguồn tiếp theo trong file `--pages`.
   - Nguồn bị dừng do hết thời gian hoặc người dùng ngắt bằng `Ctrl+C` vẫn giữ nguyên điều kiện tiếp tục cho lần chạy sau.

---

## 3. Tiêu chuẩn chất lượng dữ liệu & Bộ lọc NBA

### 3.1. Chính sách bài viết Video / Reel:
- **Không lấy caption video/reel:** Do đặc thù DOM player của Facebook chèn phụ đề tự động hoặc text rác, các bài viết có `media_type == "video"` được bỏ qua caption (`text = null`), không trích xuất hashtags/mentions/sport.
- **Tiêu chí chấp nhận:** Bài video được ghi nhận trạng thái kỹ thuật `video_skipped`, **không được chấp nhận** vào dataset bài nghiên cứu NBA (`is_accepted = 0`), và không tính vào mục tiêu số bài NBA hợp lệ.

### 3.2. Bộ lọc NBA dựa trên luật (Rule-Based Classifier):
Phân loại chính xác dựa trên bằng chứng nội dung, không gọi API ngoài, không suy diễn từ điểm xác suất giả định:
- **`sport_category`:** Gán `"Basketball"` nếu có bằng chứng bóng rổ.
- **`competition`:** Xác định giải đấu cụ thể (`"NBA"`).
- **Quy tắc bắt buộc đã kiểm chứng:**
  - `NBA Finals: Lakers vs Celtics` $\to$ Nhận diện bằng chứng NBA trực tiếp (`matched`).
  - Bài viết chỉ về `WNBA`, `EuroLeague`, `VBA`, `FIBA`, `NCAA` $\to$ Bị loại khỏi phạm vi NBA (`unmatched`), dù vẫn là bóng rổ.
  - Từ `NBA` nằm trong `WNBA` $\to$ Không bị nhận nhầm thành NBA (sử dụng regex `(?<![Ww])\bNBA\b`).
  - Câu *"I cooked curry for dinner"* $\to$ Không nhận bóng rổ/NBA vì từ `curry` đứng một mình; yêu cầu tên đầy đủ (*Stephen Curry*) hoặc ngữ cảnh bóng rổ đi kèm.
  - Bài viết chỉ gồm emoji hoặc quá ngắn $\to$ Đánh dấu `uncertain`.
  - Bài viết tuyển dụng, bán vé, quảng cáo khuyến mãi trên Fanpage NBA nhưng không có nội dung trận đấu $\to$ Đánh dấu `uncertain`.
  - Game `NBA 2K` $\to$ Loại bỏ (`unmatched`, lý do: nội dung thể thao điện tử/video game).
  - Giải phát triển `G League` $\to$ Mặc định loại khỏi phạm vi NBA chính (`unmatched`).

### 3.3. Định nghĩa bài viết hợp lệ được chấp nhận (`is_accepted = 1`):
Một bản ghi chỉ được đưa vào `facebook_dataset.csv`, `facebook_dataset.json`, và `facebook_dataset.jsonl` khi thỏa mãn đồng thời:
1. Có `post_id` chuỗi hợp lệ, không trùng với bài đã ghi nhận.
2. Là bài chữ/ảnh (`media_type != "video"`), có caption văn bản không rỗng.
3. Phân loại NBA đạt `matched`.
4. Có đầy đủ `post_url`, `published_at` (ISO 8601 có timezone offset), `collected_at`, `sport_category = "Basketball"`.
5. Có `like_count` hợp lệ (số nguyên $\ge 0$) đo lường tương tác phục vụ nghiên cứu.
6. Các trường không hiển thị như `view_count` được để `null` (ô trống trong CSV).

---

## 4. Định dạng dữ liệu 17 cột

File xuất tuân thủ chính xác 17 cột, thứ tự và định dạng:

```text
post_id, platform, text, published_at, collected_at, author_id,
language, hashtags, mentions, like_count, comment_count, share_count,
view_count, media_type, author_followers, post_url, sport_category
```

- **Mã hóa:** UTF-8-SIG (`utf-8-sig`) giúp hiển thị tốt trên Excel và các công cụ phân tích.
- **Giá trị null:** Ô trống trong CSV, `null` trong JSON/JSONL.
- **Định dạng list:** `hashtags` và `mentions` được serialize dạng JSON array string (ví dụ: `["@lakers"]` hoặc `[]`).
- **ID:** Giữ nguyên chuỗi, không ép kiểu sang số để tránh làm tròn mất ký tự.
- **Thời gian:** Chuẩn ISO 8601 kèm múi giờ (mặc định Asia/Ho_Chi_Minh: `+07:00`).

---

## 5. Cấu trúc thư mục & Artifacts

Sau khi chạy, thư mục đầu ra (ví dụ: `output/nba_60min/`) chứa các file:

```text
output/nba_60min/
├── facebook_pipeline.sqlite       # Cơ sở dữ liệu SQLite quản lý toàn bộ task & state
├── facebook_dataset.csv          # File CSV chuẩn 17 cột (chỉ chứa bài được chấp nhận)
├── facebook_dataset.json         # Danh sách JSON bài viết được chấp nhận
├── facebook_dataset.jsonl        # JSON Lines bài viết được chấp nhận
├── run_summary.json              # Báo cáo thống kê chi tiết riêng cho phiên chạy hiện tại
├── quality_report.json           # Tỷ lệ phủ 17 cột, phân bố NBA, thống kê từng nguồn, mẫu đối chiếu
├── progress.json                 # Cập nhật tiến độ thời gian thực (real-time progress)
├── pipeline.log                  # Log chi tiết quá trình vận hành
├── pipeline.lock                 # File khóa ngăn xung đột tiến trình chạy trùng
└── facebook_posts_auto_debug_*.json # Bản ghi kỹ thuật kiểm toán chi tiết (tự động giữ lại 1 file mới nhất, dọn file cũ)
```

---

## 6. Thiết lập môi trường & Đăng nhập

### 6.1. Kích hoạt môi trường ảo:
```bash
source .venv/bin/activate
```

### 6.2. Đăng nhập Facebook (Chỉ thực hiện một lần khi phiên hết hạn):
Khi gặp thông báo yêu cầu xác minh hoặc checkpoint, chạy script đăng nhập tương tác có giao diện:
```bash
python3 cloak_facebook_login.py
```
Đăng nhập tài khoản trên cửa sổ trình duyệt hiện ra, hoàn tất xác thực 2 bước (2FA nếu có). Sau khi vào trang chủ Facebook thành công, quay lại Terminal nhấn `Enter` để lưu profile tại `profiles/facebook`.

---

## 7. Lệnh chạy thử nghiệm 60 phút (Pilot Benchmark)

> [!IMPORTANT]
> **Ràng buộc:** Lệnh này do bạn **chủ động chạy** từ Terminal trên máy của bạn. Agent không tự kích hoạt lệnh này.

Mở Terminal tại thư mục project, giữ máy không ngủ và chạy:

```bash
caffeinate -i python3 -u facebook_pipeline.py \
  --pages sources/nba_pages.txt \
  --sport-hint Basketball \
  --filter-sport Basketball \
  --competition NBA \
  --batch-size 20 \
  --max-hours 1 \
  --max-refreshes 0 \
  --output-dir output/nba_60min
```

### Giải thích các tham số chính:
- `caffeinate -i`: Giữ hệ điều hành macOS không rơi vào chế độ sleep trong lúc tác vụ đang chạy (yêu cầu để mở nắp MacBook và cắm sạc).
- `--pages sources/nba_pages.txt`: Danh sách Fanpage nguồn (mặc định khởi tạo gồm `https://www.facebook.com/nba/`).
- `--sport-hint Basketball`: Gợi ý ngữ cảnh thể thao phục vụ phân loại.
- `--filter-sport Basketball`: Lọc bài viết môn bóng rổ khi xuất dữ liệu.
- `--competition NBA`: Kích hoạt bộ lọc bằng chứng giải đấu NBA.
- `--batch-size 20`: Discovery tối đa 20 URL mới rồi nhường quyền cho Extraction trích xuất dữ liệu, sau đó Discovery tiếp tục từ vị trí đã cuộn.
- `--max-hours 1`: Ngân sách thời gian tối đa 60 phút. Hết giờ, pipeline dừng thu thập mới, dành tối đa 20 giây lưu trạng thái và xuất file hoàn chỉnh.
- `--max-refreshes 0`: Tắt reload trang khi cạn bài (tránh quay lại đầu trang).
- `--output-dir output/nba_60min`: Toàn bộ database, dataset, log và báo cáo lưu độc lập tại đây.

---

## 8. Lệnh tham khảo cho phiên dài 8 giờ

> [!NOTE]
> Lệnh này chỉ dùng để tham khảo sau khi đã đánh giá kết quả từ phiên thử 60 phút:

```bash
caffeinate -i python3 -u facebook_pipeline.py \
  --pages sources/nba_pages.txt \
  --sport-hint Basketball \
  --filter-sport Basketball \
  --competition NBA \
  --batch-size 20 \
  --max-hours 8 \
  --max-refreshes 0 \
  --output-dir output/nba_60min
```

Chạy cùng `--output-dir output/nba_60min` sẽ tự động tận dụng danh sách đã cào, hàng đợi và cache của phiên 60 phút trước đó mà không cào lại bài trùng.

---

## 9. Bảng tham số dòng lệnh (CLI Reference)

| Tham số | Kiểu | Mặc định | Ý nghĩa |
|---|---|---|---|
| `--pages <file>` | File path | `None` | File chứa danh sách Fanpage Facebook (1 URL/dòng, hỗ trợ `#`). |
| `--page <url>` | URL | `None` | Thu thập từ 1 Fanpage cụ thể. |
| `--urls <file>` | File path | `None` | Thu thập từ danh sách URL bài viết có sẵn (.txt, .csv, .jsonl). |
| `--competition` | String | `NBA` | Phạm vi giải đấu mục tiêu cần khớp bằng chứng. |
| `--sport-hint` | String | `Basketball` | Gợi ý môn thể thao từ nguồn. |
| `--filter-sport` | String | `Basketball` | Bộ lọc môn thể thao áp dụng khi xuất dataset. |
| `--batch-size` | Integer | `20` | Số bài mới phát hiện trước khi chuyển sang trích xuất. |
| `--max-hours` | Float | `1.0` | Giới hạn thời gian chạy của phiên (tính bằng giờ). |
| `--source-max-scrolls` | Integer | `100` | Số vòng cuộn tối đa cho mỗi nguồn. |
| `--idle-rounds` | Integer | `10` | Số vòng cuộn không thấy bài mới trước khi chuyển nguồn. |
| `--max-refreshes` | Integer | `0` | Số lần reload trang khi cuộn đứng; 0 là tắt reload. |
| `--backlog-limit` | Integer | `150` | Giới hạn hàng đợi chờ; vượt ngưỡng này discovery tạm dừng. |
| `--target-valid` | Integer | `None` | Mục tiêu số bài NBA hợp lệ để dừng sớm (nếu muốn). |
| `--max-attempts` | Integer | `None` | Trần số URL thử xử lý trong phiên. |
| `--retry-failed` | Flag | `False` | Thử lại các bài từng bị lỗi mạng tạm thời (tối đa 3 lần). |
| `--no-followers` | Flag | `False` | Bỏ qua bước lấy số follower nếu muốn tối ưu tốc độ. |
| `--headless` | Flag | `False` | Chạy trình duyệt ở chế độ ẩn. |
| `--output-dir` | Directory | `output` | Thư mục lưu trữ database, bản xuất và báo cáo. |

---

## 10. Kiểm thử offline (Unit & Integration Tests)

Toàn bộ hệ thống được kiểm thử offline bằng **pytest**, sử dụng mock, fake page và HTML fixture cục bộ; không thực hiện cuộc gọi mạng hay mở trình duyệt thật ra internet.

Chạy kiểm thử:
```bash
python3 -m pytest tests/ -v
```

### Kết quả kiểm thử đạt 30/30 tests:
- `tests/test_local_units.py`: 16 tests về chuẩn hóa số liệu đa ngôn ngữ, timezone ISO 8601, tách biệt caption khỏi comment/sidebar, trích xuất mention và ID.
- `tests/test_nba_pipeline.py`: 14 tests bao quát 12 tiêu chí nghiệm thu:
  1. Trích xuất caption chữ/ảnh không lỗi biến `UnboundLocalError`.
  2. Bỏ qua caption bài video và loại khỏi dataset nghiên cứu.
  3. Phân lập vùng DOM bài viết, không lẫn gợi ý và bình luận.
  4. Bộ lọc NBA: phân biệt Lakers/Celtics, loại WNBA/VBA/EuroLeague, loại curry ăn tối, nhận Stephen Curry, loại game 2K/G-League.
  5. Chuẩn hóa URL, loại bỏ tracking tham số `fbclid`, chống trùng permalink.
  6. Ghi nhận URL discovery tức thì vào SQLite.
  7. Discovery session phân lô mà không reload trang từ đầu.
  8. Phục hồi task `processing` sau crash thành `pending`.
  9. Kiểm soát thời gian bằng monotonic deadline.
  10. Phát hiện checkpoint / màn hình đăng nhập để dừng phiên an toàn.
  11. Phân vùng scope SQLite, bảo vệ dữ liệu cũ không bị lẫn vào NBA dataset.
  12. Tính nhất quán tuyệt đối giữa CSV, JSON và JSONL; cache follower theo TTL.

---

## 11. Xử lý sự cố & Phiên đăng nhập (Troubleshooting)

- **Khi gặp checkpoint hoặc bắt đăng nhập:** Pipeline tự động lưu tiến độ vào SQLite và thoát với mã cảnh báo rõ ràng. Hãy chạy `python3 cloak_facebook_login.py` để đăng nhập lại trên trình duyệt, sau đó tiếp tục chạy lệnh pipeline với cùng `--output-dir`.
- **Tạm dừng bằng `Ctrl+C`:** Toàn bộ task đã hoàn thành được commit ngay lập tức vào database, file CSV/JSON/JSONL được cập nhật đến thời điểm dừng.
- **Tiến trình chạy trùng lặp:** Hệ thống sử dụng file `pipeline.lock`. Nếu có tiến trình khác đang chạy cùng profile/output, chương trình sẽ từ chối chạy để bảo vệ dữ liệu.
