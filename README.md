# Multi-Platform Sport Social Listening & Dataset Collection Pipeline

A production-grade, automated, and resumable social listening pipeline for discovering and extracting social media posts, engagement metrics, verified timestamps, author metadata, and strict competition evidence into a standard **17-column dataset schema** across **Facebook**, **X (Twitter)**, **Instagram**, and **Threads**.

Built with **Python 3**, **Playwright**, **CloakBrowser** (stealth anti-detection browser engine), **twscrape** (GraphQL pool scraper), and **SQLite** on macOS Apple Silicon.

---

## Mục lục
1. [Tổng quan kiến trúc đa nền tảng](#1-tổng-quan-kiến-trúc-đa-nền-tảng)
2. [Lệnh chạy phiên 8 giờ cho từng nền tảng (8-Hour Run Commands)](#2-lệnh-chạy-phiên-8-giờ-cho-từng-nền-tảng-8-hour-run-commands)
   - [2.1. Facebook Pipeline (8 Giờ)](#21-facebook-pipeline-8-giờ)
   - [2.2. X / Twitter Pipeline (8 Giờ)](#22-x--twitter-pipeline-8-giờ)
   - [2.3. Discovery Engine (Tạo mục tiêu thu thập)](#23-discovery-engine-tạo-mục-tiêu-thu-thập)
3. [Định dạng dữ liệu chuẩn 17 cột & Ánh xạ Metric](#3-định-dạng-dữ-liệu-chuẩn-17-cột--ánh-xạ-metric)
4. [Cơ chế cổng kiểm định chất lượng (Research Quality Gate)](#4-cơ-chế-cổng-kiểm-định-chất-lượng-research-quality-gate)
5. [Toàn vẹn Metric & Cách ly dữ liệu (Metric & Demo Isolation)](#5-toàn-vẹn-metric--cách-ly-dữ-liệu-metric--demo-isolation)
6. [Cấu trúc thư mục & Artifacts](#6-cấu-trúc-thư-mục--artifacts)
7. [Thiết lập môi trường & Đăng nhập](#7-thiết-lập-môi-trường--đăng-nhập)
8. [Bảng tham số dòng lệnh chi tiết (CLI Reference)](#8-bảng-tham-số-dòng-lệnh-chi-tiết-cli-reference)
9. [Kiểm thử tự động (Test Suite 66/66 Passed)](#9-kiểm-thử-tự-động-test-suite-6666-passed)

---

## 1. Tổng quan kiến trúc đa nền tảng

Hệ thống phục vụ nghiên cứu Social Listening thể thao (trọng tâm: **Basketball / NBA**, mở rộng: **Football, F1, Athletics**) với kiến trúc Adapter Pattern độc lập:

```text
               ┌────────────────────────────────────────────────────────┐
               │           Unified Taxonomy & Keyword Engine            │
               │   (Basketball/NBA, Football/EPL, F1, WorldAthletics)   │
               └───────────────────────────┬────────────────────────────┘
                                           │
         ┌─────────────────────────────────┼─────────────────────────────────┐
         │                                 │                                 │
         ▼                                 ▼                                 ▼
┌──────────────────┐             ┌──────────────────┐             ┌──────────────────┐
│ Facebook Adapter │             │    X Adapter     │             │ Instagram / Th.  │
│  (CloakBrowser)  │             │   (`twscrape`)   │             │   (Adapters)     │
└────────┬─────────┘             └────────┬─────────┘             └────────┬─────────┘
         │                                │                                │
         └────────────────────────────────┼────────────────────────────────┘
                                          │
                                          ▼
                       ┌──────────────────────────────────────┐
                       │   SQLite Persistent Queue & State    │
                       │ (`pipeline_tasks` / `pipeline.db`)   │
                       └──────────────────┬───────────────────┘
                                          │
                                          ▼
                       ┌──────────────────────────────────────┐
                       │ Research Quality Gate & Scope Verif. │
                       │    (is_accepted = 1 vs review)       │
                       └──────────────────┬───────────────────┘
                                          │
                                          ▼
                       ┌──────────────────────────────────────┐
                       │       Standardized 17-Column         │
                       │     (CSV, JSON, JSONL Exports)       │
                       └──────────────────────────────────────┘
```

### Các nền tảng cốt lõi:
1. **Facebook (`facebook_pipeline.py`):**
   - Thu thập qua **CloakBrowser stealth session** (profile lưu tại `profiles/facebook`).
   - Điều phối 2 tab độc lập: Tab Discovery cuộn tìm URL và giữ nguyên vị trí trên Fanpage; Tab Extraction mở cào chi tiết caption, tương tác, timestamp.
   - Cơ chế cào theo lô (`--batch-size 20`), tỷ lệ trích xuất tương tác đạt 100% yield (0% missing like_count).
2. **X / Twitter (`x_pipeline.py`):**
   - Tích hợp **`twscrape`** khai thác GraphQL endpoint với Account Pool xoay vòng.
   - Hỗ trợ tìm kiếm theo từ khóa, hashtag và mention graph snowballing.
   - Cổng kiểm soát chất lượng: Phát hiện và đưa các tweet dùng từ khóa mơ hồ (ví dụ: *"Warriors"* nói về phim ảnh hoặc hiệp sĩ thời trung cổ mà thiếu ngữ cảnh bóng rổ) vào diện rà soát (`needs_review`, `is_accepted = 0`).
   - Tách biệt tuyệt đối giữa view tweet (`viewCount`), video view (`video_views`) và bài trích dẫn (quote tweets).
   - Bảo vệ timeout (`asyncio.wait_for`) chống treo vô hạn khi hết quota tài khoản.
3. **Khám phá tự động (Autonomous Discovery):**
   - Bóc tách liên kết mention chuẩn nền tảng (ưu tiên `<a href>` thực tế trong DOM Facebook và entity `mentionedUsers` trên X).
   - Giới hạn độ sâu phân nhánh (fan-out limit 3 profile/bài) chống trôi dạt chủ đề.

---

## 2. Lệnh chạy phiên 8 giờ cho từng nền tảng (8-Hour Run Commands)

> [!IMPORTANT]
> **Hướng dẫn vận hành phiên dài:**
> - Luôn chạy kèm tiền tố `caffeinate -i` để ngăn hệ điều hành macOS tự chuyển sang chế độ ngủ (sleep) khi đang chạy.
> - Cắm sạc MacBook và để mở nắp máy.
> - Nếu mạng gián đoạn hoặc bạn chủ động dừng bằng `Ctrl+C`, mọi task đã hoàn thành đều được commit an toàn vào SQLite. Bạn có thể chạy lại cùng một lệnh bất kỳ lúc nào để **tiếp tục (resume)** mà không cào lại bài trùng.

### 2.1. Facebook Pipeline (8 Giờ)

Thu thập bài viết từ danh sách 58 Fanpage NBA chính thức và uy tín (`sources/nba_pages_active.txt`):

```bash
caffeinate -i python3 -u facebook_pipeline.py \
  --pages sources/nba_pages_active.txt \
  --sport-hint Basketball \
  --filter-sport Basketball \
  --competition NBA \
  --batch-size 20 \
  --max-hours 8 \
  --max-refreshes 0 \
  --output-dir output/facebook_run
```

#### Giải thích tham số Facebook:
* `caffeinate -i`: Giữ macOS thức trong suốt phiên 8 tiếng.
* `--pages sources/nba_pages_active.txt`: File danh sách Fanpage NBA đã được rà soát và loại bỏ các nguồn chết/đổi URL.
* `--sport-hint Basketball` & `--competition NBA`: Kích hoạt bộ phân loại và đối chiếu bằng chứng NBA.
* `--batch-size 20`: Discovery 20 bài mới rồi trích xuất ngay, giữ vị trí cuộn trên Fanpage.
* `--max-hours 8`: Đặt trần ngân sách thời gian 8 giờ. Hết thời gian, pipeline dừng thu thập mới, dành thời gian lưu state và xuất file an toàn.
* `--output-dir output/facebook_run`: Thư mục chứa cơ sở dữ liệu SQLite, file CSV, JSON, JSONL và báo cáo kiểm định.

---

### 2.2. X / Twitter Pipeline (8 Giờ)

Thu thập tweet NBA thời gian thực qua GraphQL API với vòng lặp polling liên tục:

```bash
caffeinate -i python3 -u x_pipeline.py \
  --sport Basketball \
  --competition NBA \
  --queries "#NBA,NBA lang:en,Warriors NBA,Lakers NBA,Celtics NBA" \
  --limit-per-query 50 \
  --max-hours 8 \
  --poll-interval-sec 180 \
  --search-timeout 45.0 \
  --output-dir output/x_run
```

#### Quản lý Account Pool (X Burner Accounts):
Nếu sử dụng tài khoản đăng nhập để tăng hạn ngạch truy vấn:
```bash
# Nạp danh sách tài khoản từ file (định dạng: username:password:email:email_password)
python3 x_pipeline.py --accounts-file profiles/x_accounts.txt

# Kiểm tra trạng thái pool tài khoản
python3 x_pipeline.py --list-accounts
```

#### Chế độ Demo cách ly an toàn (Quick Verification):
Dùng để kiểm tra nhanh mapping 17 cột và cấu trúc SQLite mà không tốn quota:
```bash
python3 x_pipeline.py --demo --limit-per-query 5
# Toàn bộ dữ liệu demo được cách ly hoàn toàn tại: output/x_demo/
```

---

### 2.3. Discovery Engine (Tạo mục tiêu thu thập)

Sử dụng Discovery Engine CLI để tự động sinh danh sách URL tìm kiếm và hashtag cho từng môn thể thao:

```bash
# Hiển thị URL tìm kiếm NBA cho tất cả nền tảng
python3 -m discovery_engine.cli targets --sport Basketball --competition NBA

# Hiển thị URL tìm kiếm cho giải Ngoại hạng Anh (Football EPL)
python3 -m discovery_engine.cli targets --sport Football --competition EPL

# Xuất danh sách URL mục tiêu ra file
python3 -m discovery_engine.cli targets --sport Basketball --competition NBA --output targets_nba.txt
```

---

## 3. Định dạng dữ liệu chuẩn 17 cột & Ánh xạ Metric

File xuất tuân thủ chính xác hợp đồng 17 cột theo thứ tự bắt buộc:

```text
post_id, platform, text, published_at, collected_at, author_id,
language, hashtags, mentions, like_count, comment_count, share_count,
view_count, media_type, author_followers, post_url, sport_category
```

### Bảng ánh xạ ý nghĩa Metric giữa các nền tảng:

| Cột (Column) | Ý nghĩa trên Facebook | Ý nghĩa trên X (Twitter) | Quy ước kỹ thuật |
|---|---|---|---|
| `like_count` | Tổng số reactions (Like, Love, Haha, Wow, Sad, Angry) | Lượt Favorites / Likes (`likeCount`) | Số nguyên $\ge 0$ |
| `comment_count` | Tổng số bình luận (`commentsCount`) | Lượt trả lời (`replyCount`) | Số nguyên $\ge 0$ |
| `share_count` | Tổng số lượt chia sẻ (`sharesCount`) | Lượt Retweet / Repost (`retweetCount`) | Số nguyên $\ge 0$ (Không gồm Quote) |
| `view_count` | `null` (ô trống) cho bài chữ/ảnh; video views nếu có | Tweet Impressions (`viewCount`) | `int \| null` (Không gán 0 giả định) |
| `author_followers` | Số người theo dõi Trang (`Followers`) | Số người theo dõi tài khoản (`followersCount`) | `null` nếu là placeholder user |
| `post_id` | Định danh bài viết Facebook | ID tweet trên X (dạng chuỗi) | Giữ nguyên chuỗi, không ép kiểu số |
| `hashtags` | Mảng JSON: `["NBA", "Lakers"]` | Mảng JSON: `["NBA", "Lakers"]` | Chuỗi JSON hợp lệ |
| `mentions` | Mảng JSON: `["@KingJames"]` | Mảng JSON: `["@KingJames"]` | Chuỗi JSON hợp lệ |
| `sport_category` | `"Basketball"` | `"Basketball"` | Tên môn thể thao chuẩn hóa |

> [!NOTE]
> Toàn bộ thông tin chi tiết về Quote Tweets (`quoted_tweet`), Video Views (`video_views`), trạng thái tài khoản (`is_placeholder_user`), và phân loại bài (`source_type`) được lưu giữ đầy đủ trong trường `tech_meta_json` của SQLite để phục vụ nghiên cứu sâu.

---

## 4. Cơ chế cổng kiểm định chất lượng (Research Quality Gate)

Không phải mọi bài viết lấy về đều được coi là bài hợp lệ (`is_accepted = 1`). Hệ thống áp dụng 4 tầng kiểm định:

1. **Gate 1 - Tính toàn vẹn cấu trúc:** Bắt buộc có `post_id`, `post_url` hợp lệ, `published_at` đúng chuẩn ISO và không vượt quá thời gian thực tế.
2. **Gate 2 - Caption văn bản:** Không chấp nhận bài rỗng caption. Trên Facebook, các bài video/reel có DOM phụ đề rác được đánh dấu `video_skipped` và không đưa vào dataset nghiên cứu.
3. **Gate 3 - Phân loại Scope & Xử lý từ khóa mơ hồ:**
   * Từ khóa đơn nghĩa rộng (*Warriors, Kings, Magic, Thunder, Spurs, Nets, Heat, Suns...*) nếu xuất hiện đơn lẻ trong bài viết đời sống (ví dụ: *"Ancient warriors in history"*, *"The summer heat is intense"*) mà **không có tín hiệu bóng rổ bổ trợ** sẽ bị gán `is_accepted = 0`, chuyển sang nhóm `status = 'needs_review'`.
   * Ghi rõ lý do kiểm duyệt trong `review_reasons`.
4. **Gate 4 - Bảo toàn dữ liệu thô:** Bản ghi chưa đạt chuẩn (`is_accepted = 0`) vẫn được lưu trữ nguyên vẹn trong SQLite `pipeline_tasks` kèm `tech_meta_json` phục vụ đối chiếu và hậu kiểm.

---

## 5. Toàn vẹn Metric & Cách ly dữ liệu (Metric & Demo Isolation)

* **Bảo toàn `view_count: int | None`:** Bỏ nhận định "view_count chính xác 100%". Hệ thống lưu đúng số được trả về, giữ nguyên `null` khi thiếu, tuyệt đối không lấy video view đè vào tweet view.
* **Tách riêng Quote Tweet:** Bài trích dẫn bài khác giữ nguyên metric của bài hiện tại, không gộp chỉ số của bài được trích dẫn.
* **Xử lý Placeholder User:** Nhận diện đối tượng người dùng giữ chỗ trong `twscrape` (khi tài khoản bị khóa/xóa) và gán `author_followers = None` thay vì số 0 giả định.
* **Cách ly dữ liệu Demo:**
  * Lệnh `--demo` hoạt động trên database riêng `output/x_demo/demo_pipeline.db`.
  * Bộ lọc Export nghiên cứu tự động loại bỏ bản ghi có cờ `is_demo=True`, đảm bảo không làm bẩn counter và khóa chống trùng của dữ liệu thật.

---

## 6. Cấu trúc thư mục & Artifacts

```text
playwright-demo/
├── facebook_pipeline.py            # Runner chính của Facebook Pipeline
├── x_pipeline.py                   # Runner chính của X (Twitter) Pipeline
├── facebook_dataset.py             # Schema 17 cột & Logic trích xuất DOM Playwright
├── discovery_engine/               # Động cơ khám phá đa nền tảng
│   ├── engine.py                   # Bộ điều phối DiscoveryEngine
│   ├── taxonomy.py                 # Phân loại thể thao, từ khóa, hashtags
│   ├── models.py                   # Data models (DiscoveredPost, TargetSpec)
│   └── adapters/                   # Adapters chuyên biệt
│       ├── facebook_adapter.py     # Facebook Discovery & DOM Mention Handler
│       ├── x_adapter.py            # X Discovery & Mapping 17 cột
│       ├── instagram_adapter.py    # Instagram Discovery
│       └── threads_adapter.py      # Threads Discovery
├── sources/
│   ├── nba_pages_active.txt        # 58 Fanpage NBA hoạt động tốt đã qua kiểm toán
│   └── nba_pages.txt               # Danh sách gốc
├── profiles/
│   ├── facebook/                   # Persistent browser profile của Facebook
│   └── x_accounts.db               # SQLite account pool của twscrape
├── tests/                          # Bộ kiểm thử 66 test cases
├── output/
│   ├── facebook_run/               # Dataset & artifacts của Facebook
│   ├── x_run/                      # Dataset & artifacts của X
│   └── x_demo/                     # Workspace cách ly của Demo
└── README.md                       # Tài liệu hướng dẫn sử dụng
```

### Các file đầu ra chuẩn trong mỗi thư mục output:
* `*_dataset.csv`: Dataset chuẩn 17 cột (UTF-8-SIG), chỉ chứa bài được chấp nhận (`is_accepted = 1`).
* `*_dataset.json`: Dữ liệu dạng JSON array.
* `*_dataset.jsonl`: Dữ liệu dạng JSON Lines (tiện nạp vào pandas / BigQuery).
* `quality_report.json`: Báo cáo đối soát chất lượng, tỷ lệ phủ 17 cột, danh sách bài `needs_review` và lý do.
* `run_summary.json`: Thống kê tổng quan phiên chạy.

---

## 7. Thiết lập môi trường & Đăng nhập

### 7.1. Kích hoạt môi trường ảo:
```bash
source .venv/bin/activate
```

### 7.2. Cài đặt phụ thuộc:
```bash
pip install playwright twscrape pytest
playwright install chromium
```

### 7.3. Đăng nhập Facebook (Khi phiên hết hạn):
Khi gặp cảnh báo checkpoint hoặc màn hình đăng nhập:
```bash
python3 cloak_facebook_login.py
```
Hoàn tất đăng nhập trên cửa sổ trình duyệt hiện ra, sau đó quay lại Terminal nhấn `Enter` để lưu cookie và session.

---

## 8. Bảng tham số dòng lệnh chi tiết (CLI Reference)

### 8.1. `facebook_pipeline.py`

| Tham số | Kiểu | Mặc định | Ý nghĩa |
|---|---|---|---|
| `--pages <file>` | Path | `None` | File danh sách URL Fanpage nguồn (mỗi dòng 1 URL). |
| `--competition` | String | `NBA` | Giải đấu mục tiêu cần đối chiếu bằng chứng. |
| `--sport-hint` | String | `Basketball` | Môn thể thao gợi ý. |
| `--batch-size` | Integer | `20` | Số bài mới phát hiện trước khi chuyển sang trích xuất. |
| `--max-hours` | Float | `1.0` | Giới hạn thời gian chạy (8 cho phiên 8 tiếng). |
| `--source-max-scrolls` | Integer | `100` | Số vòng cuộn tối đa cho mỗi nguồn. |
| `--idle-rounds` | Integer | `10` | Số vòng cuộn không thấy bài mới trước khi đổi nguồn. |
| `--max-refreshes` | Integer | `0` | Số lần reload trang khi đứng bài (0: tắt reload). |
| `--target-valid` | Integer | `None` | Số bài hợp lệ mục tiêu để dừng phiên sớm. |
| `--headless` | Flag | `False` | Chạy trình duyệt không giao diện. |
| `--output-dir` | Path | `output` | Thư mục lưu trữ kết quả. |

### 8.2. `x_pipeline.py`

| Tham số | Kiểu | Mặc định | Ý nghĩa |
|---|---|---|---|
| `--sport` | String | `Basketball` | Môn thể thao mục tiêu. |
| `--competition` | String | `NBA` | Giải đấu mục tiêu. |
| `--queries` | String | `None` | Danh sách truy vấn tìm kiếm cách nhau bởi dấu phẩy. |
| `--limit-per-query` | Integer | `20` | Số lượng tweet tối đa cho mỗi câu truy vấn. |
| `--max-total-posts` | Integer | `None` | Trần số lượng bài viết toàn phiên thu thập. |
| `--max-hours` | Float | `None` | Thời gian chạy tối đa (ví dụ: `8` cho phiên 8 giờ). |
| `--poll-interval-sec` | Integer | `120` | Khoảng nghỉ (giây) giữa các vòng quét trong phiên dài. |
| `--search-timeout` | Float | `45.0` | Timeout (giây) mỗi truy vấn để tránh treo khi nghẽn quota. |
| `--accounts-file` | Path | `None` | File nạp danh sách tài khoản X burner. |
| `--list-accounts` | Flag | `False` | In danh sách và trạng thái account pool. |
| `--demo` | Flag | `False` | Chạy chế độ demo mẫu cách ly tại `output/x_demo/`. |
| `--output-dir` | Path | `output/x_run` | Thư mục lưu kết quả. |

---

## 9. Kiểm thử tự động (Test Suite 66/66 Passed)

Toàn bộ hệ thống được bảo vệ bởi **66 ca kiểm thử tự động (Unit & Integration Tests)**, chạy hoàn toàn offline không gọi mạng:

```bash
PYTHONPATH=. .venv/bin/pytest -v
```

### Phân bố kiểm thử:
* `tests/test_x_pipeline.py` (9 tests): Kiểm thử ánh xạ X sang 17 cột, cách ly metric view/quote, user placeholder, bộ lọc scope gate (Warriors không ngữ cảnh), cách ly dữ liệu demo, và gộp nguồn truy vấn (multi-query deduplication).
* `tests/test_audit_improvements.py` (17 tests): Kiểm thử retry tương tác, xử lý video DOM, trích xuất số liệu đa ngôn ngữ, và đồng bộ CSV/JSON/JSONL.
* `tests/test_discovery_engine.py` (9 tests): Kiểm thử Taxonomy 4 môn thể thao, keyword classifier, bộ điều phối engine và các adapters.
* `tests/test_local_units.py` (16 tests): Kiểm thử các hàm đơn vị trích xuất ID, chuẩn hóa thời gian ISO, tách biệt caption và comment.
* `tests/test_nba_pipeline.py` (15 tests): Kiểm thử bộ lọc NBA, vòng điều phối 2 tab, monotonic deadline, và khôi phục task sau crash.
