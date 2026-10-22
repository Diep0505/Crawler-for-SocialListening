# Multi-Platform Sport Social Listening & Dataset Collection Pipeline

A production-grade, automated, and resumable social listening pipeline for discovering and extracting social media posts, engagement metrics, verified timestamps, author metadata, and strict competition evidence into a standard **17-column dataset schema** across **Facebook**, **X (Twitter)**, **Instagram**, and **Threads**.

Built with **Python 3**, **Playwright**, **CloakBrowser** (stealth anti-detection browser engine), **twscrape** (GraphQL pool scraper), and **SQLite** on macOS Apple Silicon.

---

## Mục lục
1. [Tổng quan kiến trúc đa nền tảng](#1-tổng-quan-kiến-trúc-đa-nền-tảng)
2. [Lệnh chạy phiên 8 giờ cho từng nền tảng (8-Hour Run Commands)](#2-lệnh-chạy-phiên-8-giờ-cho-từng-nền-tảng-8-hour-run-commands)
   - [2.1. Facebook Pipeline (8 Giờ)](#21-facebook-pipeline-8-giờ)
     - [2.1.1. Luồng Fanpage/Permalink](#211-luồng-theo-danh-sách-fanpageurl-permalink-discovery-mode)
     - [2.1.2. Luồng Keyword (Độc lập chủ đề)](#212-luồng-theo-từ-khóa-facebook-keyword-discovery-mode--độc-lập-chủ-đề)
   - [2.2. X / Twitter Pipeline (8 Giờ)](#22-x--twitter-pipeline-8-giờ)
   - [2.3. Discovery Engine (Tạo mục tiêu thu thập)](#23-discovery-engine-tạo-mục-tiêu-thu-thập)
3. [Định dạng dữ liệu chuẩn 17 cột & Ánh xạ Metric](#3-định-dạng-dữ-liệu-chuẩn-17-cột--ánh-xạ-metric)
4. [Cơ chế cổng kiểm định chất lượng (Research Quality Gate)](#4-cơ-chế-cổng-kiểm-định-chất-lượng-research-quality-gate)
5. [Toàn vẹn Metric & Cách ly dữ liệu (Metric & Demo Isolation)](#5-toàn-vẹn-metric--cách-ly-dữ-liệu-metric--demo-isolation)
6. [Cấu trúc thư mục & Artifacts](#6-cấu-trúc-thư-mục--artifacts)
7. [Hướng dẫn chi tiết từ khi clone về máy khác (Fresh Clone Setup Guide)](#7-hướng-dẫn-chi-tiết-thiết-lập-từ-đầu-khi-clone-về-máy-khác-fresh-clone-setup-guide)
8. [Bảng tham số dòng lệnh chi tiết (CLI Reference)](#8-bảng-tham-số-dòng-lệnh-chi-tiết-cli-reference)
9. [Kiểm thử tự động (Test Suite 81/81 Passed)](#9-kiểm-thử-tự-động-test-suite-8181-passed)

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

#### 2.1.1. Luồng theo danh sách Fanpage/URL (Permalink Discovery Mode)

Thu thập bài viết từ danh sách 58 Fanpage NBA chính thức và uy tín (`sources/nba_pages_active.txt`):

```bash
caffeinate -i python3 -u facebook_pipeline.py \
  --mode permalink \
  --pages sources/nba_pages_active.txt \
  --sport-hint Basketball \
  --filter-sport Basketball \
  --competition NBA \
  --batch-size 20 \
  --max-hours 8 \
  --max-refreshes 0 \
  --output-dir output/facebook_run
```

#### 2.1.2. Luồng theo từ khóa (Facebook Keyword Discovery Mode — Độc lập chủ đề)

Thu thập bài viết công khai (Page, cá nhân, Group công khai) từ Facebook Post Search theo từ khóa hoặc cụm từ bất kỳ mà **không cần chuẩn bị URL đầu vào**. Hoàn toàn độc lập chủ đề: thể thao, xe điện, công nghệ, mỹ phẩm, du lịch...

```text
Từ khóa / Cụm từ ──► Facebook Post Search URL ──► Scoped Post Cards Scan ──► Trích xuất Permalink thật
──► SQLite Queue & Chống trùng nhiều query ──► Bộ trích xuất bài viết ──► Bộ lọc từ khóa & Cổng kiểm định
──► Xuất bộ dữ liệu chuẩn 17 cột (CSV, JSON, JSONL, Audit Debug)
```

##### Ví dụ 1: Chủ đề Xe điện & Công nghệ Pin (Automotive / Tech)
```bash
caffeinate -i python3 -u facebook_pipeline.py \
  --mode keyword \
  --queries "xe điện,pin LFP" \
  --collection-id xe-dien-public \
  --keyword-match any \
  --limit 100 \
  --limit-per-query 30 \
  --max-hours 0.25 \
  --poll-interval-sec 180 \
  --output-dir output/facebook_keywords_ev
```

##### Ví dụ 2: Chủ đề Mỹ phẩm & Chăm sóc da (Cosmetics / Skincare)
```bash
caffeinate -i python3 -u facebook_pipeline.py \
  --mode keyword \
  --queries "kem chống nắng,serum B5" \
  --exclude-keywords "hàng giả,kém chất lượng" \
  --collection-id skincare-public \
  --keyword-match any \
  --limit 100 \
  --limit-per-query 25 \
  --max-hours 0.5 \
  --poll-interval-sec 120 \
  --output-dir output/facebook_keywords_skincare
```

##### Chạy lại hoặc tiếp tục thu thập cùng Collection:
Nếu tiến trình bị gián đoạn hoặc bạn muốn bổ sung bài viết cho cùng một collection:
```bash
# Chỉ cần giữ nguyên --collection-id (hoặc danh sách --queries ban đầu)
caffeinate -i python3 -u facebook_pipeline.py \
  --mode keyword \
  --collection-id xe-dien-public \
  --queries "xe điện,pin LFP,VinFast VF8" \
  --limit 200 \
  --output-dir output/facebook_keywords_ev
```
Hệ thống tự động nhận diện các bài đã cào trước đó từ SQLite (`pipeline_tasks` và `collection_items`), tái sử dụng nội dung đã lưu mà không mở lại permalink, đồng thời bổ sung các truy vấn phát hiện mới vào trường provenance `discovery_queries`.

##### Bảng tham số chế độ Keyword:
| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--mode keyword` | `permalink` | Kích hoạt luồng thu thập bài viết theo Facebook Post Search. |
| `--queries "q1,q2"` | `None` | Danh sách truy vấn phân tách bằng dấu phẩy; tự động trim và loại trùng. |
| `--queries-file <file>` | `None` | File UTF-8 đọc truy vấn (hỗ trợ comment `#`, mỗi dòng một cụm từ có dấu phẩy). |
| `--collection-id <id>` | Tự động sinh `col_<slug>_<hash>` | Định danh collection ổn định để quản lý phạm vi và chạy nối tiếp. |
| `--keyword-match any\|all` | `any` | Khớp ít nhất một cụm từ (`any`) hoặc toàn bộ cụm từ (`all`) trong nội dung bài viết. |
| `--exclude-keywords "e1,e2"` | `None` | Loại bỏ bài viết nếu nội dung chứa bất kỳ cụm từ loại trừ nào. |
| `--strip-accents` | `False` | Tùy chọn bỏ dấu tiếng Việt khi so khớp (mặc định yêu cầu khớp đúng dấu). |
| `--limit` | `None` | Số lượng bài viết được chấp nhận tối đa để hoàn thành collection. |
| `--limit-per-query` | `30` | Số lượng ứng viên mới tối đa cho mỗi câu truy vấn trong một lượt discovery. |
| `--poll-interval-sec` | `180` | Khoảng nghỉ giữa các vòng quét tìm kiếm (đặt `0` để chỉ quét 1 vòng). |

##### Giới hạn kỹ thuật của Facebook Search:
1. **Thứ tự không theo thời gian:** Kết quả tìm kiếm Facebook được phân phối theo thuật toán liên quan và gợi ý. Gặp một bài cũ không có nghĩa toàn bộ bài phía sau đều cũ.
2. **Preview rút gọn:** Thẻ bài viết trên trang Search thường bị cắt ngắn (`... Xem thêm`). Pipeline không bao giờ loại bài ở khâu preview mà đưa permalink vào hàng đợi để lấy caption đầy đủ trước khi đánh giá từ khóa.
3. **Phạm vi công khai:** Chỉ thu thập bài viết công khai của Page, trang cá nhân và Group công khai có thể tiếp cận được.
4. **Ổn định phiên:** Tự động phát hiện khi gặp Login Wall hoặc Checkpoint để dừng an toàn và lưu tiến độ, không spam đổi query khi phiên hết hạn.

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

## 7. Hướng dẫn chi tiết thiết lập từ đầu khi clone về máy khác (Fresh Clone Setup Guide)

Dành cho người dùng mới, giảng viên hoặc kỹ sư triển khai dự án trên một máy tính hoàn toàn mới (hỗ trợ **macOS Apple Silicon/Intel**, **Linux Ubuntu 20.04+**, và **Windows WSL2**).

### 7.1. Yêu cầu tiên quyết (Prerequisites)
- **Hệ điều hành**:
  - Khuyến nghị tốt nhất: **macOS** (tương thích tối đa với `caffeinate` chống sleep và stealth browser context).
  - Hoặc: **Linux Ubuntu/Debian** hoặc **Windows WSL2 (Ubuntu)**.
- **Python**: Phiên bản **Python 3.10** đến **3.13** (khuyến nghị `3.11` hoặc `3.12/3.13`). Kiểm tra bằng:
  ```bash
  python3 --version
  ```
- **Git**: Đã cài đặt Git trên máy (`git --version`).

---

### 7.2. Các bước cài đặt từng bước (Step-by-step Setup)

#### Bước 1: Clone kho mã nguồn về máy và chuyển nhánh
```bash
# Clone repository từ GitHub
git clone https://github.com/Diep0505/Crawler-for-SocialListening.git

# Di chuyển vào thư mục dự án
cd Crawler-for-SocialListening

# Chuyển sang nhánh tính năng đa nền tảng và keyword mới nhất
git checkout multi-platform
```

#### Bước 2: Khởi tạo và kích hoạt môi trường ảo Python (Virtual Environment)
Luôn tạo môi trường ảo độc lập để không xung đột với các gói Python toàn cục của máy:
```bash
# 1. Tạo môi trường ảo .venv
python3 -m venv .venv

# 2. Kích hoạt môi trường ảo:
# Trên macOS hoặc Linux:
source .venv/bin/activate

# (Nếu dùng Windows PowerShell: .venv\Scripts\Activate.ps1)
# (Nếu dùng Windows Command Prompt: .venv\Scripts\activate.bat)

# 3. Nâng cấp pip lên bản mới nhất
pip install --upgrade pip
```

#### Bước 3: Cài đặt các gói phụ thuộc (Dependencies)
```bash
pip install -r requirements.txt
```
*Các gói thư viện chính bao gồm:*
* `playwright`: Công cụ tự động hóa trình duyệt headless/headful.
* `cloakbrowser`: Nhân trình duyệt ngụy trang chống nhận diện bot tự động của Facebook.
* `twscrape`: Thư viện khai thác GraphQL API của X (Twitter) qua Account Pool xoay vòng.
* `lingua-language-detector`: Bộ nhận diện ngôn ngữ thống kê chính xác cao.
* `aiosqlite`, `loguru`, `pytest`: Quản lý SQLite bất đồng bộ, logging chuẩn mực và bộ kiểm thử.

#### Bước 4: Cài đặt trình duyệt Playwright Chromium
```bash
# Tải binary trình duyệt Chromium được tối ưu cho Playwright
playwright install chromium
```
> [!NOTE]
> **Dành riêng cho người dùng Linux (Ubuntu/Debian / WSL2):**
> Cần chạy thêm lệnh sau một lần để cài đặt các thư viện đồ họa hệ thống (libgbm, libasound, nss, fontconfig...):
> ```bash
> playwright install-deps chromium
> ```

#### Bước 5: Chạy kiểm thử tự động xác minh môi trường (Self-Verification)
Trước khi chạy cào thực tế, hãy chạy bộ kiểm thử offline để chắc chắn 100% môi trường đã sẵn sàng:
```bash
PYTHONPATH=. .venv/bin/pytest -v
```
> [!TIP]
> **Kết quả mong đợi:** Toàn bộ **`81 passed in ~10-15s`**.
> Bộ kiểm thử này chạy **offline 100%** (sử dụng fixture mock DOM và SQLite tạm thời), không gọi mạng và không tốn bất kỳ quota nào. Nếu toàn bộ 81 tests đều PASS, môi trường máy mới của bạn đã hoàn toàn chuẩn xác.

---

### 7.3. Cấu hình phiên đăng nhập Facebook (`profiles/facebook`)

Facebook yêu cầu phiên duyệt web có cookie hợp lệ để hiển thị kết quả tìm kiếm và các bài viết công khai. Dự án sử dụng cơ chế **Persistent Browser Profile** của CloakBrowser lưu tại thư mục `profiles/facebook/`.

1. **Khởi chạy cửa sổ đăng nhập tương tác**:
   ```bash
   python3 cloak_facebook_login.py
   ```
2. Một cửa sổ trình duyệt Chromium sẽ tự động mở ra trang đăng nhập `https://www.facebook.com/`.
3. **Thực hiện đăng nhập** tài khoản Facebook của bạn bình thường (nhập Email/SĐT, mật khẩu, và mã xác thực 2 yếu tố 2FA nếu có).
4. Sau khi vào được giao diện chính của Facebook (bảng tin hoặc trang cá nhân), quay lại cửa sổ Terminal.
5. Nhấn phím **`Enter`** tại Terminal để chương trình tự động lưu cookie, session tokens và đóng trình duyệt an toàn.

> [!IMPORTANT]
> * **Chỉ cần làm một lần duy nhất:** Thư mục `profiles/facebook/` sẽ lưu giữ trạng thái đăng nhập. Các lần chạy cào tiếp theo (cả luồng Fanpage lẫn luồng Keyword) sẽ tự động nạp session mà không yêu cầu bạn đăng nhập lại.
> * **Khi phiên hết hạn hoặc gặp Checkpoint:** Nếu sau nhiều tuần/tháng tài khoản bị đăng xuất hoặc Facebook hiển thị yêu cầu xác minh bảo mật, pipeline sẽ dừng an toàn và thông báo. Bạn chỉ cần chạy lại `python3 cloak_facebook_login.py`, đăng nhập lại rồi tiếp tục chạy pipeline.

---

### 7.4. Cấu hình tài khoản X / Twitter (Nếu sử dụng luồng X)

1. **Chế độ Demo (Không cần tài khoản):**
   Chạy thử nghiệm kiểm tra định dạng 17 cột cách ly không tốn quota:
   ```bash
   python3 x_pipeline.py --demo --limit-per-query 5
   ```
2. **Chế độ cào chính thức (Cần tài khoản X Burner):**
   Tạo file `profiles/x_accounts.txt` chứa danh sách tài khoản theo định dạng (mỗi dòng một tài khoản):
   ```text
   username:password:email:email_password
   ```
   Sau đó nạp tài khoản vào pool:
   ```bash
   python3 x_pipeline.py --accounts-file profiles/x_accounts.txt
   python3 x_pipeline.py --list-accounts
   ```

---

### 7.5. Chạy thử nghiệm kiểm tra nhanh (Quick Smoke Test)

Sau khi hoàn tất đăng nhập, hãy chạy thử nghiệm ngắn (1-2 phút) để xác nhận bot hoạt động tốt với Facebook thật:

```bash
# Thử nghiệm luồng tìm kiếm theo từ khóa (giới hạn 5 bài):
python3 -u facebook_pipeline.py \
  --mode keyword \
  --queries "xe điện,pin LFP" \
  --collection-id test-setup-pilot \
  --limit 5 \
  --limit-per-query 5 \
  --max-hours 0.05 \
  --poll-interval-sec 0 \
  --output-dir output/facebook_test
```

**Kiểm tra kết quả sinh ra trong thư mục `output/facebook_test/`:**
* `facebook_dataset.csv`: File CSV chuẩn 17 cột (UTF-8-SIG), mở trực tiếp bằng Excel không lỗi font tiếng Việt.
* `facebook_dataset.json`: Dữ liệu dạng JSON array.
* `facebook_pipeline.sqlite`: Cơ sở dữ liệu SQLite lưu vết toàn bộ task, phục vụ resume.
* `run_summary.json`: Báo cáo tóm tắt phiên cào.

---

### 7.6. Xử lý các sự cố thường gặp trên máy mới (Troubleshooting & FAQs)

| Vấn đề gặp phải | Nguyên nhân | Cách khắc phục |
|---|---|---|
| `ModuleNotFoundError: No module named 'cloak_facebook_discover'` | Python không tìm thấy thư mục gốc dự án trong `sys.path`. | Luôn đứng tại thư mục gốc của repo (`Crawler-for-SocialListening`) hoặc thêm tiền tố `PYTHONPATH=.` vào trước lệnh chạy. |
| `Executable doesn't exist at .../chrome` | Chưa tải binary Chromium của Playwright về máy. | Chạy lệnh `playwright install chromium`. Trên Linux/WSL2, chạy thêm `playwright install-deps chromium`. |
| `Browser is already running` / `SingletonLock` | Phiên cào trước bị tắt đột ngột (crash hoặc force quit) để lại file lock của Chromium. | Cơ chế `clean_profile_locks()` đã tự động xử lý khi khởi động, hoặc bạn có thể xóa file lock thủ công: `rm -f profiles/facebook/Singleton*`. |
| Máy Mac tự tắt màn hình / ngắt mạng khi cào phiên dài | Chế độ Sleep mặc định của macOS khi không có thao tác bàn phím/chuột. | Luôn thêm tiền tố `caffeinate -i` ở đầu lệnh cào dài (ví dụ: `caffeinate -i python3 -u facebook_pipeline.py ...`). Cắm sạc và để mở nắp máy. |
| Pipeline dừng với cảnh báo `Facebook authentication wall or checkpoint detected` | Cookie phiên duyệt web Facebook bị hết hạn hoặc tài khoản bị yêu cầu checkpoint. | Tiến độ cào đã được lưu an toàn vào SQLite. Hãy chạy lại lệnh `python3 cloak_facebook_login.py` để đăng nhập lại, sau đó chạy lại lệnh cào. |

---

## 8. Bảng tham số dòng lệnh chi tiết (CLI Reference)

### 8.1. `facebook_pipeline.py`

#### Tham số chung (Common Flags)
| Tham số | Kiểu | Mặc định | Ý nghĩa |
|---|---|---|---|
| `--mode` | String | `page` | Chế độ chạy: `page` (quét danh sách Fanpage) hoặc `keyword` (tìm kiếm Facebook Search theo từ khóa). |
| `--output-dir` | Path | `output` | Thư mục lưu trữ kết quả và SQLite database. |
| `--max-hours` | Float | `1.0` | Giới hạn thời gian chạy phiên (ví dụ: `8.0` cho phiên 8 tiếng). |
| `--target-valid` | Integer | `None` | Dừng sớm khi đã thu thập đủ số lượng bài viết hợp lệ mục tiêu. |
| `--batch-size` | Integer | `20` | Số lượng bài viết phát hiện trước khi trích xuất theo lô. |
| `--limit` | Integer | `None` | Giới hạn tổng số bài viết trích xuất tối đa trong toàn phiên. |
| `--headless` | Flag | `False` | Chạy trình duyệt ngầm không mở giao diện (khuyến nghị `False` để bypass bot detection tốt hơn). |

#### Tham số cho chế độ Fanpage (`--mode page`)
| Tham số | Kiểu | Mặc định | Ý nghĩa |
|---|---|---|---|
| `--pages <file>` | Path | `None` | File danh sách URL Fanpage nguồn (mỗi dòng 1 URL). |
| `--competition` | String | `NBA` | Giải đấu mục tiêu cần đối chiếu bằng chứng. |
| `--sport-hint` | String | `Basketball` | Môn thể thao gợi ý cho taxonomy. |
| `--source-max-scrolls` | Integer | `100` | Số vòng cuộn tối đa cho mỗi nguồn Fanpage. |
| `--idle-rounds` | Integer | `10` | Số vòng cuộn liên tiếp không thấy bài mới trước khi chuyển sang nguồn tiếp theo. |
| `--max-refreshes` | Integer | `0` | Số lần reload trang khi đứng bài (0: tắt reload). |

#### Tham số cho chế độ Từ khóa (`--mode keyword`)
| Tham số | Kiểu | Mặc định | Ý nghĩa |
|---|---|---|---|
| `--queries` | String | `None` | Danh sách từ khóa/cụm từ tìm kiếm, cách nhau bằng dấu phẩy (ví dụ: `"xe điện,pin LFP"`). |
| `--queries-file` | Path | `None` | Đường dẫn file `.txt` chứa danh sách từ khóa (mỗi dòng một câu truy vấn, hỗ trợ comment `#`). |
| `--collection-id` | String | `None` | Tên collection định danh đợt cào (nếu bỏ trống, hệ thống tự sinh hash ổn định từ danh sách queries). |
| `--keyword-match` | String | `all` | Điều kiện lọc nội dung bài viết: `all` (phải chứa tất cả từ khóa trong query) hoặc `any` (chứa ít nhất 1 từ khóa). |
| `--exclude-keywords` | String | `None` | Danh sách từ khóa phủ định loại trừ bài viết (cách nhau bằng dấu phẩy). |
| `--strip-accents` | Flag | `False` | Khớp từ khóa tiếng Việt không phân biệt dấu (ví dụ: `xe dien` khớp cả `xe điện`). |
| `--limit-per-query` | Integer | `None` | Số lượng permalink tối đa thu thập cho mỗi từ khóa tìm kiếm. |
| `--poll-interval-sec` | Integer | `120` | Khoảng thời gian nghỉ (giây) giữa các vòng quét tìm kiếm trong phiên dài (đặt `0` để chỉ quét 1 lượt rồi dừng). |


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

## 9. Kiểm thử tự động (Test Suite 81/81 Passed)

Toàn bộ hệ thống được bảo vệ bởi **81 ca kiểm thử tự động (Unit & Integration Tests)**, chạy hoàn toàn offline không gọi mạng:

```bash
PYTHONPATH=. .venv/bin/pytest -v
```

### Phân bố kiểm thử:
* `tests/test_facebook_keyword_pipeline.py` (15 tests): Kiểm thử toàn diện luồng Facebook theo keyword: mã hóa URL tìm kiếm tiếng Việt/ký tự đặc biệt, đọc file queries UTF-8, khớp cụm từ ranh giới từ (tránh khớp "AI" trong "Thailand"), hashtag, loại trừ từ khóa, tùy chọn bỏ dấu tiếng Việt, chuẩn hóa permalink Group/Profile, cấu trúc bảng `collections` & `collection_items`, chống trùng multi-query, tái sử dụng cache cào từ collection khác, trích xuất 17 cột đề tài generic (sport_category rỗng, metrics null, cho phép video caption), và luồng runner đầu-cuối offline.
* `tests/test_x_pipeline.py` (9 tests): Kiểm thử ánh xạ X sang 17 cột, cách ly metric view/quote, user placeholder, bộ lọc scope gate (Warriors không ngữ cảnh), cách ly dữ liệu demo, và gộp nguồn truy vấn (multi-query deduplication).
* `tests/test_audit_improvements.py` (17 tests): Kiểm thử retry tương tác, xử lý video DOM, trích xuất số liệu đa ngôn ngữ, và đồng bộ CSV/JSON/JSONL.
* `tests/test_discovery_engine.py` (9 tests): Kiểm thử Taxonomy 4 môn thể thao, keyword classifier, bộ điều phối engine và các adapters.
* `tests/test_local_units.py` (16 tests): Kiểm thử các hàm đơn vị trích xuất ID, chuẩn hóa thời gian ISO, tách biệt caption và comment.
* `tests/test_nba_pipeline.py` (15 tests): Kiểm thử bộ lọc NBA, vòng điều phối 2 tab, monotonic deadline, và khôi phục task sau crash.
