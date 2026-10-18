# Facebook NBA Scraping & Classification Pipeline Rules

Tài liệu đặc tả toàn diện về kiến trúc dữ liệu, các điều kiện chấp nhận (acceptance gates), thuật toán phân loại và chính sách trích xuất của pipeline Social Listening Facebook về đề tài giải bóng rổ nhà nghề Mỹ (NBA).

---

## 1. Bảng Tổng Hợp Tất Cả Các Acceptance Gates

Pipeline áp dụng các cổng kiểm soát chất lượng (Quality Gates) nghiêm ngặt trước khi một bản ghi được đưa vào tập dữ liệu nghiên cứu chuẩn 17 cột:

| Gate / Trường | File / Hàm | Điều kiện thực tế | Bắt buộc? | Kết quả khi thiếu / không đạt | Cấu hình liên quan | Ví dụ minh họa |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Task Key & URL** | `facebook_pipeline.py`<br>`PipelineDB.enqueue_urls` | URL hợp lệ thuộc tên miền `facebook.com`, chuẩn hóa permalink | **Bắt buộc** | Bỏ qua hoặc ghi nhận `failed` | `--urls`, `--page`, `--pages` | `https://www.facebook.com/warriors/posts/123456789` |
| **Network & Access** | `facebook_pipeline.py`<br>`check_facebook_access_issues` | Trang phản hồi mã 200/DOM tải thành công, không gặp checkpoint/login wall | **Bắt buộc** | Dừng pipeline an toàn (`stop_reason="login_or_checkpoint_required"`), lưu checkpoint | `--headless`, `cloakbrowser` session | Phát hiện form input email/pass hoặc URL `/login`, `/checkpoint` |
| **Media Type Gate** | `facebook_pipeline.py`<br>`extract_scoped_post_data` | Post không phải video/reel độc lập; bỏ qua các bài chỉ có video clip | **Bắt buộc** | Gán `status="video_skipped"`, không đưa vào dataset nghiên cứu văn bản | Chính sách video dự án (`media_type == "video"`) | Post dạng Facebook Reel hoặc Video player chiếm trọn container |
| **Post Identity (post_id)** | `facebook_dataset.py`<br>`evaluate_record_acceptance` | ID bài viết duy nhất (`pfbid...` hoặc số nguyên ID), không được rỗng | **Bắt buộc** | Loại bài: `missing_post_id` | Tự động trích từ URL permalink hoặc thuộc tính DOM `data-post-id` | `pfbid02y4vrT2oFbZ69kZUCgFwMfACidF5w2iVsfTPowSxZy9ieU24GoGbtRFnjGjjpGPvHl` |
| **Post URL (post_url)** | `facebook_dataset.py`<br>`evaluate_record_acceptance` | URL chuẩn hóa của bài viết | **Bắt buộc** | Loại bài: `missing_post_url` | Tự động resolve canonical permalink | `https://www.facebook.com/warriors/posts/pfbid...` |
| **Văn bản Caption (text)** | `facebook_dataset.py`<br>`evaluate_record_acceptance` | Chuỗi ký tự caption không rỗng, tối thiểu ký tự có nghĩa sau khi lọc URL | **Bắt buộc** | Loại bài: `missing_caption_text` | Cắt bỏ URL rác, giữ nguyên hashtag/mention có nghĩa | *"Stephen Curry dropped 42 points with 8 three-pointers for the Warriors!"* |
| **Thời điểm đăng (published_at)** | `facebook_dataset.py`<br>`evaluate_record_acceptance` | Chuỗi ISO 8601 có múi giờ (`+00:00` hoặc timezone offset thực tế) | **Bắt buộc** | Loại bài: `missing_published_at` | Trích từ thẻ `<abbr data-utime>`, hover timestamp, hoặc chuỗi thời gian exact | `2026-10-15T12:00:00+00:00` |
| **Thời điểm thu thập (collected_at)** | `facebook_dataset.py`<br>`evaluate_record_acceptance` | Chuỗi ISO 8601 thời điểm quan sát thực tế (bảo toàn khi reprocess) | **Bắt buộc** | Loại bài: `missing_collected_at` | Sinh bởi `datetime.now(timezone.utc)` | `2026-10-17T07:08:56.596703+00:00` |
| **Môn thể thao (sport_category)** | `facebook_dataset.py`<br>`classify_post_sport` | Phải chứa danh mục `Basketball` | **Bắt buộc** | Loại bài: `sport_category_mismatch` | Tự động đồng bộ `Basketball` khi phát hiện bằng chứng NBA | `Basketball` (loại bỏ nhãn giả `Shooting`, `Athletics`) |
| **Phạm vi giải đấu (competition)** | `facebook_dataset.py`<br>`classify_nba_post` | Phải đạt bằng chứng NBA (`status="matched"`), không thuộc giải ngoài phạm vi | **Bắt buộc** | Chuyển `unmatched` hoặc `uncertain` (cần review) | `--nba-policy {contextual,strict}` | Đội bóng NBA, cầu thủ NBA, hoặc context từ trang chính thức đã xác minh |
| **Tương tác (like_count)** | `facebook_dataset.py`<br>`evaluate_record_acceptance` | Số nguyên không âm ($\ge 0$), đo lường tổng reactions | **Bắt buộc** | Loại bài: `missing_or_invalid_like_count` | Trích từ nút reaction badge; không tự ý gán `0` nếu không có huy hiệu | `1250` (nguyên tắc: `null` biểu thị chưa biết/không hiển thị, `0` khi có bằng chứng) |
| **Lượt xem (view_count)** | `facebook_dataset.py`<br>`extract_scoped_post_data` | Public view count quan sát được trên bài viết | *Tùy chọn* | Để `null` (không loại bài vì thiếu view) | `--view-policy {verified_only,off}` (mặc định: `verified_only`) | `null` khi Facebook không hiển thị; `1500` khi có phần tử metric rõ ràng |
| **Ngôn ngữ (language)** | `facebook_dataset.py`<br>`detect_language_with_meta` | Mã ngôn ngữ 2 ký tự ISO 639-1 hoặc `unknown` | *Tùy chọn* | Gán `unknown` (KHÔNG phải gate loại bài) | Detector offline: `lingua-language-detector (2.2.0)` | `en`, `vi`, `es`, `tl`, `unknown` |
| **Người theo dõi (author_followers)** | `facebook_pipeline.py`<br>`fetch_author_followers` | Tổng lượt follower của trang tác giả (cached 24h) | *Tùy chọn* | Để `null` nếu không tìm thấy profile | `--no-followers` (để tắt nếu cần tối ưu tốc độ) | `12000000` |
| **Comment & Share** | `facebook_dataset.py`<br>`parse_numeric_count` | Số nguyên không âm nếu hiển thị | *Tùy chọn* | Để `null` nếu không có số liệu | Trích xuất theo vùng metric scoped | `comment_count=45`, `share_count=30` |

---

## 2. Quy Định Chi Tiết Về Trích Xuất Metric Lượt Xem (view_count)

### 2.1. Phân biệt chế độ cấu hình (`--view-policy`)

1. **`verified_only` (Mặc định):**
   - Khởi tạo giá trị `view_count = None` cho mỗi bài đăng trước khi bắt đầu trích xuất.
   - Chỉ ghi nhận số nguyên khi tìm thấy phần tử DOM metric nằm trong vùng tương tác chính thức của đúng bài viết, với nhãn và số liệu gắn kết rõ ràng.
   - Nếu Facebook không hiển thị số view cho phiên truy cập hiện tại, xuất `null` kèm lý do kỹ thuật trong metadata (`not_displayed`).
2. **`off`:**
   - Không tìm kiếm view trong DOM; luôn gán `view_count = None` và lưu lý do `view_policy_disabled`. Phù hợp cho các đợt cào tốc độ cao khi nghiên cứu không khảo sát biến số này.

### 2.2. Quy tắc loại trừ tuyệt đối vùng bắt nhầm (False Positives)

Bộ trích xuất loại bỏ triệt để các trường hợp bắt nhầm số từ giao diện:
- **Action Buttons / Lệnh người dùng:** Bỏ qua các nút lệnh điều hướng như `"3\nView more comments"`, `"28\nView 2 replies"`, `"3\nView profile"`, `"View 5 more comments"`.
- **Nội dung bài viết (Caption text):** Các câu như *"My target is 1000 views this week"* nằm trong `postContainer.querySelector('[data-ad-preview="message"]')` bị cách ly hoàn toàn, không bao giờ được chuyển thành số đo.
- **Media nhúng / Bài lồng nhau (Nested / Shared posts):** View của video được chia sẻ lại không được gán thành view của bài viết cha.
- **Không suy diễn số liệu:** Tuyệt đối không dùng công thức `max(view_count, like_count, share_count)` và không gán số view dựa trên reactions.
- **Dữ liệu lịch sử (Legacy records):** Đối với các bản ghi cũ thu thập bằng regex lỗi dòng, trong bản clean dataset được gán `view_count = null` với nhãn `legacy_unverified`, trong khi giá trị số gốc được bảo toàn trong metadata `legacy_view_count` để phục vụ audit.

---

## 3. Cơ Chế Nhận Diện Ngôn Ngữ Thống Kê Offline

### 3.1. Bộ nhận diện Lingua Singleton

- **Thư viện chuẩn:** Sử dụng `lingua-language-detector` (v2.2.0) chạy offline hoàn toàn trên kiến trúc macOS ARM và Linux x86_64, không gọi API hay dịch vụ mạng bên ngoài.
- **Tái sử dụng Singleton (`_get_lingua_detector`):** Detector được nạp và khởi tạo một lần duy nhất khi import module, tránh chi phí cấp phát bộ nhớ cho mỗi bài viết.
- **Tập ngôn ngữ hỗ trợ:** Mở rộng linh hoạt gồm Tiếng Anh (`en`), Tiếng Việt (`vi`), Tiếng Tây Ban Nha (`es`), Tiếng Pháp (`fr`), Tiếng Bồ Đào Nha (`pt`), Tiếng Tagalog/Philippines (`tl`).

### 3.2. Hiệu chuẩn ngưỡng (Threshold Calibration) và câu ngắn

- **Đoạn văn ngắn / Tên riêng / Acronym (< 5 từ hoặc < 30 ký tự):** Yêu cầu độ tin cậy khắt khe:
  $$\text{Confidence} \ge 0.65 \quad \text{và} \quad \text{Margin} \ge 0.20$$
  Các chuỗi danh từ riêng như *"2022 NBA Finals MVP Steph Curry"* hoặc địa danh ngắn *"Queropalca Peru"* nếu không vượt ngưỡng sẽ trả về nhãn `unknown` kèm lý do `low_confidence_or_ambiguous`.
- **Đoạn văn đầy đủ ngữ cảnh ($\ge 5$ từ và $\ge 30$ ký tự):**
  $$\text{Confidence} \ge 0.50 \quad \text{và} \quad \text{Margin} \ge 0.10$$
  Câu như *"Let's talk more about the Warriors latest title:"* được định danh chính xác là `en` với độ tin cậy $\approx 0.87$.
- **Quy ước vàng:** `unknown` là nhãn phân loại có độ bất định, **không phải lỗi cấu trúc và không bao giờ là nguyên nhân khiến bài viết bị loại khỏi dataset**.

---

## 4. Chính Sách Quyết Định Basketball và NBA

Quy trình phân loại tách bạch 3 câu hỏi độc lập:
1. Bài có bằng chứng về môn Basketball không?
2. Bài có thuộc phạm vi giải NBA không?
3. Bài có đủ 17 trường hợp lệ để xuất bản không?

### 4.1. Chế độ Contextual vs Strict (`--nba-policy`)

1. **`contextual` (Mặc định cho tập nguồn NBA):**
   - Tận dụng registry nguồn đã xác minh (`NBA_SOURCE_REGISTRY`) bao gồm Fanpage chính thức của NBA, 30 câu lạc bộ thành viên, và các trang báo chí chuyên sâu về NBA (như *HeatNation*, *RaptorsNation*, *DetroitBadBoys*, *AirAlamo*, *8points9seconds*).
   - Cho phép tiếp nhận các bài đăng ngắn đặc thù của đội bóng chính thức như *"Back to work 🏀🔥"*, *"Half."*, *"T-minus seven days to regular season tip-off"* với `classification_basis="verified_source_context"`.
2. **`strict` (Chế độ chỉ dựa vào văn bản):**
   - Đòi hỏi caption phải chứa trực tiếp tên đội bóng, cầu thủ, hoặc từ khóa NBA rõ ràng. Nếu thiếu từ khóa, chuyển sang trạng thái `uncertain` (cần review).

### 4.2. Xử lý các giải đấu ngoài phạm vi và xung đột ngữ cảnh

- **Giải sinh viên Philippines (UAAP):** Phân biệt cụ thể đội *"UE Red Warriors"* hoặc *"UP Fighting Maroons"* trong các bản tin thể thao Philippines. Không bao giờ gán nhầm *"UE Red Warriors"* thành Golden State Warriors của NBA.
- **Giải bóng rổ nữ (WNBA):** Tách biệt từ khóa `WNBA` để không bị match nhầm từ `NBA`. Các bài thuần túy về giải nữ được đánh dấu `unmatched` với lý do `non_nba_competition_detected (WNBA)`.
- **Esports / Video Game (NBA 2K):** Các bài quảng bá game mô phỏng thuần túy bị loại trừ khỏi phạm vi nghiên cứu thực địa (`competition="NBA2K"`).
- **Cầu thủ NBA thi đấu quốc tế (FIBA / Olympics):** Bài về cầu thủ NBA (ví dụ Luka Dončić khoác áo đội tuyển Slovenia) được giữ lại với `competition="NBA"` và `secondary_competition="FIBA"`.
- **Hỗ trợ tên cầu thủ có dấu:** Hỗ trợ đầy đủ biến thể chính tả quốc tế: Dončić / Doncic, Jokić / Jokic, Porziņģis / Porzingis, Schröder / Schroder, Valančiūnas / Valanciunas.
- **Xóa bỏ nhãn đa môn giả:** Các thuật ngữ bóng rổ như *"shooting guard"*, *"sharpening the shot"*, *"running the floor"* được phân giải chính xác thành `Basketball`, không bị kích hoạt nhãn giả `Shooting` (bắn súng) hay `Athletics` (điền kinh).

### 4.3. Phân biệt nội dung thương mại vs Tuyển dụng

- **Hàng hóa, vé, áo đấu của đội NBA:** Được giữ lại trong phạm vi nghiên cứu với cờ `content_type="commercial_or_promo"`.
- **Tin tuyển dụng / Việc làm hành chính:** Các bài dạng *"We are hiring! Apply now for an administrative role at the NBA Store NYC"* không có ngữ cảnh trận đấu được xếp vào `uncertain` với `content_type="recruitment"`.

---

## 5. Chuẩn Hợp Đồng Dữ Liệu 17 Cột (Schema Contract)

Dữ liệu xuất bản tuân thủ thứ tự 17 cột, định dạng UTF-8-BOM (`utf-8-sig`), dấu ngắt dòng `\n`, ô trống biểu thị giá trị `null`:

```text
post_id,platform,text,published_at,collected_at,author_id,language,hashtags,mentions,like_count,comment_count,share_count,view_count,media_type,author_followers,post_url,sport_category
```

- **`null` vs `0`:**
  - `null` (ô trống): Thông tin không quan sát được hoặc không hiển thị trên giao diện.
  - `0`: Khi giao diện Facebook thực tế hiển thị con số 0.
- **`like_count`:** Đại diện cho tổng số lượt bày tỏ cảm xúc (reactions) đo lường được trên bài viết. Nếu không có huy hiệu reaction (bài chưa có ai tương tác hoặc giao diện ẩn), trường này mang giá trị `null` và bài viết được đưa vào danh sách cần revisit nếu là trường bắt buộc.
- **`collected_at`:** Luôn bảo toàn mốc thời gian thu thập gốc ban đầu khi thực hiện rà soát dữ liệu ngoại tuyến (offline reprocessing); các lần xử lý sau chỉ ghi nhận `reprocessed_at` trong metadata kỹ thuật.
