# Superset Runbook — Stage 2 EDA charts + Stage 4 dashboard

Полное руководство по созданию визуализаций в Apache Superset для проекта
**"Early detection of high-potential GitHub repositories"** (team28).

> Эта работа закрывает **сразу два чек-листа**:
> * Stage 2 — *EDA Requirements* (12 баллов): по одному чарту + описанию для
>   каждого из 6 запросов `q1..q6`.
> * Stage 4 — *Dashboard* (15 баллов): один Superset-дашборд, собирающий
>   эти чарты + модельные метрики + sample prediction + data-характеристики.
>
> **Бюджет времени** — 4-5 часов сфокусированной работы. Минимально достаточный
> вариант (без полного дашборда) — ~2 часа.

---

## 0. Подготовка

### 0.1 Доступ к Superset

* URL — обычно `http://hadoop-XX.uni.innopolis.ru:8088` (точный адрес узнать у Фираса
  или поискать в материалах курса).
* Логин: `team28@innopolis.university`
* Пароль: тот же кластерный, что для PostgreSQL/Hive.

### 0.2 Подготовить CSV для импорта

Перед загрузкой в Superset нужно **очистить пустые строки в начале**
каждого CSV-файла из `output/` — beeline добавляет их при экспорте,
и Superset подхватит первую пустую строку как заголовок.

```bash
# на локалке (git-bash) или на кластере
for f in output/q*.csv output/evaluation.csv; do
    sed -i '/./,$!d' "$f"
done
```

Проверить, что первая строка теперь — реальный header:
```bash
head -1 output/q1.csv
# должно быть: language,total_repos,pre_t_stars,candidate_repos,pct_candidates
```

---

## 1. Загрузить данные в Superset

Самый быстрый путь — **загрузить CSV напрямую**, не подключая Hive.

**Settings (правый верхний угол)** → **Upload a CSV** → выбрать файл →
дать имя датасету → **Save**.

Загрузить **все 7 файлов**:

| CSV-файл из output/   | Имя датасета в Superset |
|-----------------------|-------------------------|
| `q1.csv`              | `q1`                    |
| `q2.csv`              | `q2`                    |
| `q3.csv`              | `q3`                    |
| `q4.csv`              | `q4`                    |
| `q5.csv`              | `q5`                    |
| `q6.csv`              | `q6`                    |
| `evaluation.csv`      | `evaluation`            |

После загрузки проверь типы столбцов: `Datasets` → клик по датасету →
вкладка `Columns` → числовые поля должны быть `BIGINT/DOUBLE`, не `STRING`.
Если что-то не так — поправь вручную.

---

## 2. Создать 7 чартов

Для каждого: **Charts → + Chart → выбрать датасет → выбрать Visualization Type**.

После настройки — **Save** (даём имя чарту) и заполняем поле
**Description** (требование Stage 2 — narrative explanation для каждого insight).

### 2.1 q1 — Top language ecosystems

* **Тип:** `Bar Chart`
* **Dataset:** `q1`
* **X-axis:** `language`
* **Metrics:** `SUM(pre_t_stars)`
* **Sort by:** `pre_t_stars DESC`
* **Row Limit:** 15
* **Имя:** `q1 — Top language ecosystems by pre-T WatchEvent volume`
* **Description:**
  > Programming-language ecosystems with the largest pre-T (2023)
  > WatchEvent volume. Identifies where to look for breakout candidates —
  > languages with both wide repo bases and active starring behaviour.

### 2.2 q2 — Monthly star dynamics

* **Тип:** `Line Chart`
* **Dataset:** `q2`
* **X-axis:** `event_month` (1..12)
* **Group by:** `event_year` (даст две линии — 2023 и 2024)
* **Metrics:** `SUM(monthly_stars)`
* **Имя:** `q2 — Monthly WatchEvent dynamics across the data window`
* **Description:**
  > Baseline rhythm of starring across the 18-month data window.
  > Used to calibrate the 'growth ≥ 300%' threshold relative to the
  > underlying monthly trend.

### 2.3 q3 — Pre/Post-T growth distribution

* **Тип:** `Bar Chart`
* **Dataset:** `q3`
* **X-axis:** `growth_bucket`
* **Metrics:** `SUM(pct_of_total)`
* **Sort by:** `growth_bucket ASC` (префиксы `00_`, `01_`, ... сортируются естественно)
* **Имя:** `q3 — Pre-T vs Post-T star-growth distribution`
* **Description:**
  > Distribution of repositories across growth buckets between the
  > pre-T (2023) and post-T (2024 H1) windows. The three rightmost
  > buckets (≥2x growth) define the success class — together
  > representing &lt;0.15% of all repos.

### 2.4 q4 — Threshold sensitivity

* **Тип:** `Heatmap` (если доступен в твоей версии Superset; иначе — `Pivot Table`)
* **Dataset:** `q4`
* **Rows / Y:** `stars_min`
* **Columns / X:** `growth_min`
* **Metric:** `MAX(success_pct)`
* **Имя:** `q4 — Success-label sensitivity to threshold choice`
* **Description:**
  > Share of repos qualifying as 'success' under different (stars_min,
  > growth_min) threshold combinations. Drives the calibration of the
  > pseudo-label used in Stage 3 ML training.

### 2.5 q5 — Cohort behavioral profile

* **Тип:** `Bar Chart` (grouped)
* **Dataset:** `q5`
* **X-axis:** `is_success`
* **Metrics (добавить все 6):**
  * `MAX(avg_push_share)`
  * `MAX(avg_watch_share)`
  * `MAX(avg_pr_share)`
  * `MAX(avg_issues_share)`
  * `MAX(avg_fork_share)`
  * `MAX(avg_create_share)`
* **Имя:** `q5 — Behavioural profile: success vs non-success cohorts`
* **Description:**
  > Average distribution of event types per repository, broken down
  > by success label. Higher push/PR/issues shares in the success
  > cohort signal real engineering activity behind the WatchEvent
  > spike — validating that our features carry signal.

### 2.6 q6 — Acceleration anomalies

* **Тип:** `Bar Chart` (горизонтальная ориентация если есть)
* **Dataset:** `q6`
* **X-axis (или Y если горизонтальный):** `repo_name`
* **Metrics:** `MAX(acceleration)`
* **Sort by:** `acceleration DESC`
* **Row Limit:** 20
* **Имя:** `q6 — Top accelerating repos in Q4 2023`
* **Description:**
  > Top-20 repositories with the largest WatchEvent acceleration from
  > Q3 to Q4 2023 (≥5x growth, baseline ≥5 stars). These are exactly
  > the kinds of early-breakout signals the Stage 3 ML pipeline is
  > trained to detect.

### 2.7 evaluation — Model performance

* **Тип:** `Bar Chart`
* **Dataset:** `evaluation`
* **X-axis:** `model`
* **Metrics:** `MAX(AUROC)`, `MAX(AUPR)` (две группы)
* **Имя:** `Model performance — RF / SVM / Naive Bayes`
* **Description:**
  > Test-set AUROC and AUPR for the three binary classifiers
  > (Random Forest, Linear SVC, Naive Bayes). Random Forest leads
  > on PR-AUC (0.035), a ~4× lift over Naive Bayes baseline (0.005)
  > under the 0.08% positive-class extreme imbalance.

---

## 3. Экспорт чартов в JPG

В каждом чарте: правый верхний `…` → **Download** → **Download as Image**
(или **PNG**).

Положить файлы в `output/` со следующими именами:

```
output/q1.jpg
output/q2.jpg
output/q3.jpg
output/q4.jpg
output/q5.jpg
output/q6.jpg
output/evaluation.jpg
```

---

## 4. Собрать дашборд (Stage 4)

**Dashboards → + Dashboard** → дать имя:
`Early Detection of High-Potential GitHub Repositories — team28`.

### 4.1 Структура (drag-and-drop из правой панели)

```
+------------------------------------------------------------------+
|  Header: Early Detection of High-Potential GitHub Repositories   |
+------------------------------------------------------------------+
|  Data characteristics (markdown)                                 |
|  Dataset: GH Archive 10% sample, Jan 2023 – Jun 2024.            |
|  ~2.1M repositories analysed; 18 numerical features per repo     |
|  derived from pre-T (2023) event activity. Pseudo-label =        |
|  WatchEvent count ≥ 500 AND growth ≥ 3× over [2024-01..2024-06]. |
+------------------------------------------------------------------+
|  EDA — exploratory data analysis                                 |
|  +-------------+  +-------------+                                |
|  |   q1 chart  |  |   q2 chart  |                                |
|  +-------------+  +-------------+                                |
|  +-------------+  +-------------+                                |
|  |   q3 chart  |  |   q4 chart  |                                |
|  +-------------+  +-------------+                                |
+------------------------------------------------------------------+
|  Behavioural analysis & anomalies                                |
|  +-------------+  +-------------+                                |
|  |   q5 chart  |  |   q6 chart  |                                |
|  +-------------+  +-------------+                                |
+------------------------------------------------------------------+
|  ML results                                                      |
|  +-------------+  +---------------+                              |
|  |  evaluation |  |  Sample pred  |   <- markdown panel with     |
|  |  (AUROC/    |  |  repo_id ...  |      stage3_sample_*.csv     |
|  |   AUPR)     |  |  predictions  |      content                 |
|  +-------------+  +---------------+                              |
+------------------------------------------------------------------+
```

### 4.2 Markdown-панели

**Header (большой шрифт):**
```markdown
# Early Detection of High-Potential GitHub Repositories
Anomaly detection + growth-based pseudo-labeling on GH Archive (team28)
```

**Data characteristics:**
```markdown
**Dataset:** GH Archive 10% sample (`MOD(repo.id, 10) = 0`), Jan 2023 – Jun 2024.

**Analysis unit:** repository.
**Rows after pre-T filter:** 2,100,419
**Features per repo:** 18 (event-type counts, daily-activity intensity,
intra-year growth, behavioural shares).
**Target (pseudo-label):** `success = 1` if post-T WatchEvent count ≥ 500
AND ≥ 3× the pre-T count, else 0.  Reference: Borges & Valente (MSR 2018).

**Class balance:** 1,714 positives (0.082%) — extreme imbalance.
```

**Sample prediction (markdown panel):**
```markdown
**Sample prediction**
* `repo_id` = 21331090
* True label = 1
* RF prediction = 0
* SVM prediction = 0
* NB prediction = 1

Only Naive Bayes caught this hard positive case under the extreme
class imbalance.  Full feature vector is available in
`output/stage3_sample_features.csv`.
```

### 4.3 Экспорт дашборда

Правый верхний `…` → **Download** → **As Image** → сохранить в
`output/dashboard.jpg`.

---

## 5. Минимальный путь, если поджимает время

Если на полный дашборд времени нет, делать **в этом порядке** (по убыванию баллов
за минуту работы):

1. **6 EDA-чартов** (q1..q6) — ~12 баллов Stage 2 + 6 баллов Stage 4. ⏱ ~2 часа.
2. **Чарт `evaluation`** — 4 балла Stage 4. ⏱ 15 мин.
3. **Markdown с data characteristics** — 1 балл Stage 4. ⏱ 5 мин.
4. **Сборка дашборда + экспорт** — 4 балла Stage 4 (visual quality). ⏱ 45 мин.
5. **Прочие markdown-панели** — мелочёвка. ⏱ 15 мин.

**Минимум** (1+2+3) = ~17 баллов за 2.5 часа.
**Полный план** = ~21 балл (12 EDA + 9 dashboard) за 4-5 часов.

---

## Чек-лист готовности

* [ ] CSV-ы очищены от пустых строк
* [ ] 7 датасетов загружены в Superset
* [ ] 7 чартов созданы, у каждого есть Description
* [ ] 7 JPG-файлов в `output/q*.jpg` + `output/evaluation.jpg`
* [ ] Дашборд собран и сохранён
* [ ] `output/dashboard.jpg` выгружен
* [ ] Все JPG закоммичены в репу

---

## Если что-то идёт не так

* **CSV не загружается / Superset ругается на типы:** проверь что нет пустых
  строк в начале (`head -1 q1.csv` должен показать header).
* **Чарт пустой:** проверь что метрика выбрана `MAX(...)` или `SUM(...)`,
  а не просто имя поля.
* **Heatmap для q4 недоступен:** используй `Pivot Table` — выглядит чуть
  скучнее, но передаёт ту же информацию.
* **Hive-подключение требуется:** SQLAlchemy URI =
  `hive://team28:<пароль>@hadoop-03.uni.innopolis.ru:10001/team28_projectdb`,
  но через CSV всё равно быстрее.

Если застряла где-то конкретно — пингани в чате, разберёмся.
