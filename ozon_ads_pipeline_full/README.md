# Ozon Performance Ads 全流程（含多凭证轮换） → MongoDB

本项目包含两条流水线：
1) **活动级（日报）**：获取活动列表 → 按状态筛选 → 申请日报（UUID）→ 轮询状态 → 下载报告 → 与活动元数据合并 → 入库集合 `mbcampagin`
2) **商品级（单日）**：为指定日期调用 `/api/client/statistic/products/generate/json` → 轮询 UUID → 下载 `rows` → 注入 `date` → 入库集合 `opcampaign`
   - 另提供**日期范围批跑**入口，逐日调用商品级接口

支持 **多凭证自动轮换**：遇到 429 时切换至下一组 `client_id/client_secret` 并重试，直到遍历完全部凭证。

---

## 安装

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 配置环境变量（`.env` 推荐）

> 二选一：**JSON 列表（推荐）** 或 **逗号分隔**。未配置则回退单凭证 `OZON_CLIENT_ID` / `OZON_CLIENT_SECRET`。

```env
# --- 推荐：JSON 列表 ---
OZON_CLIENTS_JSON=[
  {"client_id":"82021481-1755918620104@advertising.performance.ozon.ru","client_secret":"<secret-1>"},
  {"client_id":"57831202-1741573215907@advertising.performance.ozon.ru","client_secret":"<secret-2>"}
]

# --- 兼容：逗号分隔 ---
#OZON_CLIENT_ID_LIST=cid1,cid2
#OZON_CLIENT_SECRET_LIST=sec1,sec2

# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB=ozonads
MONGO_COLL=mbcampagin   # 活动级集合
# 商品级集合固定为 opcampaign（见 src/op_mongo_utils.py）
```

---

## 使用

### 1) 活动级（日报 → `mbcampagin`）
```bash
python main.py --date-from 2025-08-20 --date-to 2025-08-21 --states CAMPAIGN_STATE_RUNNING
# 仅处理指定活动：
python main.py --date-from 2025-08-20 --date-to 2025-08-21 --campaign-ids 16734360 16774892
```

### 2) 商品级·单日（→ `opcampaign`）
```bash
python main_products.py --day 2025-08-24
```

### 3) 商品级·日期范围批跑（→ `opcampaign`）
```bash
python main_products_range.py --date-from 2025-08-01 --date-to 2025-08-07 --delay-seconds 1.0
# 失败即停：加 --stop-on-error
```

---

## 说明
- 所有请求自动携带 `Authorization: Bearer <access_token>` 与 `client-id` 头。
- 返回数值常见为俄式格式（`,` 作小数点），已统一解析为 float。
- 报告缺少 `date` 时会按请求日期（商品级）或按区间均匀补齐（活动级）。
- Mongo 幂等键：`mbcampagin` 为 `(campaignId, date, sku)`；`opcampaign` 为 `(date, sku)`。

> 注意：不要将脚本命名为 `token.py`，避免与标准库冲突导致 `requests` 半初始化错误。
