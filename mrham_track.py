"""
=============================================================
 mrham_track.py — 추천 기록 & 사후 검증
=============================================================
 신뢰성은 설명의 정교함이 아니라 '기록된 적중률'에서 나온다.
 AI 추천을 시점·가격·근거와 함께 저장하고, 1주/1개월/3개월 뒤
 실제 성과를 채워 넣어 벤치마크(KOSPI) 대비 초과수익을 측정한다.

 필요 테이블 (Supabase SQL):
 ─────────────────────────────────────────────────────────
 CREATE TABLE recommendations (
   id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
   user_id       uuid NOT NULL,
   ticker        text NOT NULL,
   name          text,
   rec_date      date NOT NULL,
   rec_price     integer,
   target_price  integer,
   stop_price    integer,
   opinion       text,
   horizon       text,
   score_snapshot jsonb,
   reason        text,
   price_1w      integer,
   price_1m      integer,
   price_3m      integer,
   bench_1w      numeric,
   bench_1m      numeric,
   bench_3m      numeric,
   evaluated_at  timestamptz,
   created_at    timestamptz DEFAULT now()
 );
 CREATE INDEX idx_rec_user_date ON recommendations(user_id, rec_date DESC);
 CREATE UNIQUE INDEX idx_rec_unique ON recommendations(user_id, ticker, rec_date);
=============================================================
"""

from __future__ import annotations

import datetime
import re
from typing import Optional

KST = datetime.timezone(datetime.timedelta(hours=9))


def today_kst() -> datetime.date:
    return datetime.datetime.now(KST).date()


def now_kst() -> datetime.datetime:
    return datetime.datetime.now(KST)


# ═══════════════════════════════════════════════════════════
#  AI 리포트에서 추천 종목 파싱
# ═══════════════════════════════════════════════════════════
_NUM = r"[\d,]+"


def _to_int(s) -> Optional[int]:
    if s is None:
        return None
    d = re.sub(r"[^\d]", "", str(s))
    return int(d) if d else None


def parse_recommendations(report: str) -> list:
    """
    AI 마크다운 리포트에서 '신규 추천' 섹션의 종목을 추출.

    지원 형식 (둘 다):
      - **종목명(005930) | 메가트렌드 | ... | 71,000원/85,000원 | 분할매수**
      ### 📌 종목명 (005930)
      - **현재가**: 71,000원
      - **목표가**: 85,000원

    ※ 순수 함수 — 네트워크 없이 테스트 가능
    """
    if not report:
        return []

    # '신규 추천' 섹션만 잘라내기 (보유 종목 분석과 혼동 방지)
    sec = report
    m = re.search(r"신규\s*추천", report)
    if m:
        rest = report[m.start():]
        nxt = re.search(r"\n##\s", rest)
        sec = rest[:nxt.start()] if nxt else rest

    out: list = []
    seen: set = set()

    for line in sec.split("\n"):
        codes = re.findall(r"\((\d{6})\)", line)
        if not codes:
            continue
        code = codes[0]
        if code in seen:
            continue

        # 종목명: (코드) 바로 앞 텍스트에서 마크다운 기호 제거
        head = line.split(f"({code})")[0]
        head = re.sub(r"[#*\-•📌💎\[\]]", " ", head)
        head = re.sub(r"^.*?\|", "", head) if head.count("|") >= 2 else head
        name = head.strip().split("|")[-1].strip()[:40] or code

        prices = [_to_int(x) for x in re.findall(_NUM + r"\s*원", line)]
        prices = [p for p in prices if p and p > 0]

        rec = {"ticker": code, "name": name,
               "rec_price": prices[0] if len(prices) >= 1 else None,
               "target_price": prices[1] if len(prices) >= 2 else None,
               "stop_price": prices[2] if len(prices) >= 3 else None,
               "opinion": "신규매수", "horizon": None, "reason": line.strip()[:500]}

        # 라벨 기반 보강 (### 블록 형식)
        blk = _extract_block(sec, code)
        if blk:
            # [버그수정] "현재가 / 목표가 / 손절가: 71,000원 / 85,000원 / 64,000원"
            # 같은 결합 라벨 형식은 라벨-숫자 거리가 멀어 개별 정규식이 실패했음.
            # → 결합 라인을 먼저 탐지해 순서대로 매핑
            combo = re.search(
                r"현재가[^:\n]*목표가[^:\n]*(?:손절[^:\n]*)?[:：]\s*"
                r"(" + _NUM + r")\s*원[^\d]{0,20}"
                r"(" + _NUM + r")\s*원(?:[^\d]{0,20}(" + _NUM + r")\s*원)?",
                blk)
            if combo:
                for gi, key in ((1, "rec_price"), (2, "target_price"),
                                (3, "stop_price")):
                    v = _to_int(combo.group(gi)) if combo.group(gi) else None
                    if v:
                        rec[key] = v
            else:
                for label, key in (("현재가", "rec_price"), ("목표가", "target_price"),
                                   ("손절", "stop_price")):
                    mm = re.search(label + r"[^\d]{0,12}(" + _NUM + r")\s*원", blk)
                    if mm:
                        v = _to_int(mm.group(1))
                        if v:
                            rec[key] = v
            hm = re.search(r"(단기|중기|장기)", blk)
            if hm:
                rec["horizon"] = hm.group(1)
        if rec["horizon"] is None:
            hm = re.search(r"(단기|중기|장기)", line)
            rec["horizon"] = hm.group(1) if hm else None

        seen.add(code)
        out.append(rec)

    return out


def _extract_block(text: str, code: str) -> str:
    """해당 종목코드가 등장한 지점부터 다음 헤더 전까지"""
    i = text.find(f"({code})")
    if i < 0:
        return ""
    tail = text[i:]
    nxt = re.search(r"\n#{2,3}\s", tail)
    return tail[:nxt.start()] if nxt else tail[:1200]


# ═══════════════════════════════════════════════════════════
#  Supabase 저장 / 조회
# ═══════════════════════════════════════════════════════════
def save_recommendations(sb, user_id: str, recs: list,
                         score_map: Optional[dict] = None,
                         rec_date: Optional[str] = None) -> dict:
    """
    추천 목록을 recommendations 테이블에 저장 (upsert).
    Returns: {"saved": int, "status": "ok"|"skip"|"error", "message": str}
    """
    if not sb or not user_id:
        return {"saved": 0, "status": "skip", "message": "DB 미연결"}
    if not recs:
        return {"saved": 0, "status": "skip", "message": "추천 종목 없음"}

    d = rec_date or today_kst().strftime("%Y-%m-%d")
    rows = []
    for r in recs:
        if not r.get("ticker"):
            continue
        rows.append({
            "user_id":        user_id,
            "ticker":         r["ticker"],
            "name":           r.get("name"),
            "rec_date":       d,
            "rec_price":      r.get("rec_price"),
            "target_price":   r.get("target_price"),
            "stop_price":     r.get("stop_price"),
            "opinion":        r.get("opinion"),
            "horizon":        r.get("horizon"),
            "score_snapshot": (score_map or {}).get(r["ticker"]),
            "reason":         r.get("reason"),
        })
    if not rows:
        return {"saved": 0, "status": "skip", "message": "유효 행 없음"}
    try:
        sb.table("recommendations").upsert(
            rows, on_conflict="user_id,ticker,rec_date").execute()
        return {"saved": len(rows), "status": "ok", "message": f"{len(rows)}건 저장"}
    except Exception as e:
        return {"saved": 0, "status": "error", "message": str(e)}


def load_recommendations(sb, user_id: str, limit: int = 200) -> list:
    if not sb or not user_id:
        return []
    try:
        res = (sb.table("recommendations").select("*")
               .eq("user_id", user_id)
               .order("rec_date", desc=True).limit(limit).execute())
        return res.data or []
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════
#  사후 성과 평가
# ═══════════════════════════════════════════════════════════
HORIZON_DAYS = {"1w": 7, "1m": 30, "3m": 90}


def due_horizons(rec_date_str: str, row: dict,
                 today: Optional[datetime.date] = None) -> list:
    """
    평가 시점이 도래했고 아직 값이 비어 있는 구간 목록 반환.
    ※ 순수 함수
    """
    t = today or today_kst()
    try:
        rd = datetime.date.fromisoformat(str(rec_date_str)[:10])
    except Exception:
        return []
    out = []
    for h, days in HORIZON_DAYS.items():
        if (t - rd).days >= days and row.get(f"price_{h}") is None:
            out.append(h)
    return out


def evaluate_recommendations(sb, user_id: str, price_fetcher,
                             bench_fetcher=None,
                             today: Optional[datetime.date] = None) -> dict:
    """
    도래한 구간의 실제 가격을 채워 넣음.
    price_fetcher: (ticker) -> 현재가 int
    bench_fetcher: () -> KOSPI 지수 float
    """
    rows = load_recommendations(sb, user_id, limit=500)
    if not rows:
        return {"updated": 0, "status": "skip", "message": "평가할 추천 없음"}

    bench_now = None
    if bench_fetcher:
        try:
            bench_now = bench_fetcher()
        except Exception:
            bench_now = None

    updated = 0
    for row in rows:
        hs = due_horizons(row.get("rec_date"), row, today)
        if not hs:
            continue
        try:
            price = price_fetcher(row["ticker"])
        except Exception:
            continue
        if not price or price <= 0:
            continue
        patch = {f"price_{h}": int(price) for h in hs}
        if bench_now:
            patch.update({f"bench_{h}": float(bench_now) for h in hs})
        patch["evaluated_at"] = now_kst().isoformat()
        try:
            sb.table("recommendations").update(patch).eq("id", row["id"]).execute()
            updated += 1
        except Exception:
            continue
    return {"updated": updated, "status": "ok", "message": f"{updated}건 갱신"}


def calc_performance(rows: list, horizon: str = "1m") -> dict:
    """
    적중률·평균수익률·벤치마크 대비 초과수익 계산.
    ※ 순수 함수 — 네트워크 없이 테스트 가능
    """
    key = f"price_{horizon}"
    evaluated = [r for r in rows
                 if r.get(key) and r.get("rec_price") and r["rec_price"] > 0]
    if not evaluated:
        return {"count": 0, "status": "insufficient",
                "message": f"{horizon} 평가 완료 건 없음"}

    rets, hits, tgt_hits = [], 0, 0
    for r in evaluated:
        ret = (r[key] - r["rec_price"]) / r["rec_price"] * 100
        rets.append(ret)
        if ret > 0:
            hits += 1
        if r.get("target_price") and r[key] >= r["target_price"]:
            tgt_hits += 1

    n        = len(rets)
    avg      = sum(rets) / n
    win_rate = hits / n * 100
    best     = max(rets)
    worst    = min(rets)
    median   = sorted(rets)[n // 2] if n % 2 else \
               (sorted(rets)[n // 2 - 1] + sorted(rets)[n // 2]) / 2

    # 벤치마크 대비 초과수익
    excess = None
    bk = f"bench_{horizon}"
    pairs = [r for r in evaluated if r.get(bk) and r.get("bench_at_rec")]
    if pairs:
        b_rets = [(r[bk] - r["bench_at_rec"]) / r["bench_at_rec"] * 100 for r in pairs]
        excess = avg - (sum(b_rets) / len(b_rets))

    return {
        "count": n, "status": "ok", "horizon": horizon,
        "avg_return": round(avg, 2), "median_return": round(median, 2),
        "win_rate": round(win_rate, 1),
        "target_hit_rate": round(tgt_hits / n * 100, 1),
        "best": round(best, 2), "worst": round(worst, 2),
        "excess_return": round(excess, 2) if excess is not None else None,
    }


def build_performance_summary(rows: list) -> str:
    """UI·프롬프트용 성과 요약 텍스트. ※ 순수 함수"""
    if not rows:
        return "아직 기록된 추천이 없습니다. 분석을 실행하면 자동으로 기록됩니다."
    parts = [f"총 추천 기록: {len(rows)}건", ""]
    any_eval = False
    for h, label in (("1w", "1주"), ("1m", "1개월"), ("3m", "3개월")):
        p = calc_performance(rows, h)
        if p["status"] != "ok":
            continue
        any_eval = True
        ex = f" | 벤치대비 {p['excess_return']:+.2f}%p" if p["excess_return"] is not None else ""
        parts.append(
            f"[{label}] 평가 {p['count']}건 | 승률 {p['win_rate']}% | "
            f"평균 {p['avg_return']:+.2f}% | 중앙값 {p['median_return']:+.2f}% | "
            f"최고 {p['best']:+.2f}% / 최악 {p['worst']:+.2f}%{ex}")
    if not any_eval:
        parts.append("아직 평가 시점(1주)이 도래한 추천이 없습니다.")
    return "\n".join(parts)
