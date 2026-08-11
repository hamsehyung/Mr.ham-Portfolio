"""
=============================================================
 mrham_screener.py — Layer 0~2 전종목 스크리닝 엔진
=============================================================
 Layer 0: 전종목 유니버스 수집  (FDR → pykrx → Naver 폴백)
 Layer 1: 하드컷 필터            (시총·거래대금·PER·PBR·적자)
 Layer 2: 4팩터 다인자 랭킹      (가치·퀄리티·모멘텀·수급)

 설계 원칙
 ─────────────────────────────────────────────────────────
 · pykrx 1.2.8+ 는 KRX_ID/KRX_PW 로그인을 요구하므로 필수 의존 금지.
   → FinanceDataReader.StockListing 을 1순위로 사용 (로그인 불필요)
 · 비싼 데이터(수급·모멘텀)는 하드컷 통과 종목에만 수집 → 호출량 최소화
 · 모든 순수 계산 함수는 네트워크 없이 단위 테스트 가능하도록 분리
=============================================================
"""

from __future__ import annotations

import datetime
import math
import re
from typing import Optional

import numpy as np
import pandas as pd

# ── 선택적 의존성 ────────────────────────────────────────────
try:
    import FinanceDataReader as fdr
    HAS_FDR = True
except ImportError:
    HAS_FDR = False
    fdr = None

try:
    from pykrx import stock as pykrx_stock
    HAS_PYKRX = True
except ImportError:
    HAS_PYKRX = False
    pykrx_stock = None

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


KST = datetime.timezone(datetime.timedelta(hours=9))


def today_kst() -> datetime.date:
    return datetime.datetime.now(KST).date()


# ═══════════════════════════════════════════════════════════
#  하드컷 기준 (Layer 1) — 한 곳에서 관리
# ═══════════════════════════════════════════════════════════
HARDCUT = {
    "min_market_cap":  300_000_000_000,   # 시총 3,000억 이상
    "min_turnover":      3_000_000_000,   # 20일 평균 거래대금 30억 이상
    "per_min":                     0.0,   # PER > 0  (적자 제외)
    "per_max":                    60.0,   # PER ≤ 60 (극단 고평가 제외)
    "pbr_min":                     0.0,   # PBR > 0  (자본잠식 제외)
    "exclude_spac":               True,   # 스팩 제외
    "exclude_preferred":          True,   # 우선주 제외
    "exclude_reit":              False,   # 리츠는 허용
}

# ═══════════════════════════════════════════════════════════
#  팩터 가중치 (Layer 2) — 합이 1.0 이 되어야 함
# ═══════════════════════════════════════════════════════════
FACTOR_WEIGHTS = {
    "value":     0.25,   # 저PER·저PBR
    "quality":   0.25,   # ROE
    "momentum":  0.30,   # 6개월 수익률 - 최근 1개월 (반전효과 회피)
    "flow":      0.20,   # 외국인+기관 20일 순매수 / 시총
}


# ═══════════════════════════════════════════════════════════
#  Layer 0 — 전종목 유니버스 수집
# ═══════════════════════════════════════════════════════════
def _is_preferred_stock(name: str, ticker: str) -> bool:
    """우선주 판별: 종목코드 끝자리가 0이 아니거나 이름에 '우' 접미사"""
    if not ticker or len(ticker) != 6:
        return False
    # 보통주는 코드 끝자리 0, 우선주는 5/7/K/L 등
    if ticker[-1] != "0":
        return True
    if re.search(r"(우|우B|우C|\(전환\))$", str(name or "")):
        return True
    return False


def _is_spac(name: str) -> bool:
    return "스팩" in str(name or "") or "SPAC" in str(name or "").upper()


def fetch_universe_fdr() -> pd.DataFrame:
    """
    1순위: FinanceDataReader.StockListing('KRX')
    로그인 불필요. 컬럼: Code, Name, Market, Close, Marcap, Stocks, ...
    """
    if not HAS_FDR:
        raise RuntimeError("FinanceDataReader 미설치")
    df = fdr.StockListing("KRX")
    if df is None or df.empty:
        raise RuntimeError("FDR StockListing 빈 응답")
    return normalize_universe(df, source="FDR")


def fetch_universe_pykrx(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    2순위: pykrx (KRX_ID/KRX_PW 환경변수 설정 시에만 안정 동작)
    """
    if not HAS_PYKRX:
        raise RuntimeError("pykrx 미설치")
    d = date_str or today_kst().strftime("%Y%m%d")
    cap  = pykrx_stock.get_market_cap(d, market="ALL")
    fund = pykrx_stock.get_market_fundamental(d, market="ALL")
    if cap is None or cap.empty:
        raise RuntimeError("pykrx 빈 응답 (KRX 로그인 필요 가능성)")
    df = cap.join(fund, how="left").reset_index()
    df = df.rename(columns={"티커": "Code", "종가": "Close", "시가총액": "Marcap",
                            "거래대금": "TradeAmount", "상장주식수": "Stocks"})
    if "Code" not in df.columns and "index" in df.columns:
        df = df.rename(columns={"index": "Code"})
    try:
        df["Name"] = df["Code"].map(pykrx_stock.get_market_ticker_name)
    except Exception:
        df["Name"] = ""
    return normalize_universe(df, source="pykrx")


def normalize_universe(df: pd.DataFrame, source: str = "") -> pd.DataFrame:
    """
    서로 다른 소스의 컬럼명을 표준 스키마로 통일.
    표준 스키마: code, name, market, close, marcap, trade_amount, per, pbr, roe, eps, bps, div
    ※ 순수 변환 함수 — 네트워크 없이 테스트 가능
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["code", "name", "market", "close", "marcap",
                                     "trade_amount", "per", "pbr", "roe", "eps", "bps", "div"])

    out = pd.DataFrame(index=range(len(df)))
    src = df.reset_index(drop=True)

    def pick(*names, default=None):
        for n in names:
            if n in src.columns:
                return src[n]
        return pd.Series([default] * len(src))

    out["code"]         = pick("Code", "code", "티커", "종목코드").astype(str).str.zfill(6)
    out["name"]         = pick("Name", "name", "종목명", default="").astype(str)
    out["market"]       = pick("Market", "market", "시장구분", default="").astype(str)
    out["close"]        = pd.to_numeric(pick("Close", "close", "종가"), errors="coerce")
    out["marcap"]       = pd.to_numeric(pick("Marcap", "marcap", "시가총액"), errors="coerce")
    out["trade_amount"] = pd.to_numeric(pick("Amount", "TradeAmount", "거래대금"), errors="coerce")
    out["per"]          = pd.to_numeric(pick("PER", "per"), errors="coerce")
    out["pbr"]          = pd.to_numeric(pick("PBR", "pbr"), errors="coerce")
    out["eps"]          = pd.to_numeric(pick("EPS", "eps"), errors="coerce")
    out["bps"]          = pd.to_numeric(pick("BPS", "bps"), errors="coerce")
    out["div"]          = pd.to_numeric(pick("DIV", "div", "배당수익률"), errors="coerce")

    # ROE 유도: BPS·EPS 가 있으면 EPS/BPS×100, 없으면 PBR/PER×100
    roe = pd.to_numeric(pick("ROE", "roe"), errors="coerce")
    derived = pd.Series([np.nan] * len(src), dtype="float64")
    mask_eb = out["bps"].notna() & (out["bps"] > 0) & out["eps"].notna()
    derived[mask_eb] = out.loc[mask_eb, "eps"] / out.loc[mask_eb, "bps"] * 100
    mask_pp = derived.isna() & out["per"].notna() & (out["per"] > 0) & out["pbr"].notna()
    derived[mask_pp] = out.loc[mask_pp, "pbr"] / out.loc[mask_pp, "per"] * 100
    out["roe"] = roe.fillna(derived)

    out["source"] = source
    out = out[out["code"].str.match(r"^\d{6}$", na=False)]
    return out.drop_duplicates(subset=["code"]).reset_index(drop=True)


def fetch_universe() -> tuple[pd.DataFrame, str]:
    """
    폴백 체인으로 유니버스 수집.
    Returns: (DataFrame, 사용된 소스명)
    """
    errors = []
    for label, fn in (("FDR", fetch_universe_fdr), ("pykrx", fetch_universe_pykrx)):
        try:
            df = fn()
            if df is not None and len(df) > 100:
                return df, label
            errors.append(f"{label}: 종목수 부족({len(df) if df is not None else 0})")
        except Exception as e:
            errors.append(f"{label}: {e}")
    raise RuntimeError("유니버스 수집 실패 — " + " | ".join(errors))


# ═══════════════════════════════════════════════════════════
#  Layer 1 — 하드컷 필터 (순수 함수)
# ═══════════════════════════════════════════════════════════
def apply_hardcut(df: pd.DataFrame, rules: Optional[dict] = None) -> tuple[pd.DataFrame, dict]:
    """
    투자 부적격 종목을 기계적으로 제거.
    Returns: (통과 DataFrame, 단계별 탈락 집계 dict)
    ※ 순수 함수 — 네트워크 없이 테스트 가능
    """
    r = {**HARDCUT, **(rules or {})}
    stats = {"input": len(df)}
    if df is None or len(df) == 0:
        return df, {**stats, "output": 0}

    cur = df.copy()

    if r["exclude_preferred"]:
        before = len(cur)
        mask = ~cur.apply(lambda x: _is_preferred_stock(x["name"], x["code"]), axis=1)
        cur = cur[mask]
        stats["drop_preferred"] = before - len(cur)

    if r["exclude_spac"]:
        before = len(cur)
        cur = cur[~cur["name"].apply(_is_spac)]
        stats["drop_spac"] = before - len(cur)

    before = len(cur)
    cur = cur[cur["marcap"].notna() & (cur["marcap"] >= r["min_market_cap"])]
    stats["drop_marcap"] = before - len(cur)

    # 거래대금은 소스에 따라 없을 수 있음 → 있을 때만 적용
    if cur["trade_amount"].notna().any():
        before = len(cur)
        cur = cur[cur["trade_amount"].isna() | (cur["trade_amount"] >= r["min_turnover"])]
        stats["drop_turnover"] = before - len(cur)
    else:
        stats["drop_turnover"] = 0

    before = len(cur)
    cur = cur[cur["per"].notna() & (cur["per"] > r["per_min"]) & (cur["per"] <= r["per_max"])]
    stats["drop_per"] = before - len(cur)

    before = len(cur)
    cur = cur[cur["pbr"].notna() & (cur["pbr"] > r["pbr_min"])]
    stats["drop_pbr"] = before - len(cur)

    stats["output"] = len(cur)
    return cur.reset_index(drop=True), stats


# ═══════════════════════════════════════════════════════════
#  Layer 2 — 팩터 계산 (순수 함수)
# ═══════════════════════════════════════════════════════════
def rank_pct(series: pd.Series, ascending: bool = True) -> pd.Series:
    """
    시리즈를 0~100 백분위 점수로 변환.
    ascending=True  → 값이 클수록 고득점
    ascending=False → 값이 작을수록 고득점
    NaN 은 50점(중립) 처리 — 결측 종목이 부당하게 탈락하지 않도록.
    ※ 순수 함수
    """
    s = pd.to_numeric(series, errors="coerce")
    valid = s.notna()
    out = pd.Series([50.0] * len(s), index=s.index, dtype="float64")
    if valid.sum() == 0:
        return out
    if valid.sum() == 1:
        out[valid] = 50.0
        return out
    ranked = s[valid].rank(ascending=ascending, method="average", pct=True) * 100
    out[valid] = ranked
    return out


def winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """극단값을 백분위로 잘라내 팩터 왜곡 방지. ※ 순수 함수"""
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().sum() < 3:
        return s
    lo, hi = s.quantile(lower), s.quantile(upper)
    return s.clip(lower=lo, upper=hi)


def calc_momentum(prices: pd.Series) -> Optional[float]:
    """
    12-1 모멘텀의 축소판: 6개월 수익률에서 최근 1개월을 제외.
    최근 1개월을 빼는 이유 = 단기 반전효과(short-term reversal) 회피.
    prices: 날짜 오름차순 종가 시리즈 (최소 130 영업일 권장)
    ※ 순수 함수
    """
    p = pd.to_numeric(pd.Series(prices), errors="coerce").dropna()
    n = len(p)
    if n < 40:
        return None

    # [버그수정] 제외 구간 경계 off-by-one
    # 최근 21영업일(인덱스 n-21 ~ n-1)을 '제외'하려면
    # 종점은 그 구간에 포함되지 않는 n-22 여야 한다.
    # 기존 n-21 은 제외 구간의 첫날이라 급등이 그대로 반영됐음.
    EXCLUDE_RECENT = 21        # 최근 1개월 제외
    LOOKBACK       = 126       # 약 6개월
    idx_end   = n - EXCLUDE_RECENT - 1        # 1개월 전 (제외구간 직전)
    idx_start = idx_end - (LOOKBACK - EXCLUDE_RECENT)
    idx_end   = max(0, idx_end)
    idx_start = max(0, idx_start)
    if idx_end <= idx_start:
        return None

    p_start, p_end = float(p.iloc[idx_start]), float(p.iloc[idx_end])
    if p_start <= 0:
        return None
    return (p_end / p_start - 1.0) * 100.0


def calc_flow_score(foreign_net_amt: float, inst_net_amt: float,
                    marcap: float) -> Optional[float]:
    """
    수급 강도 = (외국인 + 기관 순매수 금액) / 시가총액 × 100
    금액 기준으로 정규화해야 대형주·소형주 비교가 가능.
    ※ 순수 함수
    """
    if not marcap or marcap <= 0:
        return None
    if foreign_net_amt is None and inst_net_amt is None:
        return None
    total = (foreign_net_amt or 0) + (inst_net_amt or 0)
    return total / marcap * 100.0


def compute_factor_scores(df: pd.DataFrame,
                          weights: Optional[dict] = None) -> pd.DataFrame:
    """
    4팩터 점수를 계산해 total_score 로 합산.
    입력 df 필수 컬럼: per, pbr, roe, marcap
    선택 컬럼: momentum, flow_raw  (없으면 해당 팩터 50점 중립 처리)
    ※ 순수 함수 — 네트워크 없이 테스트 가능
    """
    w = {**FACTOR_WEIGHTS, **(weights or {})}
    total_w = sum(w.values())
    if not math.isclose(total_w, 1.0, abs_tol=1e-6):
        raise ValueError(f"팩터 가중치 합이 1.0이 아닙니다: {total_w}")

    if df is None or len(df) == 0:
        return df

    out = df.copy()

    # ── 가치: 저PER + 저PBR (작을수록 고득점) ──
    per_s = rank_pct(winsorize(out["per"]), ascending=False)
    pbr_s = rank_pct(winsorize(out["pbr"]), ascending=False)
    out["score_value"] = (per_s + pbr_s) / 2

    # ── 퀄리티: ROE (클수록 고득점) ──
    out["score_quality"] = rank_pct(winsorize(out["roe"]), ascending=True)

    # ── 모멘텀 ──
    if "momentum" in out.columns:
        out["score_momentum"] = rank_pct(winsorize(out["momentum"]), ascending=True)
    else:
        out["score_momentum"] = 50.0

    # ── 수급 ──
    if "flow_raw" in out.columns:
        out["score_flow"] = rank_pct(winsorize(out["flow_raw"]), ascending=True)
    else:
        out["score_flow"] = 50.0

    out["total_score"] = (
        w["value"]    * out["score_value"] +
        w["quality"]  * out["score_quality"] +
        w["momentum"] * out["score_momentum"] +
        w["flow"]     * out["score_flow"]
    ).round(2)

    for c in ("score_value", "score_quality", "score_momentum", "score_flow"):
        out[c] = out[c].round(1)

    return out.sort_values("total_score", ascending=False).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════
#  Layer 2 보조 — 후보군에만 비싼 데이터 수집
# ═══════════════════════════════════════════════════════════
def enrich_momentum(df: pd.DataFrame, max_stocks: int = 400,
                    price_fetcher=None) -> pd.DataFrame:
    """
    하드컷 통과 종목에만 가격 이력을 받아 모멘텀 계산.
    price_fetcher: (ticker) -> pd.Series(종가)  — 테스트 시 주입 가능
    """
    if df is None or len(df) == 0:
        return df

    fetcher = price_fetcher or _default_price_fetcher
    target  = df.head(max_stocks).copy()
    moms    = []
    for code in target["code"]:
        try:
            moms.append(calc_momentum(fetcher(code)))
        except Exception:
            moms.append(None)
    target["momentum"] = moms

    rest = df.iloc[len(target):].copy()
    if len(rest) > 0:
        rest["momentum"] = None
        return pd.concat([target, rest], ignore_index=True)
    return target


def _default_price_fetcher(ticker: str) -> pd.Series:
    if not HAS_FDR:
        return pd.Series(dtype="float64")
    start = today_kst() - datetime.timedelta(days=260)
    df = fdr.DataReader(ticker, start=start)
    if df is None or df.empty or "Close" not in df.columns:
        return pd.Series(dtype="float64")
    return df["Close"].dropna()


def enrich_flow(df: pd.DataFrame, days: int = 20,
                flow_fetcher=None) -> pd.DataFrame:
    """
    수급 데이터 부착. flow_fetcher 는 전종목 dict 를 한 번에 반환해야 함.
    fetcher() -> {code: {"foreign": 금액, "institution": 금액}}
    """
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    try:
        flows = (flow_fetcher or _default_flow_fetcher)(days)
    except Exception:
        flows = {}

    if not flows:
        out["flow_raw"] = None
        return out

    vals = []
    for code, cap in zip(out["code"], out["marcap"]):
        f = flows.get(code)
        vals.append(calc_flow_score(f.get("foreign"), f.get("institution"), cap) if f else None)
    out["flow_raw"] = vals
    return out


def _default_flow_fetcher(days: int = 20) -> dict:
    """
    pykrx 전종목 순매수 (KRX_ID/KRX_PW 있을 때만 동작).
    실패 시 빈 dict → flow 팩터는 전 종목 50점 중립 처리.
    """
    if not HAS_PYKRX:
        return {}
    end   = today_kst()
    start = end - datetime.timedelta(days=days * 2)
    fd, td = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    result: dict = {}
    for investor, key in (("외국인", "foreign"), ("기관합계", "institution")):
        for market in ("KOSPI", "KOSDAQ"):
            try:
                df = pykrx_stock.get_market_net_purchases_of_equities(
                    fd, td, market, investor)
                if df is None or df.empty:
                    continue
                amt_col = next((c for c in df.columns if "순매수거래대금" in str(c)), None)
                if amt_col is None:
                    amt_col = next((c for c in df.columns if "거래대금" in str(c)), None)
                if amt_col is None:
                    continue
                for code, amt in zip(df.index.astype(str), df[amt_col]):
                    result.setdefault(code.zfill(6), {})[key] = float(amt)
            except Exception:
                continue
    return result


# ═══════════════════════════════════════════════════════════
#  통합 파이프라인
# ═══════════════════════════════════════════════════════════
def run_screening(top_n: int = 15,
                  momentum_pool: int = 400,
                  weights: Optional[dict] = None,
                  universe_df: Optional[pd.DataFrame] = None,
                  price_fetcher=None,
                  flow_fetcher=None) -> dict:
    """
    Layer 0 → 1 → 2 전체 실행.
    universe_df/price_fetcher/flow_fetcher 를 주입하면 네트워크 없이 테스트 가능.

    Returns: {
      "top": DataFrame(top_n),  "all": DataFrame(전체 스코어),
      "stats": dict,            "source": str,
      "as_of": "YYYY-MM-DD",    "error": str|None
    }
    """
    meta = {"as_of": today_kst().strftime("%Y-%m-%d"), "error": None}

    try:
        if universe_df is not None:
            uni, source = normalize_universe(universe_df, source="injected"), "injected"
        else:
            uni, source = fetch_universe()
    except Exception as e:
        return {"top": pd.DataFrame(), "all": pd.DataFrame(),
                "stats": {}, "source": None, **meta, "error": str(e)}

    filtered, stats = apply_hardcut(uni)
    if len(filtered) == 0:
        return {"top": pd.DataFrame(), "all": pd.DataFrame(),
                "stats": stats, "source": source, **meta,
                "error": "하드컷 통과 종목 0개 — 기준을 완화하세요."}

    # 1차 예비 랭킹 → 상위 pool 에만 비싼 데이터 수집
    prelim = compute_factor_scores(filtered, weights)
    prelim = enrich_momentum(prelim, max_stocks=momentum_pool, price_fetcher=price_fetcher)
    prelim = enrich_flow(prelim, flow_fetcher=flow_fetcher)
    final  = compute_factor_scores(prelim, weights)

    stats["momentum_collected"] = int(pd.to_numeric(
        final.get("momentum"), errors="coerce").notna().sum()) if "momentum" in final else 0
    stats["flow_collected"] = int(pd.to_numeric(
        final.get("flow_raw"), errors="coerce").notna().sum()) if "flow_raw" in final else 0

    return {"top": final.head(top_n).copy(), "all": final,
            "stats": stats, "source": source, **meta}


def build_candidate_context(top_df: pd.DataFrame, as_of: str = "") -> str:
    """
    AI(Layer 3)에게 전달할 후보 텍스트 생성.
    ※ 순수 함수
    """
    if top_df is None or len(top_df) == 0:
        return "(스크리닝 후보 없음 — 정량 필터를 통과한 종목이 없습니다)"

    lines = [
        f"※ 정량 스크리닝 결과 — 기준일 {as_of}",
        "※ 전종목에서 하드컷·4팩터 랭킹을 통과한 상위 후보입니다.",
        "※ 점수는 0~100 백분위. 이 목록 밖의 종목은 추천 금지.",
        "",
        "순위 | 종목명(코드) | 종합 | 가치 | 퀄리티 | 모멘텀 | 수급 | 현재가 | PER | PBR | ROE",
        "-" * 100,
    ]
    for i, r in enumerate(top_df.itertuples(), 1):
        def g(attr, default=None):
            v = getattr(r, attr, default)
            return v if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
        close = g("close"); per = g("per"); pbr = g("pbr"); roe = g("roe")
        lines.append(
            f"{i:>2} | {getattr(r,'name','')}({getattr(r,'code','')}) "
            f"| {g('total_score',0):.1f} "
            f"| {g('score_value',0):.0f} | {g('score_quality',0):.0f} "
            f"| {g('score_momentum',0):.0f} | {g('score_flow',0):.0f} "
            f"| {int(close):,}원 " if close else "| N/A "
        )
        lines[-1] += (f"| {per:.1f} " if per else "| N/A ")
        lines[-1] += (f"| {pbr:.2f} " if pbr else "| N/A ")
        lines[-1] += (f"| {roe:.1f}%" if roe else "| N/A")
    return "\n".join(lines)
