"""
=============================================================
 mrham_dart.py — DART 전자공시 실시간 알림
=============================================================
 개인이 기관과 '동시에' 받을 수 있는 유일한 원천 정보 채널.
 유상증자·대주주매도·실적정정 등은 발표 즉시 주가에 반영되므로
 수급 데이터(T-1)보다 훨씬 빠른 신호.

 API 키 발급: https://opendart.fss.or.kr  (무료, 일 20,000건)
=============================================================
"""

from __future__ import annotations

import datetime
import re
from typing import Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

KST = datetime.timezone(datetime.timedelta(hours=9))
DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"

# [v11.1] DART 호출이 앱 전체를 블로킹하지 않도록 하는 상한값
MAX_CORP_QUERIES   = 20   # 한 번에 조회할 최대 종목 수
PER_REQUEST_TIMEOUT = 4   # 개별 요청 타임아웃(초)
TOTAL_TIMEOUT      = 12   # 전체 수집 상한(초) — 초과 시 부분 결과로 진행


def today_kst() -> datetime.date:
    return datetime.datetime.now(KST).date()


# ═══════════════════════════════════════════════════════════
#  공시 영향도 분류 규칙
# ═══════════════════════════════════════════════════════════
# (키워드 튜플, 등급, 방향, 설명)
DISCLOSURE_RULES = [
    # ── 강한 악재 ──
    (("유상증자",),                 "high", "negative", "지분 희석 — 단기 주가 압박"),
    (("전환사채", "신주인수권부사채", "교환사채"),
                                    "high", "negative", "잠재 물량 출회 가능성"),
    (("최대주주변경",),             "high", "negative", "지배구조 불확실성"),
    (("감사보고서" ,"의견거절", "한정"),
                                    "high", "negative", "회계 리스크 — 상장폐지 사유 가능"),
    (("횡령", "배임"),              "high", "negative", "경영 리스크 — 거래정지 가능"),
    (("소송",),                     "normal", "negative", "우발채무 가능성"),
    (("주식소각취소", "상장폐지"),  "high", "negative", "심각한 하방 리스크"),
    # ── 강한 호재 ──
    (("자기주식취득", "자사주매입"), "high", "positive", "주주환원 — 수급 개선"),
    (("주식소각",),                 "high", "positive", "주당가치 상승"),
    (("무상증자",),                 "normal", "positive", "유동성 개선 기대"),
    (("단일판매", "공급계약"),      "high", "positive", "실적 가시성 개선"),
    (("현금ㆍ현물배당", "현금·현물배당", "배당"),
                                    "normal", "positive", "주주환원"),
    # ── 중립·확인 필요 ──
    (("영업정지", "생산중단"),      "high", "negative", "실적 직접 타격"),
    (("특별관계자", "임원ㆍ주요주주", "임원·주요주주"),
                                    "normal", "neutral", "내부자 거래 — 방향 확인 필요"),
    (("분기보고서", "반기보고서", "사업보고서"),
                                    "normal", "neutral", "정기 실적 공시"),
    (("정정",),                     "normal", "neutral", "기존 공시 수정 — 내용 확인 필요"),
]


def classify_disclosure(report_name: str) -> dict:
    """
    공시 제목으로 영향도·방향 분류.
    Returns: {"impact": "high"|"normal", "direction": "positive"|"negative"|"neutral",
              "reason": str, "matched": str|None}
    ※ 순수 함수 — 네트워크 없이 테스트 가능
    """
    name = str(report_name or "")
    clean = re.sub(r"\s+", "", name)
    for keywords, impact, direction, reason in DISCLOSURE_RULES:
        for kw in keywords:
            if re.sub(r"\s+", "", kw) in clean:
                return {"impact": impact, "direction": direction,
                        "reason": reason, "matched": kw}
    return {"impact": "low", "direction": "neutral",
            "reason": "일반 공시", "matched": None}


def fetch_disclosures(api_key: str, corp_codes: Optional[list] = None,
                      days: int = 3, session=None) -> dict:
    """
    최근 N일 공시 조회.
    corp_codes 가 None 이면 전체 공시(최대 100건), 있으면 해당 기업만.

    Returns: {"items": [...], "status": "ok"|"no_key"|"error", "message": str}
    """
    if not api_key:
        return {"items": [], "status": "no_key",
                "message": "DART API 키 미설정 — opendart.fss.or.kr 에서 무료 발급"}
    if not HAS_REQUESTS:
        return {"items": [], "status": "error", "message": "requests 미설치"}

    end   = today_kst()
    start = end - datetime.timedelta(days=days)
    params = {
        "crtfc_key":  api_key,
        "bgn_de":     start.strftime("%Y%m%d"),
        "end_de":     end.strftime("%Y%m%d"),
        "page_count": 100,
        "page_no":    1,
    }

    sess = session or requests
    items: list = []
    try:
        if corp_codes:
            # [v11.1 성능수정] 순차 호출 → 병렬 호출
            #   기존: 20종목 × timeout 8초 = 최대 160초 블로킹
            #        → Streamlit 스크립트가 멈추고 주가 캐시가 만료·축출되어
            #          포트폴리오 현재가가 '미수집'으로 표시되는 부작용 발생
            #   변경: ThreadPool 병렬 + 개별 4초 + 전체 12초 상한
            from concurrent.futures import ThreadPoolExecutor, as_completed

            targets = list(corp_codes)[:MAX_CORP_QUERIES]

            def _one(cc):
                try:
                    p = {**params, "corp_code": cc}
                    r = sess.get(DART_LIST_URL, params=p, timeout=PER_REQUEST_TIMEOUT)
                    if r.status_code != 200:
                        return []
                    d = r.json()
                    if d.get("status") == "000":
                        return d.get("list", [])
                    return []
                except Exception:
                    return []

            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = [ex.submit(_one, cc) for cc in targets]
                try:
                    for f in as_completed(futs, timeout=TOTAL_TIMEOUT):
                        items.extend(f.result() or [])
                except Exception:
                    # 전체 상한 초과 → 지금까지 모은 것만 사용 (앱은 계속 진행)
                    for f in futs:
                        f.cancel()
        else:
            r = sess.get(DART_LIST_URL, params=params, timeout=8)
            if r.status_code != 200:
                return {"items": [], "status": "error",
                        "message": f"HTTP {r.status_code}"}
            d = r.json()
            if d.get("status") == "000":
                items = d.get("list", [])
            elif d.get("status") == "013":
                items = []
            else:
                return {"items": [], "status": "error",
                        "message": f"DART 오류 {d.get('status')}: {d.get('message')}"}
    except Exception as e:
        return {"items": [], "status": "error", "message": str(e)}

    return {"items": items, "status": "ok", "message": f"{len(items)}건 수집"}


def analyze_disclosures(items: list, my_tickers: Optional[set] = None) -> list:
    """
    공시 목록을 분류·정렬. 보유 종목 우선, 그다음 영향도 순.
    ※ 순수 함수 — 네트워크 없이 테스트 가능
    """
    if not items:
        return []
    mine = my_tickers or set()
    out = []
    for it in items:
        # [버그수정] 빈 문자열을 zfill 하면 "000000" 이 되어
        # 비상장 기업 공시가 유효 종목으로 통과했음 → zfill 전에 검증
        raw = str(it.get("stock_code") or "").strip()
        if not raw or not raw.isdigit():
            continue
        code = raw.zfill(6)
        if not re.match(r"^\d{6}$", code):
            continue
        cls = classify_disclosure(it.get("report_nm", ""))
        out.append({
            "code":       code,
            "corp_name":  it.get("corp_name", ""),
            "report_nm":  it.get("report_nm", ""),
            "rcept_dt":   it.get("rcept_dt", ""),
            "rcept_no":   it.get("rcept_no", ""),
            "url":        f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={it.get('rcept_no','')}",
            "is_mine":    code in mine,
            **cls,
        })
    impact_rank    = {"high": 0, "normal": 1, "low": 2}
    direction_rank = {"negative": 0, "positive": 1, "neutral": 2}
    out.sort(key=lambda x: (
        not x["is_mine"],
        impact_rank.get(x["impact"], 9),
        direction_rank.get(x["direction"], 9),
        -int(x["rcept_dt"] or 0),
    ))
    return out


def build_disclosure_context(analyzed: list, max_items: int = 15) -> str:
    """AI 프롬프트용 공시 텍스트. ※ 순수 함수"""
    if not analyzed:
        return "(최근 공시 없음 또는 DART 미연동)"
    lines = ["※ DART 전자공시 — 보유 종목 우선, 영향도 순 정렬", ""]
    for d in analyzed[:max_items]:
        tag  = "★보유" if d["is_mine"] else "  "
        mark = {"negative": "▼악재", "positive": "▲호재", "neutral": "◎중립"}[d["direction"]]
        lv   = {"high": "[강]", "normal": "[중]", "low": "[약]"}[d["impact"]]
        lines.append(f"{tag} {mark}{lv} {d['corp_name']}({d['code']}) "
                     f"| {d['report_nm']} | {d['rcept_dt']} | {d['reason']}")
    return "\n".join(lines)


def get_critical_alerts(analyzed: list) -> list:
    """
    보유 종목의 강한 악재만 추출 — AI 분석 없이 즉시 경고용.
    ※ 순수 함수
    """
    return [d for d in analyzed
            if d["is_mine"] and d["impact"] == "high" and d["direction"] == "negative"]


# ═══════════════════════════════════════════════════════════
#  종목코드 → DART corp_code 매핑
# ═══════════════════════════════════════════════════════════
def build_corp_code_map(api_key: str, session=None) -> dict:
    """
    DART corpCode.xml (ZIP) 을 받아 {종목코드: corp_code} 매핑 생성.
    호출 비용이 크므로 앱에서 하루 1회만 캐싱할 것.
    """
    if not api_key or not HAS_REQUESTS:
        return {}
    import io
    import zipfile
    import xml.etree.ElementTree as ET

    url  = "https://opendart.fss.or.kr/api/corpCode.xml"
    sess = session or requests
    try:
        r = sess.get(url, params={"crtfc_key": api_key}, timeout=25)
        if r.status_code != 200:
            return {}

        # [v11.1 메모리수정] ET.fromstring 은 10만 기업 전체를 DOM 으로 올려
        # 수백 MB 를 점유 → Streamlit Cloud 에서 캐시 축출·OOM 유발.
        # iterparse 로 스트리밍 파싱하고 처리한 노드를 즉시 해제한다.
        # 상장사(stock_code 보유)는 약 2,800건뿐이므로 결과 dict 는 작다.
        mapping: dict = {}
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            with zf.open(zf.namelist()[0]) as fp:
                for event, elem in ET.iterparse(fp, events=("end",)):
                    if elem.tag != "list":
                        continue
                    sc = (elem.findtext("stock_code") or "").strip()
                    cc = (elem.findtext("corp_code") or "").strip()
                    # 상장 종목코드: 6자리 숫자 또는 영숫자(신형 ETF/ETN)
                    if sc and cc and re.match(r"^[0-9A-Z]{6}$", sc.upper()):
                        mapping[sc.upper()] = cc
                    elem.clear()          # 처리한 노드 즉시 해제 (핵심)
        return mapping
    except Exception:
        return {}
