"""
=============================================================
  📈 Mr.Ham — 24Hr AI 포트폴리오 매니저 v9.0
=============================================================
  🔐 보안 정책
  · Gemini API 키는 절대 서버/DB에 저장되지 않습니다.
  · 로그인 후 매 세션마다 키를 직접 입력합니다.
  · 브라우저 탭을 닫으면 키는 즉시 사라집니다.
  · 포트폴리오 데이터만 Supabase에 암호화 저장됩니다.
=============================================================
"""

import streamlit as st
import streamlit.components.v1 as components
import datetime, uuid, time, re, warnings
import requests

_http = requests.Session()
_http.headers.update({"User-Agent": "Mozilla/5.0"})
warnings.filterwarnings("ignore", category=DeprecationWarning, module="google.generativeai")

try:
    from supabase import create_client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

try:
    import google.generativeai as genai
except ImportError:
    raise ImportError("pip install google-generativeai 를 실행해주세요.")

try:
    import FinanceDataReader as fdr
    HAS_FDR = True
except ImportError:
    HAS_FDR = False; fdr = None

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


# ═══════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════
ACC_MAP = {
    "💼 일반주식계좌": "general_acc",
    "🛡️ ISA계좌":     "isa_acc",
    "🏢 퇴직연금":    "pension_acc",
    "💰 연금저축":    "savings_acc",
}
ACC_KEYS = list(ACC_MAP.keys())

NEWS_FEEDS = {
    "한국경제":     "https://www.hankyung.com/feed/all-news",
    "매일경제":     "https://www.mk.co.kr/rss/30100041/",
    "연합뉴스경제": "https://www.yna.co.kr/RSS/economy.xml",
    "머니투데이":   "https://rss.mt.co.kr/mt_top_news_rss.xml",
    "Reuters":      "https://feeds.reuters.com/reuters/businessNews",
}

MARKET_TICKERS = {
    "KOSPI": "^KS11", "KOSDAQ": "^KQ11", "S&P 500": "^GSPC",
    "나스닥 100": "^NDX", "원/달러": "KRW=X", "WTI 유가": "CL=F",
    "금(Gold)": "GC=F", "VIX": "^VIX", "달러인덱스": "DX-Y.NYB",
}

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com",
    "Accept": "application/json",
}

STAGE1_MAX_TOKENS       = 3000
STAGE2_MAX_TOKENS       = 8192
CONTINUATION_MAX_TOKENS = 4000


# ═══════════════════════════════════════════════════════════
#  Supabase 클라이언트
# ═══════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    if not HAS_SUPABASE:
        return None
    try:
        return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════
#  데이터 영속성 — 포트폴리오만 저장 (API 키 저장 없음)
# ═══════════════════════════════════════════════════════════
def _default_portfolio() -> dict:
    return {k: {} for k in ACC_MAP.values()}


def load_portfolio() -> dict:
    """Supabase에서 포트폴리오 로드 (API 키는 절대 저장/로드 안 함)"""
    default = _default_portfolio()
    sb = get_supabase()
    if not sb or not st.session_state.get("user"):
        return default
    try:
        user_id = st.session_state.user.id
        row = sb.table("portfolios").select("data").eq("user_id", user_id).execute()
        if not row.data:
            return default
        data = row.data[0].get("data") or default
        # 마이그레이션: ticker 필드 없는 구버전 호환
        for acc_key in default:
            if acc_key not in data:
                data[acc_key] = {}
            else:
                migrated = {}
                for k, v in data[acc_key].items():
                    if "ticker" not in v:
                        v["ticker"] = k
                        migrated[str(uuid.uuid4())] = v
                    else:
                        migrated[k] = v
                data[acc_key] = migrated
        return data
    except Exception as e:
        st.warning(f"데이터 로드 오류: {e}")
        return default


def save_portfolio(data: dict) -> None:
    """포트폴리오를 Supabase에 저장"""
    sb = get_supabase()
    if not sb or not st.session_state.get("user"):
        return
    try:
        user_id = st.session_state.user.id
        
        # 1. 기존 데이터가 있는지 확인
        existing = sb.table("portfolios").select("id").eq("user_id", user_id).execute()
        
        if existing.data:
            # 2. 이미 있으면 업데이트 (Update)
            sb.table("portfolios").update({
                "data":       data,
                "updated_at": datetime.datetime.now().isoformat(),
            }).eq("user_id", user_id).execute()
        else:
            # 3. 없으면 새로 만들기 (Insert)
            sb.table("portfolios").insert({
                "user_id":    user_id,
                "data":       data,
                "updated_at": datetime.datetime.now().isoformat(),
            }).execute()
            
    except Exception as e:
        st.error(f"❌ 저장 오류: {e}")


# ═══════════════════════════════════════════════════════════
#  로그인 / 회원가입 페이지
# ═══════════════════════════════════════════════════════════
def show_auth_page():
    st.markdown("""
    <div style='text-align:center;padding:50px 0 20px'>
        <div style='font-size:56px'>📈</div>
        <h1 style='font-size:30px;margin:12px 0 6px'>Mr.Ham AI 포트폴리오 매니저</h1>
        <p style='color:#888;font-size:15px'>나만의 AI 투자 분석 비서 · 어디서나 접속</p>
    </div>
    """, unsafe_allow_html=True)

    sb = get_supabase()
    if not sb:
        st.error("⚠️ Supabase 연결 실패 — SETUP_GUIDE.md 를 참고해주세요.")
        return

    _, col, _ = st.columns([1, 2, 1])
    with col:
        tab1, tab2, tab3 = st.tabs(["🔐 로그인", "📝 회원가입", "🔑 비밀번호 재설정"])

        with tab1:
            st.markdown("<br>", unsafe_allow_html=True)
            email = st.text_input("이메일", placeholder="example@email.com", key="li_email")
            pw    = st.text_input("비밀번호", type="password", key="li_pw")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("로그인", type="primary", use_container_width=True, key="btn_li"):
                if not email or not pw:
                    st.warning("이메일과 비밀번호를 입력해주세요.")
                else:
                    with st.spinner("로그인 중..."):
                        try:
                            resp = sb.auth.sign_in_with_password({"email": email, "password": pw})
                            st.session_state.user = resp.user
                            st.rerun()
                        except Exception as e:
                            msg = str(e).lower()
                            if "invalid" in msg or "credentials" in msg:
                                st.error("❌ 이메일 또는 비밀번호가 올바르지 않습니다.")
                            elif "confirm" in msg:
                                st.warning("📧 이메일 인증이 필요합니다. 받은 메일함을 확인해주세요.")
                            else:
                                st.error(f"❌ 로그인 실패: {e}")

        with tab2:
            st.markdown("<br>", unsafe_allow_html=True)
            su_email = st.text_input("이메일", placeholder="example@email.com", key="su_email")
            su_pw1   = st.text_input("비밀번호 (8자 이상)", type="password", key="su_pw1")
            su_pw2   = st.text_input("비밀번호 확인",       type="password", key="su_pw2")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("회원가입", type="primary", use_container_width=True, key="btn_su"):
                if not su_email or not su_pw1:
                    st.warning("모든 항목을 입력해주세요.")
                elif len(su_pw1) < 8:
                    st.warning("비밀번호는 8자 이상이어야 합니다.")
                elif su_pw1 != su_pw2:
                    st.error("❌ 비밀번호가 일치하지 않습니다.")
                else:
                    with st.spinner("가입 중..."):
                        try:
                            sb.auth.sign_up({"email": su_email, "password": su_pw1})
                            st.success("✅ 가입 완료! 📧 인증 메일을 확인하고 로그인해주세요.")
                        except Exception as e:
                            st.error("❌ 이미 가입된 이메일입니다." if "already" in str(e).lower() else f"❌ {e}")

        with tab3:
            st.markdown("<br>", unsafe_allow_html=True)
            r_email = st.text_input("가입한 이메일", key="r_email")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("재설정 메일 발송", use_container_width=True, key="btn_r"):
                if r_email:
                    try:
                        sb.auth.reset_password_email(r_email)
                        st.success("✅ 비밀번호 재설정 메일을 발송했습니다.")
                    except Exception as e:
                        st.error(f"❌ {e}")

    st.markdown("<div style='text-align:center;margin-top:40px;color:#bbb;font-size:12px'>⚠️ 투자 참고용 서비스입니다. 투자 결정의 책임은 본인에게 있습니다.</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
#  ★ API 키 입력 화면 (세션에만 유지, 절대 저장 안 함)
# ═══════════════════════════════════════════════════════════
def show_api_key_page():
    """로그인 후, API 키 미입력 상태일 때 표시되는 화면"""
    user_email = st.session_state.user.email

    st.markdown(f"""
    <div style='text-align:center;padding:40px 0 10px'>
        <div style='font-size:48px'>🔑</div>
        <h2 style='margin:12px 0 6px'>Gemini API 키 입력</h2>
        <p style='color:#888;font-size:14px'>👤 {user_email} 로그인됨</p>
    </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:
        # 보안 안내 박스
        st.markdown("""
        <div style='background:#f0f7ff;border-radius:12px;padding:16px 20px;margin-bottom:20px;
                    border-left:4px solid #1c83e1;font-size:14px;line-height:1.8'>
            🛡️ <b>개인정보 보호 안내</b><br>
            · API 키는 <b>서버나 DB에 저장되지 않습니다</b><br>
            · 이 브라우저 세션에만 임시로 유지됩니다<br>
            · 탭을 닫거나 로그아웃하면 즉시 사라집니다<br>
            · 포트폴리오 데이터만 내 계정에 저장됩니다
        </div>
        """, unsafe_allow_html=True)

        api_key_input = st.text_input(
            "Gemini API Key",
            type="password",
            placeholder="AIzaSy...",
            help="Google AI Studio에서 발급한 API 키를 입력하세요",
        )
        st.caption("👉 키 발급: [aistudio.google.com](https://aistudio.google.com) → Get API key → Create API key (무료)")

        st.markdown("<br>", unsafe_allow_html=True)
        col_ok, col_lo = st.columns(2)

        with col_ok:
            if st.button("✅ 확인 및 시작", type="primary", use_container_width=True):
                if not api_key_input:
                    st.warning("API 키를 입력해주세요.")
                elif not api_key_input.startswith("AIza"):
                    st.error("❌ 올바른 Gemini API 키 형식이 아닙니다. (AIza... 로 시작)")
                else:
                    with st.spinner("키 검증 중..."):
                        try:
                            models = get_available_models(api_key_input)
                            # 세션 메모리에만 저장 (DB 저장 없음)
                            st.session_state.api_key          = api_key_input
                            st.session_state.api_key_verified = True
                            st.session_state.available_models = models
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ 유효하지 않은 API 키입니다.\n\n{e}")

        with col_lo:
            if st.button("🚪 로그아웃", use_container_width=True):
                _do_logout()

    st.markdown("""
    <div style='text-align:center;margin-top:40px;color:#bbb;font-size:12px'>
        API 키를 매번 입력하는 것이 번거롭다면 브라우저의 비밀번호 자동완성을 활용해보세요.
    </div>
    """, unsafe_allow_html=True)


def _do_logout():
    sb = get_supabase()
    if sb:
        try: sb.auth.sign_out()
        except: pass
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    st.rerun()


# ═══════════════════════════════════════════════════════════
#  뉴스 & 시장 지수
# ═══════════════════════════════════════════════════════════
@st.cache_data(ttl=600)
def fetch_realtime_news(max_per_feed: int = 4) -> list:
    if not HAS_FEEDPARSER: return []
    articles = []
    for source, url in NEWS_FEEDS.items():
        try:
            resp = _http.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
            feed = feedparser.parse(resp.content)
            for entry in feed.entries[:max_per_feed]:
                title   = entry.get("title", "").strip()
                summary = re.sub(r"<[^>]+>", "", entry.get("summary", "")).strip()[:120]
                if title: articles.append({"source": source, "title": title, "summary": summary})
        except: continue
    return articles[:35]


@st.cache_data(ttl=300)
def fetch_market_indices() -> dict:
    result = {}
    if not HAS_YFINANCE: return result
    for name, ticker in MARKET_TICKERS.items():
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if len(hist) >= 2:
                prev, curr = hist["Close"].iloc[-2], hist["Close"].iloc[-1]
                chg = curr - prev; pct = chg / prev * 100
                result[name] = {"current": round(curr,2), "change": round(chg,2), "pct": round(pct,2)}
            elif len(hist) == 1:
                result[name] = {"current": round(hist["Close"].iloc[-1],2), "change": 0, "pct": 0}
        except: continue
    return result


# ═══════════════════════════════════════════════════════════
#  기업 펀더멘털 — Naver Finance
# ═══════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def fetch_naver_fundamentals(ticker: str) -> dict:
    result: dict = {}
    if not ticker: return result
    if not re.match(r"^\d{6}$", ticker):
        if HAS_YFINANCE:
            try:
                info = yf.Ticker(ticker).info
                result["per"]           = info.get("trailingPE") or info.get("forwardPE")
                result["pbr"]           = info.get("priceToBook")
                roe_raw                 = info.get("returnOnEquity")
                result["roe"]           = round(roe_raw*100,2) if roe_raw else None
                result["eps"]           = info.get("trailingEps")
                mktcap                  = info.get("marketCap",0) or 0
                result["market_cap_억"] = int(mktcap/1e8) if mktcap else None
                result["sector"]        = info.get("sector","")
                div_raw                 = info.get("dividendYield")
                result["div_yield"]     = round(div_raw*100,2) if div_raw else None
            except: pass
        return result

    SKIP = {"","-","--","n/a","해당없음","해당 없음","적자","흑자전환","전환"}
    def sf(raw):
        c = re.sub(r"[,배원원%x배\s]","",str(raw)).strip()
        if c.lower() in SKIP: return None
        try: return float(c)
        except: return None

    def parse_infos(infos):
        for item in infos:
            if not isinstance(item,dict): continue
            label    = str(item.get("label","") or item.get("title","") or "")
            code     = str(item.get("code","")  or "")
            val      = str(item.get("value","") or item.get("data","") or "")
            combined = (label+" "+code).upper()
            for key, field in {"PER":"per","PBR":"pbr","ROE":"roe","EPS":"eps"}.items():
                if key in combined and result.get(field) is None: result[field] = sf(val)
            if ("시가총액" in label or "MARKET" in combined) and result.get("market_cap_억") is None:
                d = re.sub(r"[^\d]","",val)
                if d: result["market_cap_억"] = int(d)

    def try_url(url):
        try:
            r = _http.get(url, headers=NAVER_HEADERS, timeout=8)
            return r.json() if r.status_code==200 else None
        except: return None

    def search_all(data, depth=0):
        if depth>3 or not isinstance(data,dict): return
        for v in data.values():
            if isinstance(v,list) and v and isinstance(v[0],dict): parse_infos(v)
            elif isinstance(v,dict): search_all(v, depth+1)

    for ep in ["integration","basic","finance/summary"]:
        d = try_url(f"https://m.stock.naver.com/api/stock/{ticker}/{ep}")
        if d: search_all(d)
        if all(result.get(k) is not None for k in ("per","pbr","roe","eps")): break

    # ── [추가] yfinance로 국내 주식 미래 가치 지표 보완 ──────────────
    # 네이버에서 못 가져온 항목(None)만 채움. .KS → .KQ 순서로 시도
    if HAS_YFINANCE:
        for sfx in (".KS", ".KQ"):
            try:
                info = yf.Ticker(ticker + sfx).info
                # 유효한 종목인지 확인 (빈 응답 방어)
                if not (info.get("regularMarketPrice") or info.get("currentPrice")):
                    continue
                # 기존 지표 보완 (네이버 실패분 백필)
                if result.get("per") is None:
                    result["per"] = info.get("trailingPE") or info.get("forwardPE")
                if result.get("pbr") is None:
                    result["pbr"] = info.get("priceToBook")
                if result.get("roe") is None:
                    roe_r = info.get("returnOnEquity")
                    result["roe"] = round(roe_r*100, 2) if roe_r else None
                if result.get("eps") is None:
                    result["eps"] = info.get("trailingEps")
                if result.get("market_cap_억") is None:
                    mc = info.get("marketCap", 0) or 0
                    result["market_cap_억"] = int(mc/1e8) if mc else None
                # 미래 가치·성장성 지표 (naver에 없음 → yfinance 전담)
                # Forward PER: 예상이익 기반, Trailing PER 보다 미래 지향
                if result.get("forward_per") is None:
                    result["forward_per"] = info.get("forwardPE")
                # PEG: PER ÷ 이익성장률, 1 이하=성장 대비 저평가
                if result.get("peg") is None:
                    result["peg"] = info.get("pegRatio")
                # 매출성장률 YoY (% 단위)
                if result.get("rev_growth") is None:
                    rg = info.get("revenueGrowth")
                    result["rev_growth"] = round(rg*100, 1) if rg else None
                # 부채비율: 낮을수록 재무 건전
                if result.get("debt_equity") is None:
                    result["debt_equity"] = info.get("debtToEquity")
                # 잉여현금흐름 (억원): 양수·클수록 주주환원 여력 높음
                if result.get("fcf_억") is None:
                    fcf = info.get("freeCashflow")
                    result["fcf_억"] = int(fcf/1e8) if fcf else None
                break   # .KS 성공 → .KQ 시도 불필요
            except:
                continue

    return result


@st.cache_data(ttl=600)
def fetch_investor_trend(ticker: str, days: int = 5) -> dict:
    if not ticker or not re.match(r"^\d{5,6}$",ticker): return {}
    try:
        r = _http.get(f"https://m.stock.naver.com/api/stock/{ticker}/investor",
                      headers=NAVER_HEADERS, timeout=8)
        if r.status_code != 200: return {}
        raw = r.json()
        if not isinstance(raw,list): return {}
        def si(v):
            try: return int(str(v).replace(",",""))
            except: return 0
        fn=mn=0
        for row in raw[:days]:
            fn += si(row.get("foreignNetSale", row.get("foreign_net",0)))
            mn += si(row.get("organNetSale",   row.get("institution_net",0)))
        return {"foreign_net":fn,"institution_net":mn,"days":days}
    except: return {}


# ═══════════════════════════════════════════════════════════
#  ETF
# ═══════════════════════════════════════════════════════════
ETF_KEYWORDS = ["KODEX","TIGER","KINDEX","ARIRANG","HANARO","KOSEF","KBSTAR",
                "SOL","ACE","TIMEFOLIO","FOCUS","WOORI","PLUS","SMART","파워","히어로","마이티"]

def is_etf(name: str) -> bool:
    return any(k.upper() in name.upper() for k in ETF_KEYWORDS)


@st.cache_data(ttl=3600)
def fetch_etf_naver_data(ticker: str) -> dict:
    result: dict = {}
    if not ticker or not re.match(r"^\d{6}$",ticker): return result
    SKIP = {"","-","--","n/a","해당없음","해당 없음"}
    def sf(raw):
        c = re.sub(r"[,배원원%x배\s]","",str(raw)).strip()
        if c.lower() in SKIP: return None
        try: return float(c)
        except: return None
    def try_url(url):
        try:
            r = _http.get(url,headers=NAVER_HEADERS,timeout=8)
            return r.json() if r.status_code==200 else None
        except: return None
    LABEL_MAP = [("순자산총액","aum_raw",None),("순자산","aum_raw",None),
                 ("총보수","ter",sf),("보수율","ter",sf),
                 ("추적오차율","tracking_error",sf),("추적오차","tracking_error",sf),
                 ("괴리율","premium_discount",sf),("기준가격","nav",sf),("기준가","nav",sf),
                 ("분배금수익률","div_yield",sf),("배당수익률","div_yield",sf)]
    def parse_infos(infos):
        for item in infos:
            if not isinstance(item,dict): continue
            label = str(item.get("label","") or item.get("title","") or "")
            val   = str(item.get("value","") or item.get("data","") or "")
            for sk,field,tr in LABEL_MAP:
                if sk in label and result.get(field) is None:
                    if tr: result[field] = tr(val)
                    else:
                        d = re.sub(r"[^\d]","",val)
                        if d: result[field] = d
    def parse_obj(obj):
        if not obj: return
        m = {"nav":("nav",sf),"navPrice":("nav",sf),"basePrice":("nav",sf),
             "totalAssets":("aum_raw",None),"netAssets":("aum_raw",None),
             "feeRate":("ter",sf),"totalFeeRate":("ter",sf),
             "trackingError":("tracking_error",sf),"trackingErrorRate":("tracking_error",sf),
             "premiumRate":("premium_discount",sf),"divergenceRate":("premium_discount",sf),
             "indexName":("base_index",None),"baseIndex":("base_index",None)}
        for k,(field,tr) in m.items():
            if k in obj and result.get(field) is None:
                result[field] = tr(str(obj[k])) if tr else obj[k]
    def search_all(data,depth=0):
        if depth>4 or not isinstance(data,dict): return
        for k,v in data.items():
            if isinstance(v,list) and v and isinstance(v[0],dict): parse_infos(v)
            elif isinstance(v,dict):
                if any(x in k.lower() for x in ("etf","fund","info")): parse_obj(v)
                search_all(v,depth+1)
    for ep in ["integration","etf","etfItemInfo","basic"]:
        d = try_url(f"https://m.stock.naver.com/api/stock/{ticker}/{ep}")
        if d: search_all(d)
    raw = result.pop("aum_raw",None)
    if raw and result.get("aum_억") is None:
        d = re.sub(r"[^\d]","",str(raw))
        if d:
            v = int(d)
            result["aum_억"] = v//100_000_000 if v>1_000_000_000_000 else v

    # ── [추가] yfinance로 ETF 성과 지표 보완 ─────────────────────────
    # 네이버 ETF API에 없는 베타·분배율·보수율을 yfinance로 수집
    # .KS → .KQ 순서로 시도, None 항목만 채움
    if HAS_YFINANCE:
        for sfx in (".KS", ".KQ"):
            try:
                info = yf.Ticker(ticker + sfx).info
                if not (info.get("regularMarketPrice") or info.get("currentPrice")):
                    continue
                # 베타: 시장 대비 민감도 (1 초과=시장보다 변동성 큼)
                if result.get("beta") is None:
                    result["beta"] = info.get("beta")
                # 연간 분배율(%): yield 우선, 없으면 trailing dividend yield
                if result.get("etf_yield") is None:
                    yld = info.get("yield") or info.get("trailingAnnualDividendYield")
                    result["etf_yield"] = round(yld*100, 2) if yld else None
                # 보수율(%): naver ter 없을 때 yfinance로 보완
                if result.get("yf_ter") is None:
                    ter_r = info.get("annualReportExpenseRatio")
                    result["yf_ter"] = round(ter_r*100, 4) if ter_r else None
                break
            except:
                continue

    return result


# ═══════════════════════════════════════════════════════════
#  공포·탐욕 지수
# ═══════════════════════════════════════════════════════════
def calculate_fear_greed(indices: dict) -> dict:
    score = 50
    if not indices: return {"score":50,"label":"데이터 없음","color":"#888888"}
    vix = indices.get("VIX",{}).get("current",20)
    if vix<12: score+=25
    elif vix<16: score+=15
    elif vix<20: score+=7
    elif vix<25: score-=5
    elif vix<30: score-=15
    else: score-=25
    score += max(-15,min(15,indices.get("KOSPI",{}).get("pct",0)*4))
    score += max(-10,min(10,indices.get("S&P 500",{}).get("pct",0)*3))
    score -= max(-8, min(8, indices.get("원/달러",{}).get("pct",0)*2))
    if indices.get("금(Gold)",{}).get("pct",0)>0.5: score-=5
    score = max(0,min(100,round(score)))
    if score>=80: lbl,col="극단적 탐욕 😈","#cc2200"
    elif score>=60: lbl,col="탐욕 😊","#ff7700"
    elif score>=40: lbl,col="중립 😐","#888888"
    elif score>=20: lbl,col="공포 😨","#2255cc"
    else: lbl,col="극단적 공포 😱","#001188"
    return {"score":score,"label":lbl,"color":col}


# ═══════════════════════════════════════════════════════════
#  주가 데이터
# ═══════════════════════════════════════════════════════════
def _naver_price(ticker):
    def ti(v): return int(re.sub(r"[^\d]","",str(v))) if v else 0
    for ep in ["basic","integration"]:
        try:
            r = _http.get(f"https://m.stock.naver.com/api/stock/{ticker}/{ep}",
                          headers=NAVER_HEADERS,timeout=8)
            if r.status_code==200:
                d  = r.json()
                sp = d.get("stockPrice") or d.get("stockSummary") or d
                curr = ti(sp.get("closePrice")) or ti(sp.get("currentPrice"))
                if curr>0:
                    high = ti(sp.get("highPrice52Week") or sp.get("high52Week")) or curr
                    low  = ti(sp.get("lowPrice52Week")  or sp.get("low52Week"))  or curr
                    return curr,max(high,curr),(low if low>0 else curr)
        except: pass
    return 0,0,0


@st.cache_data(ttl=300)
def get_stock_data(ticker: str) -> tuple:
    if not ticker: return 0,0,0
    is_domestic = bool(re.match(r"^\d{6}$",ticker))
    if HAS_FDR and fdr:
        try:
            df = fdr.DataReader(ticker,start=datetime.date.today()-datetime.timedelta(days=365))
            if df is not None and not df.empty and "Close" in df.columns:
                c = df["Close"].dropna()
                if len(c)>0: return int(c.iloc[-1]),int(c.max()),int(c.min())
        except: pass
    if is_domestic:
        r = _naver_price(ticker)
        if r[0]>0: return r
    if HAS_YFINANCE:
        for sfx in ([".KS",".KQ"] if is_domestic else [""]):
            try:
                tk   = yf.Ticker(ticker+sfx)
                hist = tk.history(period="5d")
                if not hist.empty and "Close" in hist.columns:
                    c = hist["Close"].dropna()
                    if len(c)>0 and int(c.iloc[-1])>0:
                        curr=int(c.iloc[-1])
                        try: info=tk.info; high=int(info.get("fiftyTwoWeekHigh",curr)); low=int(info.get("fiftyTwoWeekLow",curr))
                        except: high=low=curr
                        return curr,max(high,curr),(low if low>0 else curr)
            except: continue
    return 0,0,0


@st.cache_data(ttl=3600)
def get_moving_averages(ticker: str) -> dict:
    result: dict = {}
    if not ticker: return result
    df = None
    if HAS_FDR and fdr:
        try: df = fdr.DataReader(ticker,start=datetime.date.today()-datetime.timedelta(days=200))
        except: df=None
    if (df is None or df.empty) and HAS_YFINANCE:
        try:
            sfx=".KS" if re.match(r"^\d{6}$",ticker) else ""
            hist=yf.Ticker(ticker+sfx).history(period="200d")
            if not hist.empty: df=hist
        except: pass
    if df is None or df.empty or "Close" not in df.columns: return result
    c = df["Close"].dropna()
    for n,k in [(20,"ma20"),(60,"ma60"),(120,"ma120")]:
        if len(c)>=n: result[k]=int(c.rolling(n).mean().iloc[-1])
    if result.get("ma60") and result["ma60"]>0:
        result["curr_vs_ma60"]=round((int(c.iloc[-1])-result["ma60"])/result["ma60"]*100,1)
    return result


def calc_totals(portfolio: dict) -> tuple:
    cost=val=0
    for acc in portfolio.values():
        for info in acc.values():
            curr,_,_=get_stock_data(info.get("ticker",""))
            if curr==0: curr=info["avg_price"]
            cost+=info["avg_price"]*info["qty"]; val+=curr*info["qty"]
    return cost,val


def calc_portfolio_allocation(portfolio: dict) -> dict:
    total=0; acc_vals={}; de=fe=sv=0
    for lbl,key in ACC_MAP.items():
        av=0
        for info in portfolio.get(key,{}).values():
            curr,_,_=get_stock_data(info.get("ticker",""))
            if curr==0: curr=info.get("avg_price",0)
            iv=curr*info.get("qty",0); av+=iv
            nm=info.get("name","")
            if is_etf(nm):
                if any(k in nm.upper() for k in ["미국","S&P","나스닥","차이나","CSI","채권","달러"]): fe+=iv
                else: de+=iv
            else: sv+=iv
        acc_vals[lbl]=av; total+=av
    if total==0: return {}
    p=lambda v: round(v/total*100,1)
    return {"total_val":total,
            "by_account":{l:{"value":v,"pct":p(v)} for l,v in acc_vals.items() if v>0},
            "domestic_etf_pct":p(de),"foreign_etf_pct":p(fe),"stock_pct":p(sv)}


# ═══════════════════════════════════════════════════════════
#  포트폴리오 UI
# ═══════════════════════════════════════════════════════════
def display_portfolio(title: str, portfolio_dict: dict):
    st.subheader(title)
    if not portfolio_dict:
        st.info("보유 종목이 없습니다.")
        return
    for item_id, info in portfolio_dict.items():
        ticker=info.get("ticker","")
        curr,high_52w,low_52w=get_stock_data(ticker)
        if curr==0: curr=high_52w=low_52w=info["avg_price"]
        cost=info["avg_price"]*info["qty"]; val=curr*info["qty"]; profit=val-cost
        rate=((curr-info["avg_price"])/info["avg_price"]*100) if info["avg_price"]>0 else 0
        color="#ff4b4b" if profit>0 else "#1c83e1" if profit<0 else "#888"
        sign="+" if profit>0 else ""
        st.markdown(f"""
<div style="background:#f8f9fa;padding:14px 16px;border-radius:10px;
            margin-bottom:4px;border-left:4px solid {color}">
  <h5 style="margin:0 0 6px 0"><strong>{info["name"]}</strong>
    <span style="font-size:13px;color:gray">&nbsp;({ticker}) | {info["qty"]:,}주</span></h5>
  <div style="font-size:14px;line-height:1.9">
    평균단가: {info["avg_price"]:,}원 &nbsp;→&nbsp; <b>현재가: {curr:,}원</b><br>
    평가손익: <span style="color:{color};font-weight:700">{sign}{profit:,}원</span>
    &nbsp;(수익률: <span style="color:{color}">{sign}{rate:.2f}%</span>)
    &nbsp;|&nbsp; 평가금액: <b>{val:,}원</b>
  </div>
</div>""", unsafe_allow_html=True)
        if high_52w>low_52w:
            pos_pct=max(0.0,min(100.0,(curr-low_52w)/(high_52w-low_52w)*100))
            bc="#ff4b4b" if pos_pct>=80 else "#ffa500" if pos_pct>=60 else "#28a745" if pos_pct>=20 else "#1c83e1"
            pi=int(pos_pct)
            st.markdown(f"""
<div style="background:#f8f9fa;padding:6px 16px 12px;margin-top:-4px;
            margin-bottom:10px;border-radius:0 0 10px 10px;border-left:4px solid {color}">
  <div style="font-size:11px;color:#888;margin-bottom:4px">
    📊 52주 위치 <b>{pi}%</b> &nbsp;|&nbsp; 저점 {low_52w:,}원 &harr; 고점 {high_52w:,}원</div>
  <div style="background:#e9ecef;border-radius:4px;height:8px;position:relative;overflow:visible">
    <div style="background:{bc};border-radius:4px;height:8px;width:{pi}%"></div>
    <div style="position:absolute;top:-3px;left:{pi}%;transform:translateX(-50%);
                width:14px;height:14px;border-radius:50%;background:{bc};
                border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,.3)"></div>
  </div>
</div>""", unsafe_allow_html=True)
        else:
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
#  AI 분석용 텍스트 빌더
# ═══════════════════════════════════════════════════════════
def build_portfolio_text(portfolio: dict) -> str:
    merged: dict = {}
    for acc_label, acc_key in ACC_MAP.items():
        for info in portfolio.get(acc_key,{}).values():
            tk=info.get("ticker",""); nm=info.get("name",tk)
            if not tk: continue
            if tk not in merged: merged[tk]={"ticker":tk,"name":nm,"qty":0,"total_cost":0,"accounts":[]}
            merged[tk]["qty"]+=info["qty"]; merged[tk]["total_cost"]+=info["avg_price"]*info["qty"]
            if acc_label not in merged[tk]["accounts"]: merged[tk]["accounts"].append(acc_label)
    if not merged: return "보유 종목 없음"
    header = f"[보유 종목 목록]\n" + ", ".join([f"{m['name']}({tk})" for tk, m in merged.items()]) + f"\n총 {len(merged)}개\n" + "="*60 + "\n"
    lines=[]
    for tk,m in merged.items():
        qty=m["qty"]; avg_p=m["total_cost"]//qty if qty else 0
        curr,h52,l52=get_stock_data(tk)
        if curr==0: curr=avg_p
        rate=((curr-avg_p)/avg_p*100) if avg_p else 0
        ma=get_moving_averages(tk)
        ma_str=" | ".join(filter(None,[
            f"MA20:{ma['ma20']:,}원" if ma.get("ma20") else "",
            f"MA60:{ma['ma60']:,}원" if ma.get("ma60") else "",
            (f"60일선대비:{'+' if ma.get('curr_vs_ma60',0)>=0 else ''}{ma['curr_vs_ma60']}%"
             if ma.get("curr_vs_ma60") is not None else ""),
        ])) or "미수집"
        pos_str=""
        if h52>l52:
            drop=(h52-curr)/h52*100; pp=(curr-l52)/(h52-l52)*100
            pos_str=f"52주위치:{pp:.0f}%(고점대비-{drop:.1f}%)"
        inv=fetch_investor_trend(tk)
        inv_str=f"외국인:{'▲' if inv.get('foreign_net',0)>0 else '▼'}{abs(inv.get('foreign_net',0)):,}주 기관:{'▲' if inv.get('institution_net',0)>0 else '▼'}{abs(inv.get('institution_net',0)):,}주" if inv else "미수집"
        acc_str=" / ".join(m["accounts"])
        if is_etf(m["name"]):
            etf=fetch_etf_naver_data(tk)
            ep=" | ".join(filter(None,[
                # ── 기존 지표 ─────────────────────────────────────────
                f"AUM:{etf['aum_억']:,}억" if etf.get("aum_억") is not None else "",
                # 총보수: naver ter 우선, 없으면 yfinance yf_ter 사용
                f"총보수:{etf['ter']:.3f}%" if etf.get("ter") is not None else
                    (f"총보수:{etf['yf_ter']:.4f}%" if etf.get("yf_ter") is not None else ""),
                f"추적오차:{etf['tracking_error']:.2f}%" if etf.get("tracking_error") is not None else "",
                f"괴리율:{etf['premium_discount']:+.2f}%" if etf.get("premium_discount") is not None else "",
                f"NAV:{int(etf['nav']):,}원" if etf.get("nav") is not None else "",
                f"기초지수:{etf['base_index']}" if etf.get("base_index") else "",
                # ── [추가] yfinance 보완 지표 ─────────────────────────
                # 베타: 1 초과=시장보다 공격적, 1 미만=방어적
                f"베타:{etf['beta']:.2f}" if etf.get("beta") is not None else "",
                # 실질 분배율: 높을수록 인컴 매력도 ↑
                f"분배율:{etf['etf_yield']:.2f}%" if etf.get("etf_yield") is not None else "",
            ])) or "ETF지표 미수집"
            lines.append(f"▶ [ETF] {m['name']}({tk}) | 계좌:{acc_str}\n"
                         f"   보유:{qty:,}주 | 평단:{avg_p:,}원→현재:{curr:,}원 | 수익률:{rate:+.1f}%\n"
                         f"   {pos_str} | [ETF지표] {ep}\n   [MA] {ma_str} | [수급] {inv_str}")
        else:
            fund=fetch_naver_fundamentals(tk)
            fp=" | ".join(filter(None,[
                # ── 기존 지표 ─────────────────────────────────────────
                f"PER:{fund['per']:.1f}배" if fund.get("per") is not None else "",
                f"PBR:{fund['pbr']:.2f}배" if fund.get("pbr") is not None else "",
                f"ROE:{fund['roe']:.1f}%" if fund.get("roe") is not None else "",
                f"EPS:{int(fund['eps']):,}원" if fund.get("eps") is not None else "",
                f"시총:{fund['market_cap_억']:,}억" if fund.get("market_cap_억") is not None else "",
                # ── [추가] 미래 가치·성장성 지표 ─────────────────────
                # ForwardPER: 예상이익 기준, Trailing PER보다 미래 지향
                f"ForwardPER:{fund['forward_per']:.1f}배" if fund.get("forward_per") is not None else "",
                # PEG: 1 이하=성장 대비 저평가, 2 이상=고평가 신호
                f"PEG:{fund['peg']:.2f}" if fund.get("peg") is not None else "",
                # 매출성장률: 양수·높을수록 성장주 근거 강화
                f"매출성장:{fund['rev_growth']:+.1f}%" if fund.get("rev_growth") is not None else "",
                # 부채비율: 100 이하 양호, 200 초과 주의
                f"부채비율:{fund['debt_equity']:.0f}%" if fund.get("debt_equity") is not None else "",
                # FCF: 양수·클수록 자사주매입·배당 여력 높음
                f"FCF:{fund['fcf_억']:,}억" if fund.get("fcf_억") is not None else "",
            ])) or "미수집"
            per_tgt=""
            if fund.get("per") and fund.get("eps"):
                ip=fund["per"]*1.15; per_tgt=f"PER기반적정가:{int(ip*fund['eps']):,}원"
            lines.append(f"▶ [개별주] {m['name']}({tk}) | 계좌:{acc_str}\n"
                         f"   보유:{qty:,}주 | 평단:{avg_p:,}원→현재:{curr:,}원 | 수익률:{rate:+.1f}%\n"
                         f"   {pos_str} | [펀더멘털] {fp} | [적정가] {per_tgt or 'N/A'}\n"
                         f"   [MA] {ma_str} | [수급] {inv_str}")
    return header+"\n\n".join(lines)


def build_market_context(news, indices, fg) -> str:
    today=datetime.date.today().strftime("%Y년 %m월 %d일")
    lines=[f"[실시간 수집 — {today}]\n▣ 시장 심리: {fg.get('score','N/A')}/100 — {fg.get('label','')}"]
    if indices:
        lines.append("\n▣ 주요 지수")
        for name,v in indices.items():
            lines.append(f"  {name}: {v['current']:,.2f}  {'▲' if v['pct']>=0 else '▼'}{abs(v['pct']):.2f}%")
    vix=indices.get("VIX",{}).get("current",0)
    if vix:
        vl="매우낮음" if vix<15 else "낮음" if vix<20 else "보통" if vix<25 else "높음" if vix<30 else "공황수준"
        lines.append(f"\n  ※ VIX {vix:.2f} — {vl}")
    if news:
        lines.append("\n▣ 주요 경제 뉴스")
        for i,a in enumerate(news,1):
            lines.append(f"  {i:02d}. [{a['source']}] {a['title']}")
    return "\n".join(lines)


WATCHLIST = {
    "005930":"삼성전자","000660":"SK하이닉스","373220":"LG에너지솔루션",
    "005380":"현대차","035420":"NAVER","035720":"카카오",
    "207940":"삼성바이오로직스","105560":"KB금융","055550":"신한지주",
    "012450":"한화에어로스페이스","066570":"LG전자","000270":"기아",
}

@st.cache_data(ttl=300)
def fetch_watchlist_prices() -> dict:
    r={}
    for tk,nm in WATCHLIST.items():
        curr,h52,l52=get_stock_data(tk)
        if curr>0: r[tk]={"name":nm,"curr":curr,"high52":h52,"low52":l52}
    return r

def build_watchlist_context(wp: dict) -> str:
    if not wp: return "(시세 수집 실패)"
    lines=["종목명(코드) | 현재가 | 52주고점 | 고점대비낙폭","-"*55]
    for tk,d in wp.items():
        if d["curr"]>0 and d["high52"]>0:
            drop=(d["high52"]-d["curr"])/d["high52"]*100
            lines.append(f"{d['name']}({tk}) | {d['curr']:>9,}원 | {d['high52']:>9,}원 | -{drop:.1f}%")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
#  Gemini 2단계 호출
# ═══════════════════════════════════════════════════════════
def _model_score(name):
    n=name.lower()
    if any(x in n for x in ("embedding","aqa","retrieval","vision","robotics")): return 999
    if "1.5-flash" in n and "thinking" not in n and "preview" not in n: return 10
    if "2.0-flash" in n and "thinking" not in n and "preview" not in n and "exp" not in n: return 20
    if "2.5-flash" in n and "thinking" not in n and "preview" not in n: return 30
    if "flash" in n and "thinking" in n: return 40
    if "flash" in n and ("preview" in n or "exp" in n): return 50
    if "flash" in n: return 35
    if "pro" in n and "preview" not in n: return 60
    return 80

def get_available_models(api_key: str) -> list:
    genai.configure(api_key=api_key)
    try: raw=[m.name.replace("models/","") for m in genai.list_models() if "generateContent" in m.supported_generation_methods]
    except Exception as e: raise RuntimeError(f"모델 목록 조회 실패: {e}")
    if not raw: raise RuntimeError("사용 가능한 모델이 없습니다.")
    sm=sorted(raw,key=_model_score)
    PREF=["gemini-1.5-flash-latest","gemini-1.5-flash","gemini-2.0-flash","gemini-2.5-flash"]
    pinned=[m for p in PREF for m in sm if m==p]; rest=[m for m in sm if m not in pinned]
    seen=set(); result=[]
    for m in pinned+rest:
        if m not in seen: seen.add(m); result.append(m)
    return result

def _is_truncated(resp) -> bool:
    try:
        fr=resp.candidates[0].finish_reason
        if fr==2 or (hasattr(fr,"name") and fr.name=="MAX_TOKENS"): return True
    except: pass
    try:
        text=resp.text.rstrip()
        if text:
            ll=text.split("\n")[-1].strip()
            if ll and not any(ll.endswith(e) for e in (".", "!", "?", "다", "요", "임", "음", "됨", "세", "—", "%", ")", "]", "원", "주")): return True
    except: pass
    return False

def _call_single(model_obj, prompt, max_tokens):
    cfg=genai.types.GenerationConfig(temperature=0.15,top_p=0.85,max_output_tokens=max_tokens)
    resp=model_obj.generate_content(prompt,generation_config=cfg)
    return resp.text,_is_truncated(resp)

def call_gemini(api_key, model_name, prompt, max_tokens, stage_label="", status_ph=None, max_cont=3):
    genai.configure(api_key=api_key)
    if not st.session_state.get("available_models"):
        st.session_state.available_models=get_available_models(api_key)
    candidates=[model_name]+[m for m in st.session_state.available_models if m!=model_name]
    full_text=""; used_model=model_name; truncated=False
    for attempt,model in enumerate(candidates):
        next_m=candidates[attempt+1] if attempt+1<len(candidates) else None
        try:
            if status_ph: status_ph.text(f"🤖 [{stage_label}] {model} 호출 중...")
            m_obj=genai.GenerativeModel(model_name=model)
            full_text,truncated=_call_single(m_obj,prompt,max_tokens)
            used_model=model; st.session_state["active_model"]=model
            if attempt>0: st.toast(f"✅ {model} 전환 완료",icon="🤖")
            break
        except Exception as e:
            msg=str(e).lower()
            if "404" in msg or "not found" in msg: continue
            elif "429" in msg or "quota" in msg or "rate" in msg:
                if next_m: st.toast(f"⚠️ {model} 할당량 소진 → {next_m} 전환",icon="🔄"); time.sleep(5)
                continue
            else: raise e
    else: raise RuntimeError(f"[{stage_label}] 모든 모델 시도 실패")
    cont_count=0
    while truncated and cont_count<max_cont:
        cont_count+=1
        st.warning(f"⚠️ [{stage_label}] 응답 잘림 → 이어받기 {cont_count}회...",icon="🔄")
        cont_prompt=f"""포트폴리오 분석 리포트 작성 중 중단됨. 아래 내용 직후부터 바로 이어서 완성하세요.
[중단 직전]\n──────────\n{full_text[-800:].strip()}\n──────────
※ 인사·소개 반복 금지. 끊긴 곳에서 바로 이어서. 이미 작성된 섹션 재작성 금지."""
        try:
            for c_model in [used_model]+[m for m in candidates if m!=used_model]:
                try:
                    c_obj=genai.GenerativeModel(model_name=c_model)
                    cont_text,cont_trunc=_call_single(c_obj,cont_prompt,CONTINUATION_MAX_TOKENS)
                    truncated=cont_trunc
                    if cont_text: full_text=full_text.rstrip()+"\n"+cont_text.lstrip()
                    break
                except Exception as ce:
                    if "429" in str(ce).lower(): time.sleep(5); continue
                    elif "404" in str(ce).lower(): continue
                    truncated=False; break
        except Exception as e: st.warning(f"⚠️ 이어받기 실패: {e}"); break
    if truncated and cont_count>=max_cont:
        full_text+="\n\n---\n> ⚠️ 일부 내용이 잘렸을 수 있습니다. 종목 수를 줄여서 다시 시도해보세요."
    return full_text


def call_gemini_two_stage(api_key, model_name, market_ctx, portfolio_text, today, progress_bar=None):
    status=st.empty()
    stage1=f"""
너는 탑티어 퀀트 펀드매니저다. 오늘은 {today}이다.
아래 실시간 데이터만으로 시장 브리핑·섹터 동향을 작성하라. (종목 분석 제외)
{market_ctx}

## 1. 🌟 핵심 요약 ({today} 기준 — 핵심 시장 대응 전략 3가지)
---
## 2. 🌍 24Hr 마켓 브리핑
- 주요 지수 수치·해석 / 환율·원자재 / 시장 심리 / 핵심 리스크 2개·기회 2개
---
## 3. 📈 주도 섹터 & 투자 테마
- 수혜 섹터 2~3개 / 주의 섹터 1~2개 / 외국인·기관 수급 방향
"""
    if progress_bar: progress_bar.progress(60, text="🌍 [Stage 1] 매크로 분석 중...")
    s1=call_gemini(api_key,model_name,stage1,STAGE1_MAX_TOKENS,"Stage1:매크로",status,2)

    if progress_bar: progress_bar.progress(75, text="📊 [Stage 2] 종목 분석 중...")
    wp=fetch_watchlist_prices(); alloc=calc_portfolio_allocation(st.session_state.portfolio)
    alloc_str=""
    if alloc:
        alloc_str=f"총 평가금액: {alloc['total_val']:,}원\n국내ETF:{alloc['domestic_etf_pct']}% | 해외ETF:{alloc['foreign_etf_pct']}% | 개별주:{alloc['stock_pct']}%"

stage2=f"""
너는 탑티어 퀀트 펀드매니저다. 오늘은 {today}이다.
[Stage1 매크로 요약]\n{s1[:500]}
[보유 포트폴리오]\n{portfolio_text}
[포트폴리오 자산배분]\n{alloc_str}
[참고 시세]\n{build_watchlist_context(wp)}
[계좌별 세금]
• 💼 일반주식계좌: 매매차익 과세 없음, 배당 15.4%
• 🛡️ ISA: 200만원 비과세, 초과분 9.9%, 3년 의무보유
• 🏢 퇴직연금: 과세이연, 위험자산 70% 한도
• 💰 연금저축: 세액공제 16.5%, 55세 이후 수령

🚨 [분석 원칙]
1. 단순 뉴스나 차트보다 전달된 **미래 선행 데이터(PEG, Forward PER, FCF, 매출성장률)**를 최우선 근거로 삼을 것.
2. [보유 종목] 분석 시 단 하나도 빠짐없이 100% 개별 분석할 것.

## 4. 📊 보유 종목 분석
### 📌 [종목명] ([코드]) — [ETF/개별주] | [계좌] | 총 OO주
- **투자의견**: 강력매수/추가매수/보유/비중축소/매도
- **목표가**: OOO원 (미래 이익 전망치 기반 산출)
- **미래 가치 지표**: [개별주] PEG, FCF, 매출성장, 부채비율 수치 분석 / [ETF] 베타, 분배율, 총보수 분석
- **이동평균**: MA60 대비 ±OO%
- **수급**: 외국인▲/▼ | 기관▲/▼
- **핵심 근거**: 펀더멘털 변화 및 미래 성장성 위주 2줄
- **손절가**: OOO원
- **계좌 절세 포인트**: 1줄
---
## 5. 💡 신규 추천 종목 (미래 성장 가치 기반)
※ 주의: 뉴스에 뜬 종목이 아닌, **PEG 1.2 이하 및 FCF 양수** 등 수치적 저평가 종목을 우선 발굴할 것.
### 📌 [[계좌]] 추천: [종목명] ([코드])
- **미래 성장 가치 점수**: OO점 / 100점
- **추천 근거**: PEG 및 FCF 기반의 저평가/성장성 논리 중심 (뉴스 언급 지양)
- **현재가·목표가·손절가**: 수치 명시
- **매수 전략**: 분할 매수 시점 제시
---
## 6. ⚖️ 리밸런싱 제언
- 현재 vs 적정 배분 / 액션 플랜 3단계 (금리 민감도 반영)
---
## 7. 📅 모니터링 포인트
- 미래 가치 훼손 여부를 판단할 핵심 선행 지표 3개 (예: 분기 매출 성장률 둔화 등)
"""
    s2=call_gemini(api_key,model_name,stage2,STAGE2_MAX_TOKENS,"Stage2:종목분석",status,3)
    if progress_bar: progress_bar.progress(100, text="✅ 분석 완료!")
    status.empty()
    return f"{s1}\n\n---\n\n{s2}"


# ═══════════════════════════════════════════════════════════
#  ★ 앱 시작점 ★  (set_page_config 반드시 첫 번째 st 호출)
# ═══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Mr.Ham AI 포트폴리오",
    page_icon="📈",
    layout="wide",                    # ← 핵심: PC에서 전체 너비 사용
    initial_sidebar_state="expanded",
)

# PC/모바일 반응형 CSS
st.markdown("""
<style>
/* ════════════════════════════════════════
   헤더·푸터 숨기기
════════════════════════════════════════ */
header { visibility: hidden; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* 네이티브 collapsedControl 도 혹시 있으면 표시 (이중 보험) */
[data-testid="collapsedControl"],
[data-testid="collapsedControl"] * {
    visibility: visible !important;
}

/* ════════════════════════════════════════
   PC 공통 레이아웃
════════════════════════════════════════ */
.block-container {
    padding-top: 1.2rem !important;
    padding-bottom: 2rem !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
    max-width: 100% !important;
}

/* 사이드바 너비 */
[data-testid="stSidebar"] {
    min-width: 260px !important;
    max-width: 290px !important;
}
[data-testid="stSidebar"] > div:first-child {
    padding-top: 0.8rem;
    padding-left: 0.8rem;
    padding-right: 0.8rem;
}

/* ════════════════════════════════════════
   탭 스타일
════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: #f0f4f8;
    border-radius: 12px;
    padding: 5px;
    margin-bottom: 8px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    padding: 8px 18px;
    font-weight: 600;
    font-size: 0.92rem;
    color: #555;
}
.stTabs [aria-selected="true"] {
    background: white !important;
    color: #1c83e1 !important;
    box-shadow: 0 1px 5px rgba(0,0,0,0.12);
}

/* ════════════════════════════════════════
   버튼 공통
════════════════════════════════════════ */
.stButton > button {
    min-height: 2.6rem;
    font-size: 0.88rem;
    white-space: nowrap;          /* 텍스트 줄바꿈 방지 */
    overflow: hidden;
    text-overflow: ellipsis;
}

/* 사이드바 버튼 — 글자 잘림 방지 */
[data-testid="stSidebar"] .stButton > button {
    font-size: 0.8rem !important;
    padding-left: 6px !important;
    padding-right: 6px !important;
    white-space: nowrap !important;
}
.stTextInput > div > div > input,
.stNumberInput > div > div > input {
    font-size: 1rem !important;
}

/* ════════════════════════════════════════
   모바일 (768px 이하)
════════════════════════════════════════ */
@media (max-width: 768px) {
    .block-container {
        padding-left: 0.6rem !important;
        padding-right: 0.6rem !important;
        padding-top: 0.3rem !important;
    }
    /* 탭 텍스트 작게 */
    .stTabs [data-baseweb="tab"] {
        padding: 7px 10px;
        font-size: 0.8rem;
    }
    /* 요약 숫자 */
    div[style*="font-size:24px"] { font-size: 18px !important; }
    div[style*="font-size:26px"] { font-size: 18px !important; }
    /* 버튼 터치 영역 */
    .stButton > button { min-height: 3rem; font-size: 0.85rem; }
}
</style>
""", unsafe_allow_html=True)


# 세션 초기화
for k,v in {
    "user": None, "portfolio": None,
    "ai_report": None, "report_time": None,
    "market_ctx": None, "fear_greed": {},
    "available_models": [],
    "api_key": "",           # ← 세션에만 존재, 절대 DB 저장 안 함
    "api_key_verified": False,
    "active_model": None,
}.items():
    if k not in st.session_state: st.session_state[k]=v


# ── STEP 1: 로그인 체크 ──────────────────────────────────
if not st.session_state.user:
    show_auth_page()
    st.stop()

# ── STEP 2: 포트폴리오 로드 (최초 1회) ──────────────────
if st.session_state.portfolio is None:
    with st.spinner("📂 포트폴리오 불러오는 중..."):
        st.session_state.portfolio = load_portfolio()

# ── STEP 3: API 키 입력 체크 ─────────────────────────────
if not st.session_state.api_key_verified:
    show_api_key_page()
    st.stop()


# ═══════════════════════════════════════════════════════════
#  ★ 메인 앱 (로그인 + API 키 모두 확인된 상태) ★
# ═══════════════════════════════════════════════════════════
api_key    = st.session_state.api_key
user_email = st.session_state.user.email

st.title("📈 Mr.Ham  |  24Hr AI 포트폴리오 매니저 v9.0")
st.caption(f"👤 **{user_email}** 로그인 중  |  🔑 API 키 확인됨 (세션에만 유지, 서버 저장 없음)")

# ════════════════════════════════════════════════════════
#  ★ 사이드바 재열기 플로팅 버튼 JS 주입 ★
#  · JavaScript가 DOM에 직접 커스텀 버튼을 생성합니다
#  · CSS 방식은 Streamlit 내부 구조에 따라 동작 안 할 수 있어
#    JS 방식(window.parent)으로 완전히 대체
#  · 라이트·다크모드, PC·모바일 모두 대응
# ════════════════════════════════════════════════════════
components.html("""
<script>
(function(){
    // Streamlit 컴포넌트는 iframe 안에서 실행되므로 부모 document 접근
    var doc = window.parent ? window.parent.document : document;

    /* ── 커스텀 플로팅 버튼 생성 / 재사용 ── */
    function getOrCreate() {
        var existing = doc.getElementById('mrham-sidebar-open-btn');
        if (existing) return existing;

        var btn = doc.createElement('button');
        btn.id = 'mrham-sidebar-open-btn';
        btn.title = '메뉴 열기 (Open Menu)';
        btn.innerHTML =
            '<div style="font-size:22px;line-height:1">&#9776;</div>' +
            '<div style="font-size:9px;font-weight:700;letter-spacing:.6px;margin-top:3px">MENU</div>';

        btn.style.cssText = [
            'position:fixed',
            'left:12px',
            'top:12px',
            'transform:none',,
            'z-index:2147483647',             /* 최대 z-index */
            'background:linear-gradient(160deg,#ff6b2b 0%,#ff8c42 100%)',
            'color:#ffffff',
            'border:2.5px solid rgba(255,255,255,0.65)',
            'border-left:2.5px solid rgba(255,255,255,0.65)',
            'border-radius:12px',
            'width:42px',
            'height:42px',
            'cursor:pointer',
            'display:none',
            'flex-direction:column',
            'align-items:center',
            'justify-content:center',
            'box-shadow:5px 0 28px rgba(255,107,43,0.85), 0 0 0 1px rgba(0,0,0,0.08)',
            'padding:0',
            'font-family:system-ui,sans-serif',
            'text-align:center',
            'transition:width .18s ease, background .18s ease',
            'outline:none',
        ].join(';');

        /* hover 효과 */
        btn.addEventListener('mouseenter', function(){
            this.style.opacity = '0.85';
            this.style.background = 'linear-gradient(160deg,#e85a1e 0%,#ff7a30 100%)';
        });
        btn.addEventListener('mouseleave', function(){
            this.style.opacity = '1';
            this.style.background = 'linear-gradient(160deg,#ff6b2b 0%,#ff8c42 100%)';
        });

        /* 클릭: Streamlit 네이티브 토글 버튼 찾아서 클릭 */
        btn.addEventListener('click', function(){
            var selectors = [
                '[data-testid="collapsedControl"]',
                '[data-testid="collapsedControl"] button',
                '[data-testid="stSidebarCollapseButton"] button',
                'button[aria-label*="sidebar"]',
                'button[aria-label*="Sidebar"]',
                'button[aria-label*="navigation"]',
            ];
            for (var i = 0; i < selectors.length; i++) {
                var el = doc.querySelector(selectors[i]);
                if (el) { el.click(); return; }
            }
            /* 네이티브 버튼을 못 찾으면 사이드바 강제 노출 */
            var sb = doc.querySelector('[data-testid="stSidebar"]');
            if (sb) sb.style.transform = 'translateX(0)';
        });

        doc.body.appendChild(btn);
        return btn;
    }

    /* ── 사이드바 접힘 여부 판단 ── */
    function isSidebarCollapsed() {
        var sb = doc.querySelector('[data-testid="stSidebar"]');
        if (!sb) return true;

        /* aria-expanded 속성 확인 */
        var expanded = sb.getAttribute('aria-expanded');
        if (expanded === 'false') return true;
        if (expanded === 'true')  return false;

        /* 너비로 판단 (< 30px 이면 접힌 것) */
        try {
            var w = sb.getBoundingClientRect().width;
            return w < 30;
        } catch(e) { return false; }
    }

    /* ── 버튼 표시/숨김 업데이트 ── */
    function update() {
        try {
            var btn = getOrCreate();
            var collapsed = isSidebarCollapsed();
            btn.style.display = collapsed ? 'flex' : 'none';
        } catch(e) {}
    }

    /* 초기 실행 */
    update();
    setTimeout(update, 300);
    setTimeout(update, 800);

    /* 주기적 폴링 (500ms) */
    setInterval(update, 500);

    /* MutationObserver — 즉각 반응 */
    try {
        var obs = new MutationObserver(function(){ setTimeout(update, 60); });
        obs.observe(doc.body, {
            childList: true, subtree: true,
            attributes: true,
            attributeFilter: ['aria-expanded','style','class','data-collapsed']
        });
    } catch(e) {}
})();
</script>
""", height=0)


# ═══════════════════════════════════════════════════════════
#  사이드바
# ═══════════════════════════════════════════════════════════
with st.sidebar:

    # ── 사이드바 닫기 안내 배너 ─────────────────────────
    st.markdown("""
    <div style='background:#fff3e0;border-radius:8px;padding:9px 13px;margin-bottom:8px;
                border-left:3px solid #ff6b2b;font-size:12px;line-height:1.6;color:#7a3a00'>
        <b>📌 메뉴 접기/펼치기</b><br>
        · <b>접기</b>: 왼쪽 상단 <b style="color:#1c83e1">← 버튼</b> 클릭<br>
        · <b>펼치기</b>: 화면 왼쪽 <b style="color:#ff6b2b">🟧 주황 버튼</b> 클릭
    </div>
    """, unsafe_allow_html=True)

    # ── 유저 정보 ────────────────────────────────────────
    st.markdown(f"""
    <div style='background:#f0f7ff;border-radius:10px;padding:12px 14px;margin-bottom:4px;
                border-left:3px solid #1c83e1'>
        <div style='font-size:11px;color:#888'>로그인 계정</div>
        <div style='font-size:14px;font-weight:700;color:#1c3a5e;margin-top:2px'>{user_email}</div>
    </div>
    <div style='background:#f0fff4;border-radius:8px;padding:8px 12px;margin-bottom:6px;
                font-size:12px;color:#1a5e2e'>
        🔑 API 키: 세션에만 유지 · 서버 저장 없음
    </div>
    """, unsafe_allow_html=True)

    col_lo, col_rk = st.columns(2)
    with col_lo:
        if st.button("🚪 로그아웃", use_container_width=True, key="sb_logout"):
            _do_logout()
    with col_rk:
        if st.button("🔑 키 재입력", use_container_width=True, key="sb_rekey"):
            st.session_state.api_key          = ""
            st.session_state.api_key_verified  = False
            st.session_state.available_models  = []
            st.rerun()

    st.divider()

    # ── 모델 선택 ─────────────────────────────────────────
    st.subheader("🤖 AI 모델 선택")
    models = st.session_state.get("available_models", [])
    if not models:
        with st.spinner("모델 조회 중..."):
            try: models=get_available_models(api_key); st.session_state.available_models=models
            except: models=[]

    selected_model = None
    if models:
        selected_model=st.selectbox("모델",models,index=0)
        n=(selected_model or "").lower()
        if "flash" in n and "thinking" not in n and "preview" not in n: st.success("✅ 추천 모델")
        elif "thinking" in n: st.info("ℹ️ 추론 특화 (토큰 많이 사용)")
        elif "preview" in n or "exp" in n: st.warning("⚠️ 실험적 모델")
        active=st.session_state.get("active_model")
        if active and active!=selected_model: st.warning(f"🔄 실제 사용: `{active}`")
        st.markdown(
            "<p style='color:#e03030;font-size:11px;margin:6px 0 2px;line-height:1.4'>"
            "⚡ <b>gemini-flash-latest</b> 선택 시 토큰 절약됨!</p>",
            unsafe_allow_html=True
        )
        if st.button("🔃 모델 새로고침", use_container_width=True):
            st.session_state.available_models=[]; st.rerun()
    else:
        st.error("❌ 모델 없음 — API 키를 재입력해주세요.")

    st.divider()

    # ── 데이터 수집 상태 ──────────────────────────────────
    st.subheader("📡 데이터 수집 상태")
    missing=[x for x,y in [("FinanceDataReader",HAS_FDR),("feedparser",HAS_FEEDPARSER),("yfinance",HAS_YFINANCE)] if not y]
    if not missing: st.success("✅ 모든 라이브러리 정상")
    else: st.warning(f"⚠️ 미설치: `{', '.join(missing)}`")

    st.divider()

    # ── 종목 추가 ─────────────────────────────────────────
    st.subheader("➕ 종목 추가")
    add_acc=st.radio("계좌",ACC_KEYS,key="add_radio")
    new_tk=st.text_input("종목코드",placeholder="예: 360750")
    new_nm=st.text_input("종목명",placeholder="예: TIGER미국S&P500")
    new_qty=st.number_input("수량(주)",min_value=1,step=1,value=1)
    new_avg=st.number_input("평단가(원)",min_value=0,step=100,value=0)
    if st.button("➕ 추가",use_container_width=True):
        if new_tk and new_nm and new_avg>0:
            iid=str(uuid.uuid4())
            st.session_state.portfolio[ACC_MAP[add_acc]][iid]={"ticker":new_tk.strip(),"name":new_nm.strip(),"qty":new_qty,"avg_price":new_avg}
            save_portfolio(st.session_state.portfolio)
            st.success(f"✅ {new_nm} 추가!"); st.rerun()
        else: st.warning("모든 항목을 입력해주세요.")

    st.divider()

    # ── 종목 삭제 ─────────────────────────────────────────
    st.subheader("➖ 종목 삭제")
    del_acc=st.radio("계좌",ACC_KEYS,key="del_radio")
    del_items=st.session_state.portfolio[ACC_MAP[del_acc]]
    if del_items:
        def _fmt(iid): it=del_items[iid]; return f"{it['name']} ({it['ticker']}) — {it['qty']}주"
        del_id=st.selectbox("삭제",list(del_items.keys()),format_func=_fmt)
        if st.button("🗑️ 삭제",use_container_width=True):
            del st.session_state.portfolio[ACC_MAP[del_acc]][del_id]
            save_portfolio(st.session_state.portfolio); st.warning("삭제 완료!"); st.rerun()
    else: st.info("삭제할 종목이 없습니다.")

    st.divider()

    # ── 종목 수정 ─────────────────────────────────────────
    st.subheader("✏️ 종목 수정")
    edit_acc=st.radio("계좌",ACC_KEYS,key="edit_radio")
    edit_items=st.session_state.portfolio[ACC_MAP[edit_acc]]
    if edit_items:
        def _fmt_e(iid): it=edit_items[iid]; return f"{it['name']} ({it['ticker']}) — {it['qty']}주"
        edit_id=st.selectbox("수정",list(edit_items.keys()),format_func=_fmt_e,key="edit_sel")
        if edit_id and edit_id in edit_items:
            cur=edit_items[edit_id]
            e_nm=st.text_input("종목명",value=cur["name"],key=f"en_{edit_id}")
            e_qty=st.number_input("수량",min_value=1,step=1,value=int(cur["qty"]),key=f"eq_{edit_id}")
            e_avg=st.number_input("평단가(원)",min_value=0,step=100,value=int(cur["avg_price"]),key=f"ea_{edit_id}")
            changed=e_nm.strip()!=cur["name"] or int(e_qty)!=cur["qty"] or int(e_avg)!=cur["avg_price"]
            if changed: st.info(f"✏️ {e_nm} | {int(e_qty):,}주 | {int(e_avg):,}원")
            if st.button("✅ 저장",use_container_width=True,type="primary",disabled=not changed):
                st.session_state.portfolio[ACC_MAP[edit_acc]][edit_id].update({"name":e_nm.strip() or cur["name"],"qty":int(e_qty),"avg_price":int(e_avg)})
                save_portfolio(st.session_state.portfolio); st.success("✅ 수정 완료!"); st.rerun()
    else: st.info("수정할 종목이 없습니다.")

    st.divider()
    st.caption("주가 캐시: 5분 | 뉴스: 10분 | 펀더멘털: 1시간")


# ═══════════════════════════════════════════════════════════
#  메인 — 3탭 구조 (PC·모바일 공통)
# ═══════════════════════════════════════════════════════════

# 모바일 전용 안내 배너 (사이드바 열기 버튼 안내)
st.markdown("""
<div style='background:linear-gradient(90deg,#ff6b2b,#e85a1e);
            color:white;border-radius:10px;padding:10px 16px;margin-bottom:12px;
            font-size:13px;display:flex;align-items:center;gap:10px'>
    <span style='font-size:20px'>☰</span>
    <span>
        <b>메뉴 열기</b>: 화면 왼쪽 상단 주황 버튼을 누르면 사이드바가 열립니다.<br>
        <span style='opacity:0.85;font-size:12px'>AI 모델 선택은 사이드바에서, 종목 관리는 아래 ⚙️ 탭에서</span>
    </span>
</div>
""", unsafe_allow_html=True)

tab_pf, tab_manage, tab_ai = st.tabs(["📊 포트폴리오", "⚙️ 종목 관리", "🤖 AI 분석"])


# ══════════════════════════════
#  TAB 1 — 포트폴리오
# ══════════════════════════════
with tab_pf:
    total_cost,total_val=calc_totals(st.session_state.portfolio)
    profit=total_val-total_cost
    rate=(profit/total_cost*100) if total_cost>0 else 0
    pc="#ff4b4b" if profit>0 else "#1c83e1" if profit<0 else "#888"
    ps="▲ +" if profit>0 else "▼ " if profit<0 else ""

    st.markdown("### 💰 내 포트폴리오 전체 요약")
    c1,c2,c3=st.columns(3)
    with c1:
        st.markdown(f"""<div style='padding:10px 0'>
            <div style='font-size:13px;color:#555;font-weight:600;margin-bottom:4px'>총 투자금액 (원금)</div>
            <div style='font-size:24px;font-weight:700'>{total_cost:,}<span style='font-size:14px'> 원</span></div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div style='padding:10px 0'>
            <div style='font-size:13px;color:#555;font-weight:600;margin-bottom:4px'>총 평가금액</div>
            <div style='font-size:24px;font-weight:700'>{total_val:,}<span style='font-size:14px'> 원</span></div>
            <div style='font-size:14px;font-weight:700;color:{pc};margin-top:4px'>{ps}{profit:,} 원</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div style='padding:10px 0'>
            <div style='font-size:13px;color:#555;font-weight:600;margin-bottom:4px'>총 수익률</div>
            <div style='font-size:24px;font-weight:700;color:{pc}'>{ps}{rate:.2f}<span style='font-size:14px'> %</span></div>
        </div>""", unsafe_allow_html=True)

    st.divider()
    col1,col2=st.columns(2)
    with col1: display_portfolio("💼 일반주식계좌",st.session_state.portfolio["general_acc"])
    with col2: display_portfolio("🛡️ ISA계좌",    st.session_state.portfolio["isa_acc"])
    st.markdown("<br>",unsafe_allow_html=True)
    col3,col4=st.columns(2)
    with col3: display_portfolio("🏢 퇴직연금",   st.session_state.portfolio["pension_acc"])
    with col4: display_portfolio("💰 연금저축",   st.session_state.portfolio["savings_acc"])
    st.divider()

    with st.expander("📡 실시간 수집 데이터 미리보기", expanded=False):
        if st.button("🔃 데이터 수집", key="fetch_preview"):
            with st.spinner("수집 중..."):
                news=fetch_realtime_news(); indices=fetch_market_indices()
                fg=calculate_fear_greed(indices); ctx=build_market_context(news,indices,fg)
                st.session_state.market_ctx=ctx; st.session_state.fear_greed=fg
            st.success(f"수집 완료 — 뉴스 {len(news)}건, 지수 {len(indices)}개")
        if st.session_state.fear_greed:
            fg=st.session_state.fear_greed; score=fg.get("score",50)
            st.markdown(f"""
            <div style='margin:10px 0;padding:14px;background:#f8f9fa;border-radius:10px'>
                <b>📊 시장 심리 지수</b>
                <span style='float:right;color:{fg.get("color","#888")};font-size:17px;font-weight:700'>{score}/100 — {fg.get("label","")}</span>
                <div style='margin-top:10px;background:#ddd;border-radius:6px;height:10px'>
                    <div style='width:{score}%;background:{fg.get("color","#888")};height:10px;border-radius:6px'></div>
                </div>
            </div>""", unsafe_allow_html=True)
        if st.session_state.market_ctx: st.code(st.session_state.market_ctx, language="text")


# ══════════════════════════════
#  TAB 2 — 종목 관리 (모바일 친화)
# ══════════════════════════════
with tab_manage:
    st.markdown("### ⚙️ 종목 관리")
    st.caption("종목 추가·삭제·수정을 여기서 바로 할 수 있습니다.")

    sub1, sub2, sub3 = st.tabs(["➕ 추가", "🗑️ 삭제", "✏️ 수정"])

    # ── 추가 ──────────────────────────────────────────────
    with sub1:
        st.markdown("<br>", unsafe_allow_html=True)
        m_add_acc = st.radio("계좌 선택", ACC_KEYS, key="m_add_radio", horizontal=True)
        st.markdown("<br>", unsafe_allow_html=True)
        ca, cb = st.columns(2)
        with ca:
            m_tk  = st.text_input("종목코드", placeholder="예: 360750", key="m_tk")
            m_qty = st.number_input("수량 (주)", min_value=1, step=1, value=1, key="m_qty")
        with cb:
            m_nm  = st.text_input("종목명", placeholder="예: TIGER미국S&P500", key="m_nm")
            m_avg = st.number_input("매수 평단가 (원)", min_value=0, step=100, value=0, key="m_avg")
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("➕ 종목 추가", type="primary", use_container_width=True, key="m_btn_add"):
            if m_tk and m_nm and m_avg > 0:
                iid = str(uuid.uuid4())
                st.session_state.portfolio[ACC_MAP[m_add_acc]][iid] = {
                    "ticker": m_tk.strip(), "name": m_nm.strip(),
                    "qty": m_qty, "avg_price": m_avg,
                }
                save_portfolio(st.session_state.portfolio)
                st.success(f"✅ {m_nm} 추가 완료!"); st.rerun()
            else:
                st.warning("종목코드 · 종목명 · 평단가를 모두 입력해주세요.")

    # ── 삭제 ──────────────────────────────────────────────
    with sub2:
        st.markdown("<br>", unsafe_allow_html=True)
        m_del_acc  = st.radio("계좌 선택", ACC_KEYS, key="m_del_radio", horizontal=True)
        m_del_key  = ACC_MAP[m_del_acc]
        m_del_pool = st.session_state.portfolio[m_del_key]
        st.markdown("<br>", unsafe_allow_html=True)
        if m_del_pool:
            def _mfmt_d(iid):
                it=m_del_pool[iid]; return f"{it['name']} ({it['ticker']}) — {it['qty']:,}주 / 평단 {it['avg_price']:,}원"
            m_del_id = st.selectbox("삭제할 종목", list(m_del_pool.keys()), format_func=_mfmt_d, key="m_del_sel")
            if m_del_id:
                it = m_del_pool[m_del_id]
                st.markdown(f"""
                <div style='background:#fff3f3;border-radius:10px;padding:12px 16px;margin:10px 0;
                            border-left:4px solid #ff4b4b;font-size:14px'>
                    🗑️ 삭제 예정: <b>{it['name']}</b> ({it['ticker']}) —
                    {it['qty']:,}주 / 평단 {it['avg_price']:,}원
                </div>""", unsafe_allow_html=True)
                if st.button("🗑️ 삭제 확인", use_container_width=True, key="m_btn_del"):
                    del st.session_state.portfolio[m_del_key][m_del_id]
                    save_portfolio(st.session_state.portfolio)
                    st.warning("삭제 완료!"); st.rerun()
        else:
            st.info("해당 계좌에 삭제할 종목이 없습니다.")

    # ── 수정 ──────────────────────────────────────────────
    with sub3:
        st.markdown("<br>", unsafe_allow_html=True)
        m_edit_acc  = st.radio("계좌 선택", ACC_KEYS, key="m_edit_radio", horizontal=True)
        m_edit_key  = ACC_MAP[m_edit_acc]
        m_edit_pool = st.session_state.portfolio[m_edit_key]
        st.markdown("<br>", unsafe_allow_html=True)
        if m_edit_pool:
            def _mfmt_e(iid):
                it=m_edit_pool[iid]; return f"{it['name']} ({it['ticker']}) — {it['qty']:,}주"
            m_edit_id = st.selectbox("수정할 종목", list(m_edit_pool.keys()), format_func=_mfmt_e, key="m_edit_sel")
            if m_edit_id and m_edit_id in m_edit_pool:
                cur = m_edit_pool[m_edit_id]
                st.markdown(f"""
                <div style='background:#f0f7ff;border-radius:10px;padding:10px 16px;margin:8px 0;
                            border-left:4px solid #1c83e1;font-size:13px'>
                    📋 현재: <b>{cur['name']}</b> | {cur['qty']:,}주 | 평단 {cur['avg_price']:,}원
                </div>""", unsafe_allow_html=True)
                cc, cd = st.columns(2)
                with cc:
                    e_nm  = st.text_input("종목명", value=cur["name"], key=f"m_en_{m_edit_id}")
                    e_qty = st.number_input("수량 (주)", min_value=1, step=1, value=int(cur["qty"]), key=f"m_eq_{m_edit_id}")
                with cd:
                    e_avg = st.number_input("평단가 (원)", min_value=0, step=100, value=int(cur["avg_price"]), key=f"m_ea_{m_edit_id}")
                changed = (e_nm.strip()!=cur["name"] or int(e_qty)!=cur["qty"] or int(e_avg)!=cur["avg_price"])
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✅ 수정 저장", type="primary", use_container_width=True,
                             disabled=not changed, key="m_btn_edit"):
                    st.session_state.portfolio[m_edit_key][m_edit_id].update({
                        "name": e_nm.strip() or cur["name"],
                        "qty":  int(e_qty), "avg_price": int(e_avg),
                    })
                    save_portfolio(st.session_state.portfolio)
                    st.success("✅ 수정 완료!"); st.rerun()
                if not changed:
                    st.caption("변경된 내용이 없으면 저장 버튼이 비활성화됩니다.")
        else:
            st.info("해당 계좌에 수정할 종목이 없습니다.")


# ══════════════════════════════
#  TAB 3 — AI 분석
# ══════════════════════════════
with tab_ai:
    st.markdown("### 🤖 AI 실시간 포트폴리오 분석")
    i1,i2,i3,i4=st.columns(4)
    with i1: st.info("📰 RSS 뉴스 수집")
    with i2: st.info("📊 지수·VIX 수집")
    with i3: st.info("💹 PER·PBR·ROE 수집")
    with i4: st.info("🤖 2단계 AI 분석")

    if st.button("🔄 최신 데이터 수집 & AI 포트폴리오 분석 실행", type="primary", use_container_width=True):
        if not selected_model:
            st.error("🤖 사이드바에서 AI 모델을 선택해주세요!")
        else:
            today=datetime.date.today().strftime("%Y년 %m월 %d일")
            progress=st.progress(0,text="📡 실시간 데이터 수집 중...")
            with st.spinner("뉴스 & 지수 수집 중..."):
                news=fetch_realtime_news(); indices=fetch_market_indices()
                fg=calculate_fear_greed(indices); mc=build_market_context(news,indices,fg)
                st.session_state.market_ctx=mc; st.session_state.fear_greed=fg
            progress.progress(25,text=f"✅ 뉴스 {len(news)}건, 지수 {len(indices)}개")
            progress.progress(40,text="💹 펀더멘털 수집 중...")
            with st.spinner("Naver Finance 수집 중..."):
                pwf=build_portfolio_text(st.session_state.portfolio)
            progress.progress(55,text="🤖 AI 분석 시작...")
            try:
                report=call_gemini_two_stage(api_key,selected_model,mc,pwf,today,progress_bar=progress)
                st.session_state.ai_report=report
                st.session_state.report_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success(f"✅ 분석 완료 — {st.session_state.report_time}")
            except Exception as e:
                progress.empty(); st.error(f"❌ AI 분석 오류: {e}")
                if "429" in str(e) or "quota" in str(e).lower():
                    st.warning("💡 할당량 초과 → 다른 모델 선택하거나 1~2분 후 재시도해주세요.")

    if st.session_state.ai_report:
        st.divider()
        col_i,col_d=st.columns([3,1])
        with col_i: st.markdown(f"**📋 분석 시각:** {st.session_state.report_time}")
        with col_d:
            st.download_button("📥 리포트 다운로드 (.md)",
                data=st.session_state.ai_report.encode("utf-8"),
                file_name=f"AI_투자리포트_{(st.session_state.report_time or 'report')[:10]}.md",
                mime="text/markdown",use_container_width=True)
        st.markdown(st.session_state.ai_report)

st.divider()
st.markdown("""
<div style='font-size:12px;color:#aaa;text-align:center;line-height:2'>
    ⚠️ 본 서비스는 투자 참고용입니다. 모든 투자 결정의 책임은 본인에게 있습니다.<br>
    Gemini API 키는 서버에 저장되지 않으며, 세션 종료 시 즉시 삭제됩니다.<br>
    <b>Powered by</b> Gemini AI · FinanceDataReader · Naver Finance · yfinance · Supabase
</div>
""", unsafe_allow_html=True)
