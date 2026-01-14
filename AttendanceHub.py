# AttendanceHub_GSheets.py
# إدارة الغيابات للمكوّنين + Google Sheets backend (MB/Bizerte)
# WhatsApp (فردي/جماعي) + تجاوز 10% + Import + سجل الإشعارات
# ✅ FAST Sheets access (ws_map cache) + أقل metadata calls
# ✅ 10% WhatsApp: رسالة واحدة لكل متكوّن فيها كل المواد اللي فات فيهم

import os
import json
import time
import uuid
import urllib.parse
from datetime import datetime, date, timedelta

import pandas as pd
import streamlit as st
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials


# ================== إعداد الصفحة ==================
st.set_page_config(page_title="AttendanceHub - Mega Formation", layout="wide")

st.markdown(
    """
    <div style='text-align:center'>
      <h1>🕒 AttendanceHub - إدارة الغيابات</h1>
      <p>متكوّنين، مواد، غيابات، واتساب، 10٪ - مع Google Sheets</p>
    </div>
    <hr/>
    """,
    unsafe_allow_html=True,
)

# ================== إعداد Google Sheets ==================
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

# أسماء الشيتات
TRAINEES_SHEET = "Trainees"
SUBJECTS_SHEET = "Subjects"
ABSENCES_SHEET = "Absences"
NOTIF_LOG_SHEET = "Notifications_Log"

TRAINEES_COLS = ["id", "nom", "telephone", "tel_parent", "branche", "specialite", "date_debut", "actif"]

SUBJECTS_COLS = [
    "id",
    "nom_matiere",
    "branche",
    "specialites",  # قائمة تخصّصات مفصولة بفاصلة
    "heures_totales",
    "heures_semaine",
]

ABSENCES_COLS = ["id", "trainee_id", "subject_id", "date", "heures_absence", "justifie", "commentaire"]

NOTIF_LOG_COLS = [
    "id",
    "trainee_id",
    "phone",
    "target",       # Trainee / Parent
    "branche",
    "period_from",
    "period_to",
    "period_label",
    "sent_at_iso",  # تاريخ ووقت الإرسال (UTC ISO)
]


# ================== Robust Google API helpers ==================
def _apierr_details(e: Exception) -> str:
    try:
        if hasattr(e, "response") and e.response is not None:
            try:
                return json.dumps(e.response.json(), ensure_ascii=False)
            except Exception:
                return str(e.response.text)
    except Exception:
        pass
    return str(e)


def _status_code(e: Exception) -> int:
    try:
        if hasattr(e, "response") and e.response is not None:
            return int(getattr(e.response, "status_code", 0) or 0)
    except Exception:
        pass
    return 0


def _should_retry_api_error(e: Exception) -> bool:
    return _status_code(e) in (429, 500, 502, 503, 504)


def _retry_sleep_fast(i: int):
    # ✅ أسرع من قبل باش ما يقعدش يدور برشا
    time.sleep(0.35 * (2 ** i))


def safe_row_values(ws, row: int, tries: int = 4):
    last_err = None
    for i in range(tries):
        try:
            return ws.row_values(row)
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err


def safe_get_all_values(ws, tries: int = 4):
    last_err = None
    for i in range(tries):
        try:
            return ws.get_all_values()
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err


def safe_update(ws, rng: str, values, tries: int = 4):
    last_err = None
    for i in range(tries):
        try:
            return ws.update(rng, values)
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err


def safe_update_cell(ws, row: int, col: int, value, tries: int = 4):
    last_err = None
    for i in range(tries):
        try:
            return ws.update_cell(row, col, value)
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err


def safe_append_row(ws, row_values, tries: int = 4):
    last_err = None
    for i in range(tries):
        try:
            return ws.append_row(row_values)
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err


def safe_delete_rows(ws, row_index: int, tries: int = 4):
    last_err = None
    for i in range(tries):
        try:
            return ws.delete_rows(row_index)
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err


# ================== Auth ==================
def make_client_and_sheet_id():
    # 1) Streamlit secrets (cloud)
    if "gcp_service_account" in st.secrets:
        try:
            sa_info = dict(st.secrets["gcp_service_account"])
            creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
            client_ = gspread.authorize(creds)

            if "SPREADSHEET_ID" not in st.secrets:
                st.error("⚠️ المفتاح SPREADSHEET_ID مش موجود في secrets.")
                st.stop()

            sheet_id_ = st.secrets["SPREADSHEET_ID"]
            return client_, sheet_id_
        except Exception as e:
            st.error(f"⚠️ خطأ في gcp_service_account داخل secrets: {e}")
            st.stop()

    # 2) Local service_account.json
    elif os.path.exists("service_account.json"):
        try:
            creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
            client_ = gspread.authorize(creds)
            sheet_id_ = "PUT_YOUR_SHEET_ID_HERE"
            return client_, sheet_id_
        except Exception as e:
            st.error(f"⚠️ خطأ في قراءة service_account.json: {e}")
            st.stop()

    else:
        st.error(
            "❌ لا وجدنا لا gcp_service_account في Streamlit secrets لا ملف service_account.json.\n\n"
            "▶ في Streamlit Cloud: زيد gcp_service_account و SPREADSHEET_ID في secrets.\n"
            "▶ لوكال: حط service_account.json في نفس فولدر الملف."
        )
        st.stop()


client, SPREADSHEET_ID = make_client_and_sheet_id()


# ================== FAST worksheet cache (Fix الدوّارة + fetch_sheet_metadata) ==================
WSMAP_TTL_SEC = 120

def _now_ts() -> float:
    return time.time()

def _invalidate_sheet_cache():
    st.session_state.pop("sh_obj", None)
    st.session_state.pop("sh_id", None)
    st.session_state.pop("ws_map", None)
    st.session_state.pop("ws_map_at", None)

def get_spreadsheet():
    if st.session_state.get("sh_id") == SPREADSHEET_ID and "sh_obj" in st.session_state:
        return st.session_state["sh_obj"]

    last_err = None
    for i in range(4):
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            st.session_state["sh_obj"] = sh
            st.session_state["sh_id"] = SPREADSHEET_ID
            return sh
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            st.error("❌ Google Sheets APIError (open_by_key):\n" + _apierr_details(e))
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)

    st.error("❌ فشل فتح Google Sheet بعد retries:\n" + _apierr_details(last_err))
    raise last_err

def get_ws_map(sh, force_refresh: bool = False):
    ts = st.session_state.get("ws_map_at", 0)
    ws_map = st.session_state.get("ws_map")

    if (not force_refresh) and ws_map and (_now_ts() - ts) < WSMAP_TTL_SEC:
        return ws_map

    last_err = None
    for i in range(4):
        try:
            wss = sh.worksheets()  # ✅ metadata مرة وحدة بدل worksheet() كل مرة
            ws_map = {w.title.strip(): w for w in wss}
            st.session_state["ws_map"] = ws_map
            st.session_state["ws_map_at"] = _now_ts()
            return ws_map
        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _retry_sleep_fast(i)
                continue
            raise
        except Exception as e:
            last_err = e
            _retry_sleep_fast(i)
    raise last_err

def ensure_ws(title: str, columns: list[str]):
    title = title.strip()
    last_err = None

    for i in range(4):
        try:
            sh = get_spreadsheet()
            ws_map = get_ws_map(sh, force_refresh=False)

            ws = ws_map.get(title)
            if ws is None:
                ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
                safe_update(ws, "1:1", [columns])
                get_ws_map(sh, force_refresh=True)  # refresh بعد الإنشاء
                return ws

            header = safe_row_values(ws, 1)
            if (not header) or (header[: len(columns)] != columns):
                safe_update(ws, "1:1", [columns])

            return ws

        except gse.APIError as e:
            last_err = e
            if _should_retry_api_error(e):
                _invalidate_sheet_cache()
                _retry_sleep_fast(i)
                continue
            st.error(f"❌ APIError في ensure_ws('{title}'):\n" + _apierr_details(e))
            raise
        except Exception as e:
            last_err = e
            _invalidate_sheet_cache()
            _retry_sleep_fast(i)

    st.error(f"❌ فشل ensure_ws('{title}') بعد retries:\n" + _apierr_details(last_err))
    raise last_err


def append_record(sheet_name: str, cols: list[str], rec: dict):
    ws = ensure_ws(sheet_name, cols)
    row = [str(rec.get(c, "")) for c in cols]
    safe_append_row(ws, row)
    st.cache_data.clear()


def delete_record_by_id(sheet_name: str, cols: list[str], rec_id: str):
    ws = ensure_ws(sheet_name, cols)
    vals = safe_get_all_values(ws)
    if not vals or len(vals) < 2:
        return
    header = vals[0]
    id_idx = header.index("id") if "id" in header else 0

    for i, r in enumerate(vals[1:], start=2):
        if len(r) > id_idx and r[id_idx] == rec_id:
            safe_delete_rows(ws, i)
            st.cache_data.clear()
            break


def update_record_fields_by_id(sheet_name: str, cols: list[str], rec_id: str, updates: dict):
    ws = ensure_ws(sheet_name, cols)
    vals = safe_get_all_values(ws)
    if not vals or len(vals) < 2:
        return
    header = vals[0]
    if "id" not in header:
        return

    id_idx = header.index("id")
    row_idx = None
    for i, r in enumerate(vals[1:], start=2):
        if len(r) > id_idx and r[id_idx] == rec_id:
            row_idx = i
            break

    if not row_idx:
        return

    for field, value in updates.items():
        if field in header:
            col_idx = header.index(field) + 1
            safe_update_cell(ws, row_idx, col_idx, str(value))

    st.cache_data.clear()


def delete_records_by_branch(sheet_name: str, cols: list[str], branch_value: str) -> int:
    ws = ensure_ws(sheet_name, cols)
    vals = safe_get_all_values(ws)
    if not vals or len(vals) < 2:
        return 0

    header = vals[0]
    if "branche" not in header:
        return 0

    b_idx = header.index("branche")
    rows_to_delete = []
    for i, r in enumerate(vals[1:], start=2):
        if len(r) > b_idx and r[b_idx] == branch_value:
            rows_to_delete.append(i)

    for row_i in reversed(rows_to_delete):
        safe_delete_rows(ws, row_i)

    if rows_to_delete:
        st.cache_data.clear()
    return len(rows_to_delete)


def append_notification_log(
    trainee_id: str,
    phone: str,
    target: str,
    branche: str,
    period_from: date,
    period_to: date,
    period_label: str,
):
    rec = {
        "id": uuid.uuid4().hex[:12],
        "trainee_id": trainee_id,
        "phone": phone,
        "target": target,
        "branche": branche,
        "period_from": period_from.strftime("%Y-%m-%d"),
        "period_to": period_to.strftime("%Y-%m-%d"),
        "period_label": period_label,
        "sent_at_iso": datetime.utcnow().isoformat(),
    }
    append_record(NOTIF_LOG_SHEET, NOTIF_LOG_COLS, rec)


# ================== Helpers ==================
def normalize_phone(s: str) -> str:
    digits = "".join(c for c in str(s) if c.isdigit())
    if len(digits) == 8:
        return "216" + digits
    return digits


def wa_link(number: str, message: str) -> str:
    num = normalize_phone(number)
    if not num:
        return ""
    return f"https://wa.me/{num}?text={urllib.parse.quote(message)}"


def branch_password(branch: str) -> str:
    try:
        m = st.secrets["branch_passwords"]
        if "Menzel" in branch or branch == "MB":
            return str(m.get("MB", ""))
        if "Bizerte" in branch or branch == "BZ":
            return str(m.get("BZ", ""))
    except Exception:
        pass
    return ""


def as_float(x) -> float:
    try:
        return float(str(x).replace(",", ".").strip() or 0)
    except Exception:
        return 0.0


def build_whatsapp_message_for_trainee(
    tr_row,
    df_abs_all,
    df_sub_all,
    branch_name,
    d_from: date,
    d_to: date,
    period_label: str,
) -> tuple[str, list[str]]:
    """
    ✅ FIX:
    - Details (list of absences) stays limited to the selected period.
    - BUT 10% computation (remaining / exceeded) is cumulative across ALL TIME.
    """

    trainee_id = tr_row["id"]
    df_abs_t = df_abs_all[df_abs_all["trainee_id"] == trainee_id].copy()

    if df_abs_t.empty:
        return "", ["لا توجد غيابات لهذا المتكوّن في أي فترة."]

    # --- parse dates ---
    df_abs_t["date_dt"] = pd.to_datetime(df_abs_t["date"], errors="coerce")

    # =========================
    # (1) Details only for period
    # =========================
    mask_period = (df_abs_t["date_dt"].dt.date >= d_from) & (df_abs_t["date_dt"].dt.date <= d_to)
    df_abs_period = df_abs_t[mask_period].copy()

    if df_abs_period.empty:
        return "", ["لا توجد غيابات في هذه الفترة."]

    # attach subject info for period details
    df_abs_period = df_abs_period.merge(
        df_sub_all[["id", "nom_matiere", "heures_totales"]],
        left_on="subject_id",
        right_on="id",
        how="left",
        suffixes=("", "_sub"),
    )

    detail_lines = []
    for _, r in df_abs_period.iterrows():
        dstr = r["date_dt"].strftime("%Y-%m-%d") if pd.notna(r["date_dt"]) else str(r["date"])
        subj = str(r.get("nom_matiere", "") or "").strip()
        h = as_float(r.get("heures_absence", 0))
        just = "مبرر" if str(r.get("justifie", "")).strip() == "Oui" else "غير مبرر"
        detail_lines.append(f"- {dstr} | {subj} | {h:.2f} ساعة ({just})")

    # =========================
    # (2) 10% cumulative (ALL TIME)
    # =========================
    df_abs_alltime = df_abs_t.copy()

    df_abs_alltime = df_abs_alltime.merge(
        df_sub_all[["id", "nom_matiere", "heures_totales"]],
        left_on="subject_id",
        right_on="id",
        how="left",
        suffixes=("", "_sub"),
    )

    df_eff_all = df_abs_alltime[df_abs_alltime["justifie"] != "Oui"].copy()
    df_eff_all["heures_absence_f"] = df_eff_all["heures_absence"].apply(as_float)
    df_eff_all["heures_totales_f"] = df_eff_all["heures_totales"].apply(as_float)

    stats_lines = []
    elim_lines = []

    if not df_eff_all.empty:
        grp_t = df_eff_all.groupby("nom_matiere", as_index=False).agg(
            total_abs=("heures_absence_f", "sum"),
            heures_tot=("heures_totales_f", "first"),
        )
        grp_t["limit_10"] = grp_t["heures_tot"] * 0.10
        grp_t["remaining"] = grp_t["limit_10"] - grp_t["total_abs"]

        # ترتيب: المواد اللي أقرب/فاتت 10% تظهر الأول
        grp_t = grp_t.sort_values("remaining", ascending=True).reset_index(drop=True)

        for _, g in grp_t.iterrows():
            mat_name = str(g["nom_matiere"]).strip()
            total_abs = float(g["total_abs"] or 0)
            heures_tot = float(g["heures_tot"] or 0)
            limit_10 = float(g["limit_10"] or 0)
            remaining = float(g["remaining"] or 0)

            stats_lines.append(
                f"- {mat_name}:\n"
                f"   • مجموع الغياب غير المبرر (إجمالي): {total_abs:.2f} ساعة\n"
                f"   • حدّ 10٪: {limit_10:.2f} ساعة (من {heures_tot:.2f} ساعة)\n"
                f"   • الباقي قبل الإقصاء: {max(0.0, remaining):.2f} ساعة"
            )

            if remaining <= 0:
                elim_lines.append(mat_name)

    # =========================
    # Build message
    # =========================
    msg_lines = []
    msg_lines.append("السلام عليكم،")
    msg_lines.append("إدارة هيكل التكوين تحب تعلمك بتفاصيل الغيابات اللي تمّ تسجيلها في الفترة المحدّدة:")
    msg_lines.append("")
    msg_lines.append(f"👤 المتكوّن: {tr_row.get('nom', '')}")
    msg_lines.append(f"🏫 الفرع: {branch_name}")
    msg_lines.append(f"🔧 التخصّص: {tr_row.get('specialite', '')}")
    msg_lines.append(f"🕒 الفترة: {period_label}")
    msg_lines.append("")
    msg_lines.append("📋 تفاصيل الغيابات في هذه الفترة:")
    msg_lines.extend(detail_lines)

    if stats_lines:
        msg_lines.append("")
        msg_lines.append("📊 ملخّص الغيابات غير المبررة حسب المواد (إجمالي منذ بداية التكوين):")
        msg_lines.extend(stats_lines)

    if elim_lines:
        msg_lines.append("")
        msg_lines.append("⚠️ يؤسفني إعلامكم أنّ هذه المادة/المواد سيتم إجراء الإمتحان بشهر أوت وذلك لتجاوزكم الحد الأقصى المسموح به من الغيابات (10٪):")
        for m in elim_lines:
            msg_lines.append(f"- {m}")

    msg_lines.append("")
    msg_lines.append("🙏 نشكروك على تفهّمك، ومرحبا بيك في الإدارة لأي استفسار.")

    msg = "\n".join(msg_lines)
    info_debug = [
        f"غيابات في الفترة: {len(df_abs_period)}",
        f"غيابات غير مبررة محسوبة لــ10٪ (إجمالي): {len(df_eff_all)}"
    ]
    return msg, info_debug

# ================== Load data ==================
@st.cache_data(ttl=300)
def load_trainees():
    ws = ensure_ws(TRAINEES_SHEET, TRAINEES_COLS)
    try:
        vals = safe_get_all_values(ws)
        if not vals or len(vals) < 2:
            return pd.DataFrame(columns=TRAINEES_COLS)
        return pd.DataFrame(vals[1:], columns=vals[0])
    except gse.APIError as e:
        st.error("❌ APIError في load_trainees:\n" + _apierr_details(e))
        return pd.DataFrame(columns=TRAINEES_COLS)


@st.cache_data(ttl=300)
def load_subjects():
    ws = ensure_ws(SUBJECTS_SHEET, SUBJECTS_COLS)
    try:
        vals = safe_get_all_values(ws)
        if not vals or len(vals) < 2:
            return pd.DataFrame(columns=SUBJECTS_COLS)
        return pd.DataFrame(vals[1:], columns=vals[0])
    except gse.APIError as e:
        st.error("❌ APIError في load_subjects:\n" + _apierr_details(e))
        return pd.DataFrame(columns=SUBJECTS_COLS)


@st.cache_data(ttl=300)
def load_absences():
    ws = ensure_ws(ABSENCES_SHEET, ABSENCES_COLS)
    try:
        vals = safe_get_all_values(ws)
        if not vals or len(vals) < 2:
            return pd.DataFrame(columns=ABSENCES_COLS)
        return pd.DataFrame(vals[1:], columns=vals[0])
    except gse.APIError as e:
        st.error("❌ APIError في load_absences:\n" + _apierr_details(e))
        return pd.DataFrame(columns=ABSENCES_COLS)


@st.cache_data(ttl=300)
def load_notifications():
    ws = ensure_ws(NOTIF_LOG_SHEET, NOTIF_LOG_COLS)
    try:
        vals = safe_get_all_values(ws)
        if not vals or len(vals) < 2:
            return pd.DataFrame(columns=NOTIF_LOG_COLS)
        return pd.DataFrame(vals[1:], columns=vals[0])
    except gse.APIError as e:
        st.error("❌ APIError في load_notifications:\n" + _apierr_details(e))
        return pd.DataFrame(columns=NOTIF_LOG_COLS)


# ================== Sidebar: branch + password ==================
st.sidebar.markdown("## ⚙️ إعدادات الفرع")
branch = st.sidebar.selectbox("اختر الفرع", ["Menzel Bourguiba", "Bizerte"])

pw_need = branch_password(branch)
key_pw = f"branch_pw_ok::{branch}"

if pw_need:
    if key_pw not in st.session_state:
        st.session_state[key_pw] = False
    if not st.session_state[key_pw]:
        pw_try = st.sidebar.text_input("🔐 كلمة سرّ الفرع", type="password")
        if st.sidebar.button("دخول الفرع"):
            if pw_try == pw_need:
                st.session_state[key_pw] = True
                st.sidebar.success("تم الدخول ✅")
            else:
                st.sidebar.error("كلمة سرّ غير صحيحة ❌")
        st.stop()
else:
    st.sidebar.warning("⚠️ لم يتم ضبط كلمة المرور لهذا الفرع في secrets.branch_passwords")

st.sidebar.success(f"أنت الآن داخل فرع: **{branch}**")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["👤 المتكوّنون", "📚 المواد", "📅 الغيابات", "💬 واتساب + 10٪", "📜 سجل الإشعارات"]
)

# ================== Tab1: Trainees ==================
with tab1:
    st.subheader("👤 إدارة المتكوّنين")

    df_tr = load_trainees()
    df_tr = df_tr[df_tr["branche"] == branch].copy() if (not df_tr.empty and "branche" in df_tr.columns) else df_tr

    st.markdown("### ➕ إضافة متكوّن جديد")
    with st.form("add_trainee_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            nom = st.text_input("الاسم واللقب")
            tel = st.text_input("📞 هاتف المتكوّن")
        with c2:
            tel_parent = st.text_input("📞 هاتف الولي (اختياري)")
            spec = st.text_input("🔧 التخصّص (مثال: Anglais A2)")
        with c3:
            dt_deb = st.date_input("📅 تاريخ بداية التكوين", value=date.today())
        submitted_tr = st.form_submit_button("📥 حفظ المتكوّن")

    if submitted_tr:
        if not nom.strip() or not tel.strip() or not spec.strip():
            st.error("❌ الاسم، الهاتف، والتخصّص إجباريين.")
        else:
            new_id = uuid.uuid4().hex[:10]
            new_row = {
                "id": new_id,
                "nom": nom.strip(),
                "telephone": normalize_phone(tel),
                "tel_parent": normalize_phone(tel_parent),
                "branche": branch,
                "specialite": spec.strip(),
                "date_debut": dt_deb.strftime("%Y-%m-%d"),
                "actif": "1",
            }
            try:
                append_record(TRAINEES_SHEET, TRAINEES_COLS, new_row)
                st.success("✅ تم إضافة المتكوّن.")
                st.rerun()
            except Exception as e:
                st.error(f"خطأ أثناء إضافة المتكوّن: {e}")

    st.markdown("### 📋 قائمة المتكوّنين")
    if df_tr.empty:
        st.info("لا يوجد متكوّنون بعد في هذا الفرع.")
    else:
        st.dataframe(
            df_tr[["id", "nom", "telephone", "tel_parent", "specialite", "date_debut", "actif"]],
            use_container_width=True,
        )

        st.markdown("### 🗑️ حذف متكوّن")
        options_tr_del = [f"[{i}] {r['nom']} — {r['specialite']} ({r['telephone']})"
                          for i, (_, r) in enumerate(df_tr.iterrows())]
        pick_tr_del = st.selectbox("اختر المتكوّن للحذف", options_tr_del, key="del_tr_pick")
        if st.button("❗ حذف المتكوّن نهائيًا", key="del_tr_btn"):
            try:
                idx = int(pick_tr_del.split("]")[0].replace("[", "").strip())
                tr_id = df_tr.iloc[idx]["id"]
                delete_record_by_id(TRAINEES_SHEET, TRAINEES_COLS, tr_id)
                st.success("✅ تم الحذف.")
                st.rerun()
            except Exception as e:
                st.error(f"خطأ أثناء الحذف: {e}")


# ================== Tab2: Subjects ==================
with tab2:
    st.subheader("📚 إدارة المواد")

    df_sub_all = load_subjects()
    df_sub = df_sub_all[df_sub_all["branche"] == branch].copy() if (not df_sub_all.empty and "branche" in df_sub_all.columns) else df_sub_all

    # specs_all تشمل Trainees + Subjects (باش multiselect ما يطيّحش)
    df_tr_all = load_trainees()
    specs_from_trainees = []
    if (not df_tr_all.empty) and ("specialite" in df_tr_all.columns):
        specs_from_trainees = [s.strip() for s in df_tr_all["specialite"].dropna().unique().tolist() if str(s).strip()]

    specs_from_subjects = []
    if (not df_sub_all.empty) and ("specialites" in df_sub_all.columns):
        for x in df_sub_all["specialites"].dropna().tolist():
            parts = [p.strip() for p in str(x).split(",") if p.strip()]
            specs_from_subjects.extend(parts)

    specs_all = sorted(set(specs_from_trainees + specs_from_subjects))

    st.markdown("### ➕ إضافة مادة جديدة")
    with st.form("add_subject_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            mat_nom = st.text_input("اسم المادة")
        with c2:
            heures_tot = st.number_input("إجمالي الساعات (للمادة)", min_value=0.0, step=1.0)
        with c3:
            heures_week = st.number_input("عدد الساعات في الأسبوع", min_value=0.0, step=1.0)

        spec_choices = st.multiselect("🔧 التخصّصات المرتبطة بهذه المادة", specs_all)
        sub_submit = st.form_submit_button("📥 حفظ المادة")

    if sub_submit:
        if not mat_nom.strip():
            st.error("❌ اسم المادة إجباري.")
        elif not spec_choices:
            st.error("❌ اختر على الأقل تخصّص واحد للمادة.")
        else:
            new_id = uuid.uuid4().hex[:10]
            rec = {
                "id": new_id,
                "nom_matiere": mat_nom.strip(),
                "branche": branch,
                "specialites": ",".join(spec_choices),
                "heures_totales": str(heures_tot),
                "heures_semaine": str(heures_week),
            }
            try:
                append_record(SUBJECTS_SHEET, SUBJECTS_COLS, rec)
                st.success("✅ تم إضافة المادة.")
                st.rerun()
            except Exception as e:
                st.error(f"خطأ أثناء إضافة المادة: {e}")

    st.markdown("### 📋 قائمة المواد في هذا الفرع")
    if df_sub.empty:
        st.info("لا توجد مواد بعد.")
    else:
        df_show = df_sub.copy()
        df_show["specialites"] = df_show["specialites"].fillna("")
        st.dataframe(
            df_show[["id", "nom_matiere", "specialites", "heures_totales", "heures_semaine"]],
            use_container_width=True,
        )

        st.markdown("### ✏️ تعديل مادة")
        opts_edit = [f"[{i}] {r['nom_matiere']} — {r['specialites']} ({r['heures_totales']}h)"
                     for i, (_, r) in enumerate(df_sub.iterrows())]
        pick_edit = st.selectbox("اختر مادة للتعديل", opts_edit, key="edit_subject_pick")
        idx_edit = int(pick_edit.split("]")[0].replace("[", "").strip())
        row_edit = df_sub.iloc[idx_edit]

        with st.form("edit_subject_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                new_name = st.text_input("اسم المادة", value=row_edit["nom_matiere"])
            with c2:
                new_tot = st.number_input("إجمالي الساعات", value=as_float(row_edit["heures_totales"]), step=1.0)
            with c3:
                new_week = st.number_input("ساعات في الأسبوع", value=as_float(row_edit["heures_semaine"]), step=1.0)

            current_specs = [s.strip() for s in str(row_edit["specialites"]).split(",") if s.strip()]
            current_specs = [s for s in current_specs if s in specs_all]  # ✅ مهم
            new_specs = st.multiselect("التخصّصات", specs_all, default=current_specs)

            sub_ok = st.form_submit_button("💾 حفظ التعديلات")

        if sub_ok:
            try:
                sid = row_edit["id"]
                updates = {
                    "nom_matiere": new_name.strip(),
                    "heures_totales": str(new_tot),
                    "heures_semaine": str(new_week),
                    "specialites": ",".join(new_specs),
                }
                update_record_fields_by_id(SUBJECTS_SHEET, SUBJECTS_COLS, sid, updates)
                st.success("✅ تم تعديل المادة.")
                st.rerun()
            except Exception as e:
                st.error(f"خطأ أثناء تعديل المادة: {e}")

        st.markdown("### 🗑️ حذف مادة")
        opts_del = [f"[{i}] {r['nom_matiere']} — {r['specialites']}"
                    for i, (_, r) in enumerate(df_sub.iterrows())]
        pick_del = st.selectbox("اختر مادة للحذف", opts_del, key="del_subject_pick")
        if st.button("❗ حذف المادة", key="del_subject_btn"):
            try:
                idxd = int(pick_del.split("]")[0].replace("[", "").strip())
                sid = df_sub.iloc[idxd]["id"]
                delete_record_by_id(SUBJECTS_SHEET, SUBJECTS_COLS, sid)
                st.success("✅ تم الحذف.")
                st.rerun()
            except Exception as e:
                st.error(f"خطأ أثناء الحذف: {e}")

        st.markdown("---")
        st.markdown("### 🧨 حذف كل المواد (في هذا الفرع فقط)")
        st.warning("تنبيه: هذا يحذف **كل مواد الفرع الحالي فقط**.")
        confirm_del_all = st.checkbox("أنا متأكد", key="confirm_del_all_subjects")
        if st.button("🗑️ حذف كل مواد الفرع", key="del_all_subjects_btn"):
            if not confirm_del_all:
                st.error("لازم تعمل ✅ تأكيد قبل الحذف.")
            else:
                try:
                    n = delete_records_by_branch(SUBJECTS_SHEET, SUBJECTS_COLS, branch)
                    st.success(f"✅ تم حذف {n} مادة من فرع {branch}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"خطأ أثناء حذف كل المواد: {e}")


# ================== Tab3: Absences ==================
with tab3:
    st.subheader("📅 تسجيل / تعديل / حذف الغيابات")

    df_tr_all = load_trainees()
    df_tr_b = df_tr_all[df_tr_all["branche"] == branch].copy() if not df_tr_all.empty else pd.DataFrame()

    df_sub_all = load_subjects()
    df_sub_b = df_sub_all[df_sub_all["branche"] == branch].copy() if not df_sub_all.empty else pd.DataFrame()

    df_abs_all = load_absences()

    if df_tr_b.empty:
        st.info("لا يوجد متكوّنون في هذا الفرع.")
    elif df_sub_b.empty:
        st.info("لا توجد مواد مضبوطة في هذا الفرع.")
    else:
        specs_in_branch = sorted([s for s in df_tr_b["specialite"].dropna().unique() if s])
        spec_choice = st.selectbox("🔧 اختر التخصّص (لإظهار المتكوّنين)", ["(الكل)"] + specs_in_branch, key="abs_spec_choice")

        df_tr_view = df_tr_b.copy()
        if spec_choice != "(الكل)":
            df_tr_view = df_tr_view[df_tr_view["specialite"] == spec_choice].copy()

        if df_tr_view.empty:
            st.info("لا يوجد متكوّنون بهذا التخصّص في هذا الفرع.")
        else:
            # ---- إضافة غياب جديد (واحد) ----
            st.markdown("### ➕ إضافة غياب (غياب مفرد)")

            options_tr = [f"[{i}] {r['nom']} — {r['specialite']} ({r['telephone']})"
                          for i, (_, r) in enumerate(df_tr_view.iterrows())]
            tr_pick = st.selectbox("اختر المتكوّن", options_tr, key="abs_add_pick_tr")
            idx_tr = int(tr_pick.split("]")[0].replace("[", "").strip())
            row_tr = df_tr_view.iloc[idx_tr]

            spec_tr = str(row_tr["specialite"])
            df_sub_for_tr = df_sub_b[df_sub_b["specialites"].fillna("").str.contains(spec_tr, na=False)].copy()

            if df_sub_for_tr.empty:
                st.warning("لا توجد مواد مربوطة بهذا التخصّص. اضبط المواد في تبويب المواد.")
            else:
                opts_sub = [f"[{i}] {r['nom_matiere']} ({r['heures_totales']}h)"
                            for i, (_, r) in enumerate(df_sub_for_tr.iterrows())]
                sub_pick = st.selectbox("اختر المادة", opts_sub, key="abs_add_pick_sub")
                idx_sub = int(sub_pick.split("]")[0].replace("[", "").strip())
                row_sub = df_sub_for_tr.iloc[idx_sub]

                with st.form("add_abs_form"):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        abs_date = st.date_input("تاريخ الغياب", value=date.today(), key="abs_add_date")
                    with c2:
                        h_abs = st.number_input("عدد ساعات الغياب", min_value=0.0, step=0.5, key="abs_add_hours")
                    with c3:
                        is_justified = st.checkbox("غياب مبرر؟", value=False, key="abs_add_just")
                    comment = st.text_area("ملاحظة (اختياري)", key="abs_add_comment")
                    submit_abs = st.form_submit_button("📥 حفظ الغياب")

                if submit_abs:
                    if h_abs <= 0:
                        st.error("❌ عدد ساعات الغياب يجب أن يكون > 0.")
                    else:
                        new_id = uuid.uuid4().hex[:10]
                        rec = {
                            "id": new_id,
                            "trainee_id": row_tr["id"],
                            "subject_id": row_sub["id"],
                            "date": abs_date.strftime("%Y-%m-%d"),
                            "heures_absence": str(h_abs),
                            "justifie": "Oui" if is_justified else "Non",
                            "commentaire": comment.strip(),
                        }
                        try:
                            append_record(ABSENCES_SHEET, ABSENCES_COLS, rec)
                            st.success("✅ تم تسجيل الغياب.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"خطأ أثناء تسجيل الغياب: {e}")

            # ---- تعديل / حذف غياب واحد ----
            st.markdown("---")
            st.markdown("### ✏️ تعديل / 🗑️ حذف غياب مفرد")

            df_abs_all = load_absences()
            if df_abs_all.empty:
                st.info("لا توجد غيابات مسجلة بعد.")
            else:
                df_abs0 = df_abs_all.copy()
                df_abs0 = df_abs0.rename(columns={"id": "abs_id"})
                df_abs0["heures_absence_f"] = df_abs0["heures_absence"].apply(as_float)

                df_abs = df_abs0.merge(
                    df_tr_all[["id", "nom", "branche", "specialite", "telephone"]],
                    left_on="trainee_id",
                    right_on="id",
                    how="left",
                    suffixes=("", "_tr"),
                ).merge(
                    df_sub_all[["id", "nom_matiere"]],
                    left_on="subject_id",
                    right_on="id",
                    how="left",
                    suffixes=("", "_sub"),
                )

                df_abs = df_abs[df_abs["branche"] == branch].copy()
                if df_abs.empty:
                    st.info("لا توجد غيابات في هذا الفرع.")
                else:
                    df_abs["date_dt"] = pd.to_datetime(df_abs["date"], errors="coerce")
                    df_abs = df_abs.sort_values("date_dt", ascending=False).reset_index(drop=True)

                    options_abs_edit = [
                        f"[{i}] {r['nom']} — {r['nom_matiere']} — {r['date']} — {r['heures_absence_f']}h — مبرر: {r['justifie']}"
                        for i, (_, r) in enumerate(df_abs.iterrows())
                    ]
                    pick_abs = st.selectbox("اختر الغياب للتعديل / الحذف", options_abs_edit, key="abs_edit_pick")

                    idx_abs = int(pick_abs.split("]")[0].replace("[", "").strip())
                    row_a = df_abs.iloc[idx_abs]

                    with st.form("edit_abs_form"):
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            base_date = row_a["date_dt"].date() if pd.notna(row_a["date_dt"]) else date.today()
                            new_date = st.date_input("تاريخ الغياب", value=base_date, key="abs_edit_date")
                        with c2:
                            new_hours = st.number_input("ساعات الغياب", value=float(row_a["heures_absence_f"]), step=0.5, key="abs_edit_hours")
                        with c3:
                            new_just = st.selectbox("مبرر؟", ["Non", "Oui"], index=1 if str(row_a["justifie"]).strip() == "Oui" else 0, key="abs_edit_just")
                        new_comment = st.text_area("ملاحظة", value=str(row_a.get("commentaire", "")), key="abs_edit_comment")

                        b1, b2 = st.columns(2)
                        with b1:
                            submit_edit_abs = st.form_submit_button("💾 حفظ التعديل")
                        with b2:
                            delete_abs = st.form_submit_button("🗑️ حذف هذا الغياب")

                    if submit_edit_abs:
                        try:
                            aid = row_a["abs_id"]
                            updates = {
                                "date": new_date.strftime("%Y-%m-%d"),
                                "heures_absence": str(new_hours),
                                "justifie": new_just,
                                "commentaire": new_comment.strip(),
                            }
                            update_record_fields_by_id(ABSENCES_SHEET, ABSENCES_COLS, aid, updates)
                            st.success("✅ تم تعديل الغياب.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"خطأ أثناء تعديل الغياب: {e}")

                    if delete_abs:
                        try:
                            aid = row_a["abs_id"]
                            delete_record_by_id(ABSENCES_SHEET, ABSENCES_COLS, aid)
                            st.success("✅ تم حذف الغياب.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"خطأ أثناء حذف الغياب: {e}")

            # ---- حذف جماعي (Bulk) ----
            st.markdown("---")
            st.markdown("### 🗑️ حذف مجموعة غيابات (Bulk)")

            df_abs_all = load_absences()
            if df_abs_all.empty:
                st.info("لا توجد غيابات للحذف.")
            else:
                specs_bulk = sorted([s for s in df_tr_b["specialite"].dropna().unique() if s])
                spec_bulk = st.selectbox("🔧 التخصّص (للحذف الجماعي)", ["(الكل)"] + specs_bulk, key="bulk_spec")
                df_tr_bulk = df_tr_b.copy()
                if spec_bulk != "(الكل)":
                    df_tr_bulk = df_tr_bulk[df_tr_bulk["specialite"] == spec_bulk]

                if df_tr_bulk.empty:
                    st.info("لا يوجد متكوّنون بهذا التخصّص.")
                else:
                    labels_map_bulk = {f"{r['nom']} — {r['specialite']} ({r['telephone']})": r["id"]
                                       for _, r in df_tr_bulk.iterrows()}
                    label_tr_bulk = st.selectbox("👤 اختر المتكوّن", list(labels_map_bulk.keys()), key="bulk_tr_pick")
                    trainee_id_bulk = labels_map_bulk[label_tr_bulk]

                    df_abs_t_bulk = df_abs_all[df_abs_all["trainee_id"] == trainee_id_bulk].copy()
                    if df_abs_t_bulk.empty:
                        st.info("لا توجد غيابات لهذا المتكوّن.")
                    else:
                        df_abs_t_bulk = df_abs_t_bulk.merge(
                            df_sub_all[["id", "nom_matiere"]],
                            left_on="subject_id",
                            right_on="id",
                            how="left",
                            suffixes=("", "_sub"),
                        )

                        sub_choices_bulk = sorted(df_abs_t_bulk["nom_matiere"].dropna().unique())
                        sub_bulk = st.selectbox("📚 المادة (اختياري)", ["(الكل)"] + sub_choices_bulk, key="bulk_sub")

                        c1, c2 = st.columns(2)
                        with c1:
                            d_from_bulk = st.date_input("من تاريخ", value=date.today() - timedelta(days=7), key="bulk_from")
                        with c2:
                            d_to_bulk = st.date_input("إلى تاريخ", value=date.today(), key="bulk_to")

                        if d_to_bulk < d_from_bulk:
                            st.error("❌ تاريخ النهاية لازم يكون بعد البداية.")
                        else:
                            if st.button("🗑️ حذف كل الغيابات في هذه الفترة", key="bulk_delete_btn"):
                                try:
                                    df_abs_t_bulk["date_dt"] = pd.to_datetime(df_abs_t_bulk["date"], errors="coerce")
                                    mask = (df_abs_t_bulk["date_dt"].dt.date >= d_from_bulk) & (df_abs_t_bulk["date_dt"].dt.date <= d_to_bulk)
                                    if sub_bulk != "(الكل)":
                                        mask &= (df_abs_t_bulk["nom_matiere"] == sub_bulk)

                                    to_del = df_abs_t_bulk[mask]
                                    if to_del.empty:
                                        st.info("لا توجد غيابات مطابقة للحذف.")
                                    else:
                                        for _, rdel in to_del.iterrows():
                                            delete_record_by_id(ABSENCES_SHEET, ABSENCES_COLS, rdel["id"])
                                        st.success(f"✅ تم حذف {len(to_del)} غياب(ات).")
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"خطأ أثناء الحذف الجماعي: {e}")

            # ---- Import ----
            st.markdown("---")
            st.markdown("### 📥 استيراد غيابات من ملف Excel/CSV")

            st.info(
                "الملف لازم يحتوي الأعمدة التالية على الأقل:\n"
                "- trainee_id\n- subject_id\n- date (YYYY-MM-DD)\n- heures_absence\n"
                "اختياري: justifie (Oui/Non), commentaire"
            )

            template_df = pd.DataFrame(
                {"trainee_id": [], "subject_id": [], "date": [], "heures_absence": [], "justifie": [], "commentaire": []}
            )
            tmpl_csv = template_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ تحميل نموذج CSV", data=tmpl_csv, file_name="absences_template.csv", mime="text/csv")

            uploaded = st.file_uploader("حمّل ملف الغيابات (CSV أو Excel)", type=["csv", "xlsx"], key="import_abs")
            if uploaded is not None:
                try:
                    df_up = pd.read_excel(uploaded) if uploaded.name.lower().endswith(".xlsx") else pd.read_csv(uploaded)

                    req_cols = {"trainee_id", "subject_id", "date", "heures_absence"}
                    if not req_cols.issubset(set(df_up.columns)):
                        st.error(f"❌ الملف لازم يحتوي الأعمدة: {', '.join(req_cols)}")
                    else:
                        count_ok = 0
                        for _, r in df_up.iterrows():
                            try:
                                rec = {
                                    "id": uuid.uuid4().hex[:10],
                                    "trainee_id": str(r["trainee_id"]).strip(),
                                    "subject_id": str(r["subject_id"]).strip(),
                                    "date": str(r["date"]).split()[0],
                                    "heures_absence": str(r["heures_absence"]),
                                    "justifie": "Oui" if str(r.get("justifie", "Non")).strip() == "Oui" else "Non",
                                    "commentaire": str(r.get("commentaire", "")).strip(),
                                }
                                append_record(ABSENCES_SHEET, ABSENCES_COLS, rec)
                                count_ok += 1
                            except Exception:
                                continue
                        st.success(f"✅ تم استيراد {count_ok} غياب(ات).")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء قراءة الملف: {e}")


# ================== Tab4: WhatsApp + exceed 10% + period notify ==================
def build_exceed_10pct_message_one(
    trainee_name: str,
    branch_name: str,
    spec: str,
    items: list,
    remedial_month: str
) -> str:
    """
    items: list of dicts: {matiere, total_abs, limit_10, excess, heures_tot}
    """
    lines = []
    lines.append("السلام عليكم،")
    lines.append("إدارة هيكل التكوين تحب تعلمك أنّه تمّ تجاوز 10٪ من الغيابات غير المبرّرة في المواد التالية:")
    lines.append("")
    lines.append(f"👤 المتكوّن: {trainee_name}")
    lines.append(f"🏫 الفرع: {branch_name}")
    if spec:
        lines.append(f"🔧 التخصّص: {spec}")
    lines.append("")
    lines.append("📌 المواد اللي تمّ تجاوز 10٪ فيها:")
    for it in items:
        lines.append(
            f"- {it['matiere']}:\n"
            f"   • مجموع الغياب غير المبرر: {it['total_abs']:.2f} ساعة\n"
            f"   • حدّ 10٪: {it['limit_10']:.2f} ساعة (من {it['heures_tot']:.2f} ساعة)\n"
            f"   • تجاوز بـ: {it['excess']:.2f} ساعة"
        )
    lines.append("")
    lines.append(f"📌 دورة التدارك: {remedial_month}")
    lines.append("")
    lines.append("🙏 شكراً على التفهّم. لأي استفسار مرحبا بكم في الإدارة.")
    return "\n".join(lines)

with tab4:
    st.subheader("💬 واتساب الغيابات + 🚨 تجاوز 10٪")

    df_tr_all = load_trainees()
    df_tr_b = df_tr_all[df_tr_all["branche"] == branch].copy() if not df_tr_all.empty else pd.DataFrame()

    df_sub_all = load_subjects()
    df_sub_b = df_sub_all[df_sub_all["branche"] == branch].copy() if not df_sub_all.empty else pd.DataFrame()

    df_abs_all = load_absences()

    if df_tr_b.empty or df_sub_b.empty or df_abs_all.empty:
        st.info("يلزم يكون فما متكوّنين + مواد + غيابات باش تخدم الميزة.")
    else:
        # =========================================================
        # (A) اللي فاتو 10٪ (غير مبرر) + زر واتساب (رسالة واحدة لكل متكوّن)
        # =========================================================
        st.markdown("## 🚨 اللي فاتو 10٪ (غيابات غير مبرّرة) — رسالة واحدة فيها كل المواد")

        df_abs = df_abs_all.merge(
            df_tr_b[["id", "nom", "telephone", "tel_parent", "specialite"]],
            left_on="trainee_id",
            right_on="id",
            how="inner",
            suffixes=("", "_tr"),
        ).merge(
            df_sub_b[["id", "nom_matiere", "heures_totales"]],
            left_on="subject_id",
            right_on="id",
            how="inner",
            suffixes=("", "_sub"),
        )

        df_abs["heures_absence_f"] = df_abs["heures_absence"].apply(as_float)
        df_abs["heures_totales_f"] = df_abs["heures_totales"].apply(as_float)
        df_eff = df_abs[(df_abs["justifie"] != "Oui") & (df_abs["heures_totales_f"] > 0)].copy()

        if df_eff.empty:
            st.success("💚 ما فماش غيابات غير مبرّرة محسوبة.")
        else:
            grp = df_eff.groupby(["trainee_id", "subject_id"], as_index=False).agg(
                total_abs=("heures_absence_f", "sum"),
                nom=("nom", "first"),
                tel=("telephone", "first"),
                tel_parent=("tel_parent", "first"),
                spec=("specialite", "first"),
                matiere=("nom_matiere", "first"),
                heures_tot=("heures_totales_f", "first"),
            )
            grp["limit_10"] = grp["heures_tot"] * 0.10
            grp["excess"] = grp["total_abs"] - grp["limit_10"]

            exceeded = grp[grp["excess"] > 0].copy()
            exceeded["total_abs"] = exceeded["total_abs"].round(2)
            exceeded["excess"] = exceeded["excess"].round(2)
            exceeded["limit_10"] = exceeded["limit_10"].round(2)
            exceeded = exceeded.sort_values(["trainee_id", "excess"], ascending=[True, False]).reset_index(drop=True)

            if exceeded.empty:
                st.success("💚 ما فما حد فاتو 10٪ توّا.")
            else:
                st.dataframe(
                    exceeded.rename(columns={
                        "nom": "المتكوّن",
                        "matiere": "المادة",
                        "total_abs": "مجموع الغياب غير المبرر",
                        "excess": "تجاوز بـ",
                    })[["المتكوّن", "المادة", "مجموع الغياب غير المبرر", "تجاوز بـ"]],
                    use_container_width=True,
                )

                c1, c2, c3 = st.columns([2, 1, 1])
                with c1:
                    target = st.radio("المرسل إليه", ["المتكوّن", "الولي"], horizontal=True, key="exceed_target")
                with c2:
                    remedial_month = st.selectbox("شهر التدارك", ["جويلية", "أوت"], key="remedial_month")
                with c3:
                    do_log = st.checkbox("📒 سجّل في سجل الإشعارات", value=True, key="exceed_log")

                if st.button("🔄 توليد رسائل 10٪ (مجمّعة)", key="btn_exceed_build"):
                    st.caption("✅ لكل متكوّن: رسالة واحدة فيها كل المواد اللي فات فيها 10٪.")

                    for trainee_id, g in exceeded.groupby("trainee_id", sort=False):
                        trainee_name = str(g["nom"].iloc[0])
                        tel_t = str(g["tel"].iloc[0] or "")
                        tel_p = str(g["tel_parent"].iloc[0] or "")
                        spec = str(g.get("spec", "").iloc[0] or "")

                        phone_target = tel_t if target == "المتكوّن" else tel_p
                        phone_target = normalize_phone(phone_target)
                        if not phone_target:
                            continue

                        items = []
                        for _, r in g.iterrows():
                            items.append({
                                "matiere": str(r["matiere"]),
                                "total_abs": float(r["total_abs"]),
                                "limit_10": float(r["limit_10"]),
                                "excess": float(r["excess"]),
                                "heures_tot": float(r["heures_tot"]),
                            })

                        msg = build_exceed_10pct_message_one(
                            trainee_name=trainee_name,
                            branch_name=branch,
                            spec=spec,
                            items=items,
                            remedial_month=remedial_month,
                        )
                        link = wa_link(phone_target, msg)

                        st.markdown(
                            f"""
                            <div style="margin-bottom:10px; padding:10px; border:1px solid #eee; border-radius:8px;">
                              <b>👤 {trainee_name}</b><br/>
                              مواد متجاوزة: <b>{len(items)}</b><br/>
                              <a href="{link}" target="_blank"
                                 style="display:inline-block;margin-top:8px;padding:7px 14px;background-color:#25D366;color:white;text-decoration:none;border-radius:7px;font-weight:700;font-size:14px;">
                                 📲 واتساب (رسالة واحدة)
                              </a>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        if do_log:
                            try:
                                append_notification_log(
                                    trainee_id=str(trainee_id),
                                    phone=phone_target,
                                    target="Trainee" if target == "المتكوّن" else "Parent",
                                    branche=branch,
                                    period_from=date.today(),
                                    period_to=date.today(),
                                    period_label=f"تجاوز 10٪ (مجمّع) + تدارك {remedial_month}",
                                )
                            except Exception:
                                pass

        st.markdown("---")

        # =========================================================
        # (B) إعلام الغيابات حسب المدة + واتساب (فردي/جماعي) ✅
        # =========================================================
        st.markdown("## 💬 إعلام الغيابات حسب المدة (يوم/أسبوع/شهر/مخصص)")

        # -------- فردي --------
        st.markdown("### 👤 فردي")

        specs_branch = sorted([s for s in df_tr_b["specialite"].dropna().unique() if s])
        spec_filter = st.selectbox("🔧 اختر التخصّص", ["(الكل)"] + specs_branch, key="wa_spec_single")
        df_tr_wa = df_tr_b.copy()
        if spec_filter != "(الكل)":
            df_tr_wa = df_tr_wa[df_tr_wa["specialite"] == spec_filter]

        if df_tr_wa.empty:
            st.info("لا يوجد متكوّنون بهذا التخصّص.")
        else:
            labels_map_wa = {
                f"{r['nom']} — {r['specialite']} ({r['telephone']})": r["id"]
                for _, r in df_tr_wa.iterrows()
            }
            label_tr_wa = st.selectbox("👤 اختر المتكوّن للرسالة", list(labels_map_wa.keys()), key="wa_trainee_single")
            trainee_id_wa = labels_map_wa[label_tr_wa]
            tr_row = df_tr_all[df_tr_all["id"] == trainee_id_wa].iloc[0]

            target_wa = st.radio("المرسل إليه", ["المتكوّن", "الولي"], horizontal=True, key="wa_target_single")
            phone_target = tr_row["telephone"] if target_wa == "المتكوّن" else tr_row["tel_parent"]
            phone_target = normalize_phone(phone_target)

            st.markdown("#### 🕒 اختر الفترة")
            period_type = st.radio("نوع الفترة", ["يوم", "أسبوع", "شهر", "مخصص"], horizontal=True, key="wa_period_single")
            today = date.today()

            if period_type == "يوم":
                d_single = st.date_input("اليوم", value=today, key="wa_day_single")
                d_from = d_single
                d_to = d_single
                period_label = f"بتاريخ {d_single.strftime('%Y-%m-%d')}"
            elif period_type == "أسبوع":
                week_start = st.date_input("بداية الأسبوع", value=today, key="wa_week_start_single")
                d_from = week_start
                d_to = week_start + timedelta(days=6)
                period_label = f"من {d_from.strftime('%Y-%m-%d')} إلى {d_to.strftime('%Y-%m-%d')}"
            elif period_type == "شهر":
                any_day = st.date_input("أي يوم من الشهر المطلوب", value=today, key="wa_month_day_single")
                first = any_day.replace(day=1)
                next_first = first.replace(year=first.year + 1, month=1) if first.month == 12 else first.replace(month=first.month + 1)
                last = next_first - timedelta(days=1)
                d_from = first
                d_to = last
                period_label = f"من {d_from.strftime('%Y-%m-%d')} إلى {d_to.strftime('%Y-%m-%d')} (شهر كامل)"
            else:
                c1, c2 = st.columns(2)
                with c1:
                    d_from = st.date_input("من تاريخ", value=today - timedelta(days=7), key="wa_from_single")
                with c2:
                    d_to = st.date_input("إلى تاريخ", value=today, key="wa_to_single")
                if d_to < d_from:
                    st.error("❌ تاريخ النهاية لازم يكون بعد البداية.")
                    d_from, d_to = d_to, d_from
                period_label = f"من {d_from.strftime('%Y-%m-%d')} إلى {d_to.strftime('%Y-%m-%d')}"

            if st.button("📲 جهّز رسالة الواتساب (فردي)", key="btn_wa_single"):
                if not phone_target:
                    st.error("❌ ما فماش رقم هاتف مضبوط للمتكوّن/الولي.")
                else:
                    msg, info_debug = build_whatsapp_message_for_trainee(
                        tr_row, df_abs_all, df_sub_all, branch, d_from, d_to, period_label
                    )
                    if not msg:
                        st.info("لا توجد غيابات في هذه الفترة لهذا المتكوّن.")
                    else:
                        st.caption("معلومة تقنية: " + " | ".join(info_debug))
                        st.text_area("نص الرسالة (يمكنك تعديله قبل الإرسال)", value=msg, height=250, key="wa_msg_preview_single")
                        link = wa_link(phone_target, msg)
                        st.markdown(f"[📲 افتح رسالة الواتساب الجاهزة]({link})")

                        try:
                            append_notification_log(
                                trainee_id=tr_row["id"],
                                phone=phone_target,
                                target="Trainee" if target_wa == "المتكوّن" else "Parent",
                                branche=branch,
                                period_from=d_from,
                                period_to=d_to,
                                period_label=period_label,
                            )
                        except Exception:
                            pass

        st.markdown("---")

        # -------- جماعي --------
        st.markdown("### 👥 جماعي (عدة متكوّنين في نفس الفترة)")

        spec_batch = st.selectbox("🔧 اختر التخصّص (للجماعي)", ["(الكل)"] + specs_branch, key="wa_spec_batch")
        df_tr_batch = df_tr_b.copy()
        if spec_batch != "(الكل)":
            df_tr_batch = df_tr_batch[df_tr_batch["specialite"] == spec_batch]

        if df_tr_batch.empty:
            st.info("لا يوجد متكوّنون لهذا الشرط.")
        else:
            st.markdown("#### 🕒 اختر الفترة المشتركة")
            period_type_b = st.radio("نوع الفترة", ["يوم", "أسبوع", "شهر", "مخصص"], horizontal=True, key="wa_period_batch")
            today_b = date.today()

            if period_type_b == "يوم":
                d_single_b = st.date_input("اليوم", value=today_b, key="wa_day_batch")
                d_from_b = d_single_b
                d_to_b = d_single_b
                period_label_b = f"بتاريخ {d_single_b.strftime('%Y-%m-%d')}"
            elif period_type_b == "أسبوع":
                week_start_b = st.date_input("بداية الأسبوع", value=today_b, key="wa_week_start_batch")
                d_from_b = week_start_b
                d_to_b = week_start_b + timedelta(days=6)
                period_label_b = f"من {d_from_b.strftime('%Y-%m-%d')} إلى {d_to_b.strftime('%Y-%m-%d')}"
            elif period_type_b == "شهر":
                any_day_b = st.date_input("أي يوم من الشهر المطلوب", value=today_b, key="wa_month_day_batch")
                first_b = any_day_b.replace(day=1)
                next_first_b = first_b.replace(year=first_b.year + 1, month=1) if first_b.month == 12 else first_b.replace(month=first_b.month + 1)
                last_b = next_first_b - timedelta(days=1)
                d_from_b = first_b
                d_to_b = last_b
                period_label_b = f"من {d_from_b.strftime('%Y-%m-%d')} إلى {d_to_b.strftime('%Y-%m-%d')} (شهر كامل)"
            else:
                c1, c2 = st.columns(2)
                with c1:
                    d_from_b = st.date_input("من تاريخ", value=today_b - timedelta(days=7), key="wa_from_batch")
                with c2:
                    d_to_b = st.date_input("إلى تاريخ", value=today_b, key="wa_to_batch")
                if d_to_b < d_from_b:
                    st.error("❌ تاريخ النهاية لازم يكون بعد البداية.")
                    d_from_b, d_to_b = d_to_b, d_from_b
                period_label_b = f"من {d_from_b.strftime('%Y-%m-%d')} إلى {d_to_b.strftime('%Y-%m-%d')}"

            target_batch = st.radio("المرسل إليه في الجماعي", ["المتكوّن", "الولي"], horizontal=True, key="wa_target_batch")

            if st.button("📲 توليد روابط الواتساب لكل المتكوّنين (جماعي)", key="btn_wa_batch"):
                rows_out = []
                for _, tr in df_tr_batch.iterrows():
                    phone_t = tr["telephone"] if target_batch == "المتكوّن" else tr["tel_parent"]
                    phone_t = normalize_phone(phone_t)
                    if not phone_t:
                        continue

                    msg_t, _ = build_whatsapp_message_for_trainee(
                        tr, df_abs_all, df_sub_all, branch, d_from_b, d_to_b, period_label_b
                    )
                    if not msg_t:
                        continue

                    link_t = wa_link(phone_t, msg_t)
                    rows_out.append({"المتكوّن": tr["nom"], "التخصّص": tr.get("specialite", ""), "الهاتف": phone_t, "رابط": link_t, "trainee_id": tr["id"]})

                    try:
                        append_notification_log(
                            trainee_id=tr["id"],
                            phone=phone_t,
                            target="Trainee" if target_batch == "المتكوّن" else "Parent",
                            branche=branch,
                            period_from=d_from_b,
                            period_to=d_to_b,
                            period_label=period_label_b,
                        )
                    except Exception:
                        pass

                if not rows_out:
                    st.info("لا يوجد متكوّنين لديهم غيابات في هذه الفترة حسب الشروط.")
                else:
                    st.caption("إضغط على الزر قدّام كل متكوّن لفتح الواتساب.")
                    for i, row in enumerate(rows_out, start=1):
                        st.markdown(
                            f"""
                            <div style="margin-bottom:10px; padding:8px; border:1px solid #eee; border-radius:6px;">
                              <b>{i}. {row['المتكوّن']}</b><br/>
                              التخصّص: {row.get('التخصّص','')}<br/>
                              الهاتف: {row['الهاتف']}<br/>
                              <a href="{row['رابط']}" target="_blank"
                                 style="display:inline-block;margin-top:6px;padding:6px 14px;background-color:#25D366;color:white;text-decoration:none;border-radius:6px;font-weight:700;font-size:14px;">
                                 📲 فتح واتساب
                              </a>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )


# ================== Tab5: Notifications log ==================
with tab5:
    st.subheader("📜 سجل الإشعارات المرسلة")

    df_tr_all = load_trainees()
    df_notif = load_notifications()

    if df_notif.empty:
        st.info("ما زال ما تمّ تسجيل حتى إشعار مرسل.")
    else:
        df_notif_b = df_notif[df_notif["branche"] == branch].copy()
        if df_notif_b.empty:
            st.info("ما فماش إشعارات مسجلة لهذا الفرع.")
        else:
            df_tr_all_small = df_tr_all[["id", "nom", "specialite"]].rename(columns={"id": "trainee_id"})
            df_notif_b = df_notif_b.merge(df_tr_all_small, on="trainee_id", how="left")

            def fmt_ts(x: str) -> str:
                try:
                    dt = datetime.fromisoformat(x)
                    return dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    return x

            df_notif_b["تاريخ الإرسال"] = df_notif_b["sent_at_iso"].apply(fmt_ts)
            df_notif_b = df_notif_b.sort_values("sent_at_iso", ascending=False).reset_index(drop=True)

            df_notif_b = df_notif_b.rename(
                columns={
                    "nom": "المتكوّن",
                    "specialite": "التخصّص",
                    "phone": "الهاتف",
                    "target": "المرسل إليه",
                    "period_label": "الفترة",
                }
            )

            st.dataframe(
                df_notif_b[["تاريخ الإرسال", "المتكوّن", "التخصّص", "الهاتف", "المرسل إليه", "الفترة"]],
                use_container_width=True,
            )

