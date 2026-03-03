import streamlit as st
import pandas as pd
from datetime import datetime
import calendar
import altair as alt
import warnings
import time
import os

# [구글 연동용 라이브러리]
import gspread
from oauth2client.service_account import ServiceAccountCredentials

warnings.filterwarnings('ignore')

if 'recent_entries' not in st.session_state:
    st.session_state.recent_entries = []

# =====================================================================
# 1. 🌐 구글 연결 & 데이터 최적화 (초고속 캐싱 적용)
# =====================================================================
# 💡 한 번 연결한 구글 서버는 1시간 동안 계속 유지합니다! (속도 10배 향상)
@st.cache_resource(ttl=3600)
def init_google_connection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    key_file = 'disco-arcana-489123-u3-f20de925b249.json'
    
    if os.path.exists(key_file):
        creds = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)
    else:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    
    client = gspread.authorize(creds)
    # 직접 올려주신 구글 시트 주소
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1VSzc14zjG9FP16xSyqZJl1Fqy_lpJhBD6I8T_yHzoSo/edit"
    doc = client.open_by_url(SHEET_URL)
    
    ws_t = doc.worksheet('transactions')
    ws_f = doc.worksheet('fixed_expenses')
    ws_c = doc.worksheet('payment_checks')
    return ws_t, ws_f, ws_c

ws_trans, ws_fixed, ws_checks = init_google_connection()

T_COLS = ['id', 'date', 'type', 'content', 'category', 'payment_method', 'amount', 'memo', 'is_fixed', 'transfer_account']
F_COLS = ['id', 'content', 'category', 'payment_method', 'transfer_account', 'amount', 'payment_day', 'start_date', 'end_date', 'installment_months', 'memo']
C_COLS = ['id', 'year', 'month', 'item_name', 'is_paid']

# 💡 데이터를 한 번 읽어오면 10초간 기억해서 구글의 에러 차단을 막습니다!
@st.cache_data(ttl=10)
def _fetch_records(ws_name):
    ws_map = {'trans': ws_trans, 'fixed': ws_fixed, 'checks': ws_checks}
    return ws_map[ws_name].get_all_records()

def load_data(ws_name, cols):
    records = _fetch_records(ws_name)
    if not records:
        ws_map = {'trans': ws_trans, 'fixed': ws_fixed, 'checks': ws_checks}
        ws = ws_map[ws_name]
        if not ws.row_values(1):
            ws.append_row(cols)
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(records)
    for c in cols:
        if c not in df.columns: df[c] = ""
    return df

def rewrite_sheet(ws_name, df, cols):
    ws_map = {'trans': ws_trans, 'fixed': ws_fixed, 'checks': ws_checks}
    ws = ws_map[ws_name]
    ws.clear()
    if not df.empty:
        write_df = df[cols].fillna("").astype(str)
        ws.update([write_df.columns.values.tolist()] + write_df.values.tolist())
    else:
        ws.update([cols])
    _fetch_records.clear() # 저장 직후에는 과거 기억을 지우고 새로고침!

# 최초 헤더 생성 확인
_ = load_data('trans', T_COLS)
_ = load_data('fixed', F_COLS)
_ = load_data('checks', C_COLS)


# =====================================================================
# 2. UI 및 환경 셋팅
# =====================================================================
st.set_page_config(page_title="짜장당근 가계부 (클라우드)", layout="wide")
st.title("☁️ 짜장당근 맞춤형 가계부 (Google 연동됨)")

MY_ACCOUNTS = [
    "지정 안 함",
    "농협 3120111632011",
    "국민은행 659401-01-585635",
    "하나은행 36791039350407",
    "우리은행 1002557320818",
    "신한은행 110-367-020370",
    "카카오뱅크(혜경) 3333-01-5988576",
    "농협 3120111632011",
    "카카오뱅크(정민) 3333-17-5922472",
    "토스뱅크 1000-7396-8428",
]

ALL_CATEGORIES = ["식비", "생활", "교통", "미용", "문화생활", "짜장당근", "치료/예방", "여행", "차량", "할부", "대출", "주거", "통신", "월결제", "보험", "급여", "부수입", "상여금", "용돈", "금융소득", "저축", "투자", "카드대금", "단순이체", "기타"]

PAYMENT_METHODS = ["현대카드", "우리카드", "국민카드", "삼성카드", "현금", "체크카드", "자동이체", "계좌입금", "계좌이체", "기타"]

def safe_format(val):
    try:
        if pd.isna(val) or val == "" or val == "None": return "0 원"
        return f"{int(float(val)):,} 원"
    except: return "0 원"

st.sidebar.header("📅 조회 월 선택")
current_year = datetime.now().year
current_month = datetime.now().month

selected_year = st.sidebar.selectbox("연도", [current_year - 1, current_year, current_year + 1, current_year + 2], index=1)
selected_month = st.sidebar.selectbox("월", list(range(1, 13)), index=current_month - 1)

st.sidebar.divider()
st.sidebar.write("### 💾 데이터베이스 (구글 시트)")
st.sidebar.info("데이터는 모두 구글 시트에 실시간으로 자동 저장됩니다.")
st.sidebar.markdown(f"[엑셀(구글시트) 원본 열기](https://docs.google.com/spreadsheets/d/1VSzc14zjG9FP16xSyqZJl1Fqy_lpJhBD6I8T_yHzoSo/edit)")

target_month_str = f"{selected_year}년 {selected_month}월"
st.subheader(f"📊 {target_month_str} 요약 및 관리")

KOR_COLS = {'content': '내용(품목)', 'category': '카테고리', 'payment_method': '결제방식', 'transfer_account': '이체계좌', 'amount': '금액', 'payment_day': '결제일(일)', 'start_date': '시작일(YYYY-MM-DD)', 'end_date': '종료일(YYYY-MM-DD)', 'installment_months': '할부/반복(개월)', 'memo': '비고'}
ENG_COLS = {v: k for k, v in KOR_COLS.items()}

# =====================================================================
# 3. 탭 구성
# =====================================================================
tab1, tab2 = st.tabs(["📝 내역 및 할부 입력", "📈 월별 통계 및 달력 조회"])

with tab1:
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.write("#### ✏️ 수동 지출/수입 기입")
        t_date = st.date_input("날짜", datetime.today()) 
        t_type = st.selectbox("구분", ["지출", "수입", "이체"])
        
        if t_type == "수입":
            cat_options = ["급여", "부수입", "상여금", "용돈", "금융소득", "기타"]
            pay_options = ["계좌입금", "현금", "기타"]
        elif t_type == "이체":
            cat_options = ["저축", "투자", "카드대금", "단순이체", "기타"]
            pay_options = ["계좌이체", "현금", "기타"]
        else:
            cat_options = ["식비", "생활", "교통", "미용", "문화생활", "짜장당근", "치료/예방", "여행", "차량", "할부", "기타"]

            pay_options = PAYMENT_METHODS

        t_content = st.text_input("내용 (예: 세탁기 구매, 월급 등)")
        t_category = st.selectbox("카테고리", cat_options)
        t_amount = st.number_input("금액 (숫자만 입력)", min_value=0, step=1000)
        t_method = st.selectbox("결제/입금방식", pay_options)
        t_memo = st.text_input("비고 (단순 메모)")
        
        if st.button("내역 저장하기", type="primary"):
            if not t_content: st.warning("⚠️ 내용을 입력해 주세요!")
            elif t_amount == 0: st.error("⚠️ 금액을 1원 이상 입력해 주세요!")
            else:
                new_id = int(time.time() * 1000000)
                ws_trans.append_row([new_id, t_date.strftime("%Y-%m-%d"), t_type, t_content, t_category, t_method, t_amount, t_memo, 0, ""])
                _fetch_records.clear() # 추가 후 기억 지우기
                st.success("✅ 수동 기입 완료! (구글 시트에 저장됨)")
                
                new_entry = {
                    "날짜": t_date.strftime("%Y-%m-%d"), "구분": t_type, "내용(품목)": t_content,
                    "카테고리": t_category, "결제방식": t_method, "금액": f"{t_amount:,} 원", "비고": t_memo
                }
                st.session_state.recent_entries.insert(0, new_entry)
                
        if len(st.session_state.recent_entries) > 0:
            st.write("##### 👇 이번 접속 중 기입한 내역 (최신순)")
            st.dataframe(pd.DataFrame(st.session_state.recent_entries), hide_index=True, use_container_width=True)
                
    with col2:
        st.write("#### 📌 고정비 및 할부 추가")
        with st.form("add_fixed_form", clear_on_submit=True):
            st.info("시작일과 결제일을 적어두면 매월 알아서 계산됩니다. 만기 없는 일반 고정비는 '할부 개월'을 0으로 비워두세요.")
            f_content = st.text_input("내용 (예: 세탁기 할부, 넷플릭스, 차량대출)")
            f_cat = st.selectbox("카테고리", ["할부", "대출", "주거", "통신", "월결제", "보험", "기타"])
            f_method = st.selectbox("결제방식", PAYMENT_METHODS)
            f_account = st.selectbox("이체 계좌 (선택)", MY_ACCOUNTS)
            f_amount = st.number_input("금액 (숫자만 입력)", min_value=0, step=1000)
            f_pay_day = st.number_input("매월 결제일 (1~31일 지정)", min_value=1, max_value=31, value=1, step=1)
            f_start = st.date_input("할부/시작일 (이 날짜 이후부터 부과됨)")
            f_end_str = st.text_input("종료일 (선택. 금액 변동 시 이전 기록 보존용. 예: 2026-04-30)")
            f_months = st.number_input("할부 개월 수 (일반 고정비는 0)", min_value=0, step=1)
            f_memo = st.text_input("비고 (메모)")
            
            if st.form_submit_button("템플릿에 추가"):
                if not f_content: st.warning("⚠️ 내용을 입력해 주세요!")
                elif f_amount == 0: st.error("⚠️ 금액을 입력해 주세요!")
                else:
                    new_id = int(time.time() * 1000000)
                    final_account = "" if f_account == "지정 안 함" else f_account
                    start_str = f_start.strftime("%Y-%m-%d")
                    ws_fixed.append_row([new_id, f_content, f_cat, f_method, final_account, f_amount, f_pay_day, start_str, f_end_str, f_months, f_memo])
                    _fetch_records.clear()
                    st.success("✅ 추가되었습니다! 아래 목록에 반영됩니다.")
                    st.rerun()

    st.divider()
    st.write("### 📋 현재 등록된 고정비/할부 목록 관리")
    st.caption("항목을 선택하고 Delete 키를 누르면 삭제할 수 있습니다.")

    fixed_df = load_data('fixed', F_COLS)
    
    if not fixed_df.empty:
        status_list = []
        for _, row in fixed_df.iterrows():
            s_date_str = str(row['start_date']).strip() if pd.notna(row['start_date']) else ""
            e_date_str = str(row.get('end_date', '')).strip() if pd.notna(row.get('end_date')) else ""
            install_months = pd.to_numeric(row['installment_months'], errors='coerce')
            is_installment = pd.notna(install_months) and install_months > 0

            curr_month_val = selected_year * 12 + selected_month
            start_month_val = 0
            if s_date_str and s_date_str not in ('nan', 'None', ''):
                try: 
                    start_dt = pd.to_datetime(s_date_str)
                    start_month_val = start_dt.year * 12 + start_dt.month
                except: pass
            
            end_month_val = 999999
            if e_date_str and e_date_str not in ('nan', 'None', ''):
                try:
                    end_dt = pd.to_datetime(e_date_str)
                    end_month_val = end_dt.year * 12 + end_dt.month
                except: pass

            if start_month_val > 0 and curr_month_val < start_month_val: status_list.append("⏳ 시작 전")
            elif curr_month_val > end_month_val: status_list.append("🔴 종료됨")
            elif is_installment and start_month_val > 0 and (curr_month_val - start_month_val) >= install_months: status_list.append("🔴 종료됨")
            elif is_installment and start_month_val > 0: status_list.append(f"🟢 진행중 ({curr_month_val - start_month_val + 1}/{int(install_months)}회차)")
            else: status_list.append("🟢 매월 반복")

        fixed_df['상태(조회월 기준)'] = status_list
        
        expired_mask = fixed_df['상태(조회월 기준)'] == '🔴 종료됨'
        expired_df = fixed_df[expired_mask]
        active_df = fixed_df[~expired_mask]
        
        if not active_df.empty:
            ui_df = active_df.drop(columns=['id']).rename(columns=KOR_COLS)
            ordered_cols = ['내용(품목)', '카테고리', '결제방식', '이체계좌', '금액', '결제일(일)', '시작일(YYYY-MM-DD)', '종료일(YYYY-MM-DD)', '할부/반복(개월)', '비고', '상태(조회월 기준)']
            ui_df = ui_df[ordered_cols]
            
            edited_ui_df = st.data_editor(ui_df, num_rows="dynamic", use_container_width=True, key="fixed_editor",
                                          column_config={
                                              "금액": st.column_config.NumberColumn(format="%d"),
                                              "결제일(일)": st.column_config.NumberColumn(min_value=1, max_value=31, step=1), 
                                              "카테고리": st.column_config.SelectboxColumn("카테고리", options=ALL_CATEGORIES),
                                              "결제방식": st.column_config.SelectboxColumn("결제방식", options=PAYMENT_METHODS),
                                              "이체계좌": st.column_config.SelectboxColumn("이체계좌", options=MY_ACCOUNTS),
                                              "상태(조회월 기준)": st.column_config.TextColumn(disabled=True)
                                          })
            
            if st.button("💾 목록 변경사항 구글 시트에 적용"):
                save_df = edited_ui_df.drop(columns=['상태(조회월 기준)'], errors='ignore').rename(columns=ENG_COLS)
                
                combined_list = []
                for _, row in save_df.iterrows():
                    c_val = str(row.get('content', '')).strip()
                    if not c_val or c_val in ('None', 'nan'): continue
                    acct = str(row.get('transfer_account', '')).strip()
                    if acct in ('지정 안 함', 'None', 'nan'): acct = ""
                    
                    combined_list.append({
                        'id': int(time.time() * 1000000) + len(combined_list),
                        'content': c_val, 'category': row.get('category', ''), 'payment_method': row.get('payment_method', ''),
                        'transfer_account': acct, 'amount': row.get('amount', 0), 'payment_day': row.get('payment_day', 1),
                        'start_date': row.get('start_date', ''), 'end_date': row.get('end_date', ''), 
                        'installment_months': row.get('installment_months', 0), 'memo': row.get('memo', '')
                    })
                    
                for _, row in expired_df.iterrows():
                    combined_list.append({
                        'id': row['id'], 'content': row['content'], 'category': row['category'], 'payment_method': row['payment_method'],
                        'transfer_account': row['transfer_account'], 'amount': row['amount'], 'payment_day': row['payment_day'],
                        'start_date': row['start_date'], 'end_date': row['end_date'], 
                        'installment_months': row['installment_months'], 'memo': row['memo']
                    })
                    
                rewrite_sheet('fixed', pd.DataFrame(combined_list), F_COLS)
                st.success("✅ 구글 시트에 반영되었습니다!")
                st.rerun() 
        else:
            st.info("등록된 진행 중인 고정비/할부가 없습니다.")
    else:
        st.info("등록된 고정비/할부가 없습니다.")

with tab2:
    df = load_data('trans', T_COLS)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    month_df = df[(df['date'].dt.year == selected_year) & (df['date'].dt.month == selected_month)].copy()
    
    fixed_df = load_data('fixed', F_COLS)
    dynamic_fixed_rows = []
    
    for _, row in fixed_df.iterrows():
        s_date_str = str(row['start_date']).strip() if pd.notna(row['start_date']) else ""
        e_date_str = str(row.get('end_date', '')).strip() if pd.notna(row.get('end_date')) else ""
        install_months = pd.to_numeric(row['installment_months'], errors='coerce')
        pay_day_val = pd.to_numeric(row.get('payment_day'), errors='coerce')
        
        is_installment = pd.notna(install_months) and install_months > 0
        final_memo = str(row['memo']) if pd.notna(row['memo']) else ""
        
        is_active = True
        start_dt = None
        curr_month_val = selected_year * 12 + selected_month
        
        if s_date_str and s_date_str not in ('nan', 'None', ''):
            try: 
                start_dt = pd.to_datetime(s_date_str)
                start_month_val = start_dt.year * 12 + start_dt.month
                if curr_month_val < start_month_val: is_active = False 
                elif is_installment and (curr_month_val - start_month_val) >= install_months: is_active = False 
                elif is_active and is_installment: final_memo += f" ({curr_month_val - start_month_val + 1}/{int(install_months)}회차)"
            except: pass
            
        if is_active and e_date_str and e_date_str not in ('nan', 'None', ''):
            try:
                end_dt = pd.to_datetime(e_date_str)
                end_month_val = end_dt.year * 12 + end_dt.month
                if curr_month_val > end_month_val: is_active = False
            except: pass
        
        if is_active:
            if pd.notna(pay_day_val) and pay_day_val > 0: target_day = int(pay_day_val)
            elif start_dt: target_day = start_dt.day
            else: target_day = 1
                
            last_day = calendar.monthrange(selected_year, selected_month)[1]
            safe_target_day = min(target_day, last_day)
            
            final_date = pd.to_datetime(f"{selected_year}-{selected_month:02d}-{safe_target_day:02d}")
            dynamic_fixed_rows.append({
                'id': 0, 'date': final_date, 'type': '지출', 'content': row.get('content', ''), 'category': row['category'], 
                'payment_method': row['payment_method'], 'amount': pd.to_numeric(row['amount'], errors='coerce'), 
                'memo': final_memo, 'is_fixed': True, 'transfer_account': row['transfer_account']
            })

    dynamic_df = pd.DataFrame(dynamic_fixed_rows)
    if not dynamic_df.empty:
        month_df = pd.concat([month_df, dynamic_df], ignore_index=True)

    if not month_df.empty:
        month_df['amount'] = pd.to_numeric(month_df['amount'], errors='coerce').fillna(0)
        
        total_income = month_df[month_df['type'] == '수입']['amount'].sum()
        total_expense = month_df[month_df['type'] == '지출']['amount'].sum()
        
        fixed_mask = month_df['is_fixed'].astype(str).str.lower().isin(['1', 'true'])
        fixed_expense_total = month_df[(month_df['type'] == '지출') & fixed_mask]['amount'].sum()
        
        st.write(f"### 💰 {target_month_str} 결산")
        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        m_col1.metric("총 수입", f"{total_income:,.0f} 원")
        m_col2.metric("총 지출", f"{total_expense:,.0f} 원")
        m_col3.metric("고정비/할부 합계", f"{fixed_expense_total:,.0f} 원")
        m_col4.metric("수입-지출(남은돈)", f"{(total_income - total_expense):,.0f} 원")
        
        st.divider()

        st.write("#### ✅ 이번 달 정기 결제/이체 납부 체크리스트")
        checklist_data = []
        if not dynamic_df.empty:
            for _, row in dynamic_df.iterrows():
                pay_day = row['date'].day
                name = f"[{row['category']}] {row['content']} ({row['payment_method']})"
                acct = str(row.get('transfer_account', '')).strip()
                if acct and acct not in ('None', 'nan', '지정 안 함'): name += f" - {acct}"
                checklist_data.append({"카테고리": row['category'], "결제일": pay_day, "항목 (결제 내역)": name, "예상 청구 금액": row['amount']})
                
        card_expenses = month_df[(month_df['type'] == '지출') & (month_df['payment_method'].str.contains('카드', na=False))]
        if not card_expenses.empty:
            card_totals = card_expenses.groupby('payment_method')['amount'].sum()
            for card_name, amt in card_totals.items():
                checklist_data.append({"카테고리": "카드대금", "결제일": 99, "항목 (결제 내역)": f"[카드대금] {card_name} 이번 달 누적 지출", "예상 청구 금액": amt})

        chk_df = pd.DataFrame(checklist_data)
        if not chk_df.empty:
            chk_df = chk_df.groupby(['카테고리', '결제일', '항목 (결제 내역)'])['예상 청구 금액'].sum().reset_index()
            chk_df['sort_prio'] = chk_df['카테고리'].apply(lambda x: 0 if x == '대출' else 1)
            chk_df = chk_df.sort_values(by=['sort_prio', '결제일', '카테고리']).drop(columns=['sort_prio'])
            chk_df['결제일'] = chk_df['결제일'].apply(lambda x: "-" if x == 99 else f"{x}일")
            
            all_cats = chk_df['카테고리'].unique().tolist()
            selected_cats = st.multiselect("🔍 볼 카테고리 필터링", all_cats, default=all_cats)
            
            all_checks = load_data('checks', C_COLS)
            curr_checks = all_checks[(all_checks['year'] == selected_year) & (all_checks['month'] == selected_month)]
            paid_map = dict(zip(curr_checks['item_name'], curr_checks['is_paid'].astype(str) == '1'))
            
            chk_df['완료여부 (체크)'] = chk_df['항목 (결제 내역)'].map(lambda x: paid_map.get(x, False))
            if chk_df['완료여부 (체크)'].all(): st.balloons()
            
            filtered_chk_df = chk_df[chk_df['카테고리'].isin(selected_cats)].copy()
            if not filtered_chk_df.empty:
                filtered_chk_df['예상 청구 금액'] = filtered_chk_df['예상 청구 금액'].apply(safe_format)
                edited_chk_df = st.data_editor(filtered_chk_df, hide_index=True, use_container_width=True,
                    column_config={"카테고리": st.column_config.TextColumn(disabled=True), "결제일": st.column_config.TextColumn(disabled=True), "항목 (결제 내역)": st.column_config.TextColumn(disabled=True), "예상 청구 금액": st.column_config.TextColumn(disabled=True), "완료여부 (체크)": st.column_config.CheckboxColumn("납부 완료")})
                
                if st.button("💾 체크리스트 저장"):
                    for _, row in edited_chk_df.iterrows(): paid_map[row['항목 (결제 내역)']] = row['완료여부 (체크)']
                    keep_checks = all_checks[~((all_checks['year'] == selected_year) & (all_checks['month'] == selected_month))]
                    new_chk_list = keep_checks.to_dict('records')
                    
                    for item_name, is_paid in paid_map.items():
                        new_chk_list.append({'id': int(time.time() * 1000000), 'year': selected_year, 'month': selected_month, 'item_name': item_name, 'is_paid': 1 if is_paid else 0})
                    rewrite_sheet('checks', pd.DataFrame(new_chk_list), C_COLS)
                    st.success("✅ 구글 시트에 저장되었습니다!")
                    st.rerun()
            else: st.info("선택하신 카테고리에 해당하는 내역이 없습니다.")
        else: st.info("이번 달에 납부할 고정비나 카드 내역이 없습니다.")
            
        st.divider()
        st.write("#### 📊 카테고리별 지출 비율")
        cat_expense = month_df[month_df['type'] == '지출'].groupby('category')['amount'].sum().reset_index()
        if not cat_expense.empty:
            cat_expense.columns = ['카테고리', '지출 합계']
            cat_expense = cat_expense.sort_values(by='지출 합계', ascending=False)
            
            p_col1, p_col2 = st.columns([1, 1])
            with p_col1:
                base = alt.Chart(cat_expense).encode(theta=alt.Theta(field="지출 합계", type="quantitative", stack=True), color=alt.Color(field="카테고리", type="nominal", legend=None))
                pie_chart = (base.mark_arc(innerRadius=50, outerRadius=110) + base.mark_text(radius=135, size=15, fontWeight='bold').encode(text=alt.Text(field="카테고리", type="nominal"))).properties(height=300)
                st.altair_chart(pie_chart, use_container_width=True)
                
            with p_col2:
                cat_display = cat_expense.copy()
                cat_display['지출 합계'] = cat_display['지출 합계'].apply(safe_format)
                st.dataframe(cat_display, use_container_width=True, hide_index=True)
        
        st.divider()
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1:
            st.write("#### 💳 이번 달 결제수단별 지출")
            card_expense = month_df[month_df['type'] == '지출'].groupby('payment_method')['amount'].sum().reset_index()
            card_expense.columns = ['결제수단', '지출 합계']
            card_expense['지출 합계'] = card_expense['지출 합계'].apply(safe_format)
            st.dataframe(card_expense, use_container_width=True, hide_index=True)
            
        with col_c2:
            st.write(f"#### 🗓️ {target_month_str} 지출 달력")
            st.caption("💡 금액 위에 마우스를 올리면 상세 내역을 볼 수 있습니다!")
            
            manual_mask = (month_df['type'] == '지출') & (~fixed_mask)
            fixed_mask_only = (month_df['type'] == '지출') & fixed_mask
            
            daily_manual = month_df[manual_mask].groupby(month_df['date'].dt.strftime('%Y-%m-%d'))['amount'].sum().to_dict()
            daily_loan = month_df[fixed_mask_only & (month_df['category'] == '대출')].groupby(month_df['date'].dt.strftime('%Y-%m-%d'))['amount'].sum().to_dict()
            daily_inst = month_df[fixed_mask_only & (month_df['category'] == '할부')].groupby(month_df['date'].dt.strftime('%Y-%m-%d'))['amount'].sum().to_dict()
            daily_other = month_df[fixed_mask_only & (~month_df['category'].isin(['대출', '할부']))].groupby(month_df['date'].dt.strftime('%Y-%m-%d'))['amount'].sum().to_dict()
            
            daily_details = {}
            for _, row in month_df[month_df['type'] == '지출'].iterrows():
                d_str = row['date'].strftime('%Y-%m-%d')
                if d_str not in daily_details: daily_details[d_str] = {'manual': [], 'loan': [], 'inst': [], 'other': []}
                amt_str = f"{int(row['amount']):,}원"
                safe_content = str(row['content']).replace("'", "").replace('"', "") if pd.notna(row['content']) else "이름없음"
                item_str = f"▪ {safe_content}: {amt_str}"
                
                is_fxd = str(row['is_fixed']).lower() in ['1', 'true']
                if not is_fxd: daily_details[d_str]['manual'].append(item_str)
                elif row['category'] == '대출': daily_details[d_str]['loan'].append(item_str)
                elif row['category'] == '할부': daily_details[d_str]['inst'].append(item_str)
                else: daily_details[d_str]['other'].append(item_str)

            cal = calendar.monthcalendar(selected_year, selected_month)
            cal_html = "<table style='width:100%; border-collapse: collapse; text-align:center; font-family: sans-serif;'><tr style='background-color:#f0f2f6; color:#333;'><th style='padding:10px; border:1px solid #ddd; width:14%;'>월</th><th style='border:1px solid #ddd; width:14%;'>화</th><th style='border:1px solid #ddd; width:14%;'>수</th><th style='border:1px solid #ddd; width:14%;'>목</th><th style='border:1px solid #ddd; width:14%;'>금</th><th style='border:1px solid #ddd; width:14%;'>토</th><th style='border:1px solid #ddd; width:14%;'>일</th></tr>"
            
            for week in cal:
                cal_html += "<tr>"
                for day in week:
                    if day == 0: cal_html += "<td style='border:1px solid #ddd; height:95px; background-color:#fafafa;'></td>"
                    else:
                        date_str = f"{selected_year}-{selected_month:02d}-{day:02d}"
                        html_parts = []
                        if daily_manual.get(date_str, 0) > 0: 
                            tt = "&#10;".join(daily_details.get(date_str, {}).get('manual', []))
                            html_parts.append(f"<div title='{tt}' style='color:#ff4b4b; font-weight:bold; font-size:13px; margin-top:3px; cursor:pointer;'>{daily_manual[date_str]:,.0f}원</div>")
                        if daily_loan.get(date_str, 0) > 0: 
                            tt = "&#10;".join(daily_details.get(date_str, {}).get('loan', []))
                            html_parts.append(f"<div title='{tt}' style='color:#1f77b4; font-weight:bold; font-size:12px; margin-top:3px; cursor:pointer;'>대출 {daily_loan[date_str]:,.0f}</div>")
                        if daily_inst.get(date_str, 0) > 0: 
                            tt = "&#10;".join(daily_details.get(date_str, {}).get('inst', []))
                            html_parts.append(f"<div title='{tt}' style='color:#2ca02c; font-weight:bold; font-size:12px; margin-top:3px; cursor:pointer;'>할부 {daily_inst[date_str]:,.0f}</div>")
                        if daily_other.get(date_str, 0) > 0: 
                            tt = "&#10;".join(daily_details.get(date_str, {}).get('other', []))
                            html_parts.append(f"<div title='{tt}' style='color:#ff7f0e; font-weight:bold; font-size:12px; margin-top:3px; cursor:pointer;'>고정 {daily_other[date_str]:,.0f}</div>")
                        
                        amt_html = "".join(html_parts)
                        cal_html += f"<td style='border:1px solid #ddd; height:95px; vertical-align:top; padding:5px;'><div style='text-align:left; font-size:12px; color:#555;'>{day}</div>{amt_html}</td>"
                cal_html += "</tr>"
            cal_html += "</table>"
            st.markdown(cal_html, unsafe_allow_html=True)
        
        st.divider()
        st.write("#### 📋 이번 달 상세 내역")
        display_df = month_df.copy()
        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        display_df = display_df[['date', 'content', 'type', 'category', 'payment_method', 'transfer_account', 'amount', 'memo', 'is_fixed']]
        display_df.columns = ['날짜', '내용(품목)', '구분', '카테고리', '결제방식', '계좌정보(고정비)', '금액', '비고', '자동계산여부']
        display_df['금액'] = display_df['금액'].apply(safe_format)
        st.dataframe(display_df.sort_values(by='날짜', ascending=False), use_container_width=True, hide_index=True)

        st.write("---")
        with st.expander("🛠️ 이번 달 수동 기입 내역 수정 및 삭제 (잘못 입력한 항목 고치기)", expanded=False):
            st.info("수동으로 기입한 내역만 이곳에서 수정/삭제할 수 있습니다.")
            
            all_trans = load_data('trans', T_COLS)
            all_trans['date'] = pd.to_datetime(all_trans['date'], errors='coerce')
            curr_manual = all_trans[(all_trans['date'].dt.year == selected_year) & (all_trans['date'].dt.month == selected_month) & (~all_trans['is_fixed'].astype(str).str.lower().isin(['1', 'true']))].copy()
            
            if not curr_manual.empty:
                curr_manual['date'] = curr_manual['date'].dt.strftime('%Y-%m-%d')
                edit_ui_df = curr_manual.rename(columns={'date':'날짜', 'type':'구분', 'content':'내용(품목)', 'category':'카테고리', 'payment_method':'결제방식', 'amount':'금액', 'memo':'비고'})
                original_ids = curr_manual['id'].tolist()
                
                edited_manual_df = st.data_editor(edit_ui_df.drop(columns=['id', 'is_fixed', 'transfer_account']), num_rows="dynamic", use_container_width=True, key="manual_editor", 
                                                  column_config={"금액": st.column_config.NumberColumn(format="%d"), "카테고리": st.column_config.SelectboxColumn("카테고리", options=ALL_CATEGORIES), "결제방식": st.column_config.SelectboxColumn("결제방식", options=PAYMENT_METHODS)})
                
                if st.button("💾 변경사항 구글 시트에 저장"):
                    keep_tx = all_trans[~all_trans['id'].isin(original_ids)]
                    new_tx_list = keep_tx.to_dict('records')
                    
                    for _, row in edited_manual_df.iterrows():
                        content_val = str(row.get('내용(품목)', '')).strip()
                        if not content_val or content_val in ('None', 'nan'): continue
                        safe_amt = 0 if pd.isna(row.get('금액')) else row.get('금액')
                        new_tx_list.append({
                            'id': int(time.time() * 1000000) + len(new_tx_list), 'date': row['날짜'], 'type': row['구분'],
                            'content': content_val, 'category': row['카테고리'], 'payment_method': row['결제방식'],
                            'amount': safe_amt, 'memo': row.get('비고', ''), 'is_fixed': 0, 'transfer_account': ""
                        })
                    rewrite_sheet('trans', pd.DataFrame(new_tx_list), T_COLS)
                    st.success("✅ 구글 시트에 수정/삭제가 반영되었습니다!")
                    st.rerun() 
            else: st.info("이번 달에 수동으로 기입한 내역이 없습니다.")
    else: st.info(f"{target_month_str}의 데이터가 없습니다.")
